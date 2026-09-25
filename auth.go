package main

// auth — nn.npln.auth.v1.Auth. The NPLN entry point: S3 presents its external id
// token (the NSA/BAAS token from account.dat) and we hand back an opaque access token.
//
// The identity is now DYNAMIC + GATED: we resolve the NSA to the owning Nextendo
// account, REFUSE unless that account is a verified, non-frozen Nextendo account
// (fail-closed, same rule as the NEX games), and embed the account PID in the access
// token so the friends/presence services can look the player up. Set
// NPLN_ALLOW_UNVERIFIED=1 only bypasses email verification; identity proof and account lookup
// remain mandatory.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	authpb "npln.nintendo.net/npln-practice/proto/auth/v1"
)

type authServer struct {
	authpb.UnimplementedAuthServer
}

func short(s string) string {
	if len(s) > 24 {
		return s[:24] + "…"
	}
	return s
}

func describe(t *authpb.ExternalIdToken) string {
	if t == nil {
		return "<none>"
	}
	if v := t.GetNsaIdToken(); v != "" {
		return "nsa:" + short(v)
	}
	if v := t.GetDummyExtIdToken(); v != "" {
		return "dummy:" + v
	}
	return "<empty>"
}

// capturedUser is used only to align frozen replay payloads. It must never be used as a
// live player's identity or as a fallback for missing authentication.
//
// [Nextendo] Our captures cover TWO accounts and we were mixing them. The friends capture belongs
// to u-exemple5000000000000, but every GameRecord document we hold -- PointCard, VsResults,
// CoopResults, VsUserAttribute, CoopUserAttribute, going back to 2023 -- belongs to
// u-exemple7000000000000. Running the session as the former while serving the latter's records
// (merely renamed) means the identity inside the data never matches the identity of the session,
// and it is also the account with no history, which is exactly the account that has to call
// GameRecord/InitializeAttributes. Run as the account we actually have a complete history for.
// Both ids are 22 characters, so captured payloads can be realigned by a straight string swap.
// Override with NPLN_CAPTURED_USER to go back to the friends-capture identity.
const capturedUserGameRecord = "u-exemple7000000000000"
const capturedUserFriends = "u-exemple5000000000000"

var capturedUser = func() string {
	if v := os.Getenv("NPLN_CAPTURED_USER"); v != "" {
		return v
	}
	return capturedUserGameRecord
}()

// alignCapturedIdentity swaps the two captured account ids so a replayed body is self-consistent
// with the identity the session actually runs as. Same length, so this never disturbs protobuf
// length prefixes.
func alignCapturedIdentity(b []byte) []byte {
	other := capturedUserFriends
	if capturedUser == capturedUserFriends {
		other = capturedUserGameRecord
	}

	out := bytes.ReplaceAll(b, []byte(other), []byte(capturedUser))
	return out
}

func allowUnverified() bool { return os.Getenv("NPLN_ALLOW_UNVERIFIED") != "" }

// A prior server-issued JWT can remain cryptographically valid for its TTL after an upgrade.
// Do not treat the signature alone as proof that the PID/UID is still backed by a live
// Nextendo account: every authenticated RPC checks the account service, with a short cache to
// avoid an HTTP lookup for every high-frequency game call.
const accountIdentityCacheTTL = 15 * time.Second

type accountIdentityCacheKey struct {
	PID uint64
	UID string
}

type accountIdentityCacheEntry struct {
	account   nplnAccountData
	expiresAt time.Time
}

var accountIdentityCache = struct {
	sync.Mutex
	entries map[accountIdentityCacheKey]accountIdentityCacheEntry
}{entries: make(map[accountIdentityCacheKey]accountIdentityCacheEntry)}

func clearAccountIdentityCache() {
	accountIdentityCache.Lock()
	accountIdentityCache.entries = make(map[accountIdentityCacheKey]accountIdentityCacheEntry)
	accountIdentityCache.Unlock()
}

func rememberAccountIdentity(account *nplnAccountData) {
	if account == nil || account.PID == 0 {
		return
	}
	uid := cleanBanUID(account.UserID)
	if !validBanUID(uid) {
		return
	}
	now := time.Now()
	entry := accountIdentityCacheEntry{
		account:   nplnAccountData{PID: account.PID, UserID: uid, Verified: account.Verified, Disabled: account.Disabled},
		expiresAt: now.Add(accountIdentityCacheTTL),
	}
	key := accountIdentityCacheKey{PID: account.PID, UID: uid}
	accountIdentityCache.Lock()
	for cachedKey, cached := range accountIdentityCache.entries {
		if !now.Before(cached.expiresAt) {
			delete(accountIdentityCache.entries, cachedKey)
		}
	}
	if len(accountIdentityCache.entries) >= 4096 {
		for cachedKey := range accountIdentityCache.entries {
			delete(accountIdentityCache.entries, cachedKey)
			break
		}
	}
	accountIdentityCache.entries[key] = entry
	accountIdentityCache.Unlock()
}

// validateAccountIdentity ensures the signed token still maps to the canonical, enabled
// Nextendo account. Callers must check bans separately so a ban takes effect immediately.
func validateAccountIdentity(pid uint64, tokenUID string) (*nplnAccountData, error) {
	canonicalUID := cleanBanUID(tokenUID)
	if pid == 0 || !validBanUID(canonicalUID) || tokenUID != canonicalUID {
		return nil, status.Error(codes.Unauthenticated, "valid canonical Nextendo identity required")
	}

	key := accountIdentityCacheKey{PID: pid, UID: canonicalUID}
	now := time.Now()
	accountIdentityCache.Lock()
	if cached, ok := accountIdentityCache.entries[key]; ok && now.Before(cached.expiresAt) {
		account := cached.account
		accountIdentityCache.Unlock()
		return &account, nil
	}
	delete(accountIdentityCache.entries, key)
	accountIdentityCache.Unlock()

	account, err := accountFriends(pid)
	if err != nil {
		if accountLookupNotFound(err) {
			return nil, status.Error(codes.PermissionDenied, "Nextendo account not found")
		}
		log.Printf("[NPLN Auth] account lookup pid=%d unavailable: %v", pid, err)
		return nil, status.Error(codes.Unavailable, "Nextendo account lookup unavailable")
	}
	accountUID := cleanBanUID(account.UserID)
	if account.PID != pid || !validBanUID(accountUID) || accountUID != canonicalUID {
		return nil, status.Error(codes.PermissionDenied, "Nextendo account identity mismatch")
	}
	if account.Disabled {
		return nil, status.Error(codes.PermissionDenied, "Nextendo account is disabled")
	}
	if !account.Verified && !allowUnverified() {
		return nil, status.Error(codes.PermissionDenied, "Nextendo account not verified")
	}

	rememberAccountIdentity(account)

	return account, nil
}

func tenantOr(t string) string {
	if t == "" {
		return "tenants/t-dce9377b-lp1"
	}
	return t
}

// gatedIdentity resolves a cryptographically proven external identity to a Nextendo account
// and returns (pid, userPath). NPLN_ALLOW_UNVERIFIED may bypass email verification only; it
// never makes an unverified external identity or unknown account acceptable.
func gatedIdentity(ext *authpb.ExternalIdToken, tenant string) (uint64, string, error) {
	tenant = tenantOr(tenant)

	// An unresolvable identity is refused. It must never inherit a replay/capture identity.
	// IDENTITE NON RESOLUE = REFUS. C'est la regle que le projet s'est donnee, et elle n'etait pas
	// appliquee ici.
	//
	// ⚠️ CE QUE FAISAIT L'ANCIEN REPLI, ET CE QU'IL A COUTE. Il rendait pid=0 et l'identite du
	// compte CAPTURE — un compte partage, choisi pour qu'un attaquant ne puisse pas viser une
	// victime precise. Sauf que ce compte porte une vraie progression et une vraie sauvegarde : le
	// 2026-08-21, un joueur s'est connecte et a trouve le pseudo, le niveau et la progression de
	// quelqu'un d'autre. Deux joueurs tombes dedans en meme temps s'ecrasent mutuellement.
	//
	// Mesure du 2026-08-22 : le repli se declenche environ deux fois par heure, motif « NSA has no
	// Nextendo account ». Ce n'est pas une panne rare, c'est un flux continu.
	//
	// On refuse donc, franchement. Un joueur sans compte Nextendo resoluble ne joue pas en ligne —
	// il ne joue pas SOUS L'IDENTITE D'UN AUTRE. Le message est explicite pour qu'il sache quoi
	// faire au lieu de subir une erreur de communication opaque.
	//
	fallback := func(why string) (uint64, string, error) {
		log.Printf("[NPLN Auth] identite non resolue (%s) -> REFUS", why)
		return 0, "", status.Error(codes.PermissionDenied,
			"Nextendo account not recognised — sign in with your Nextendo account to play online")
	}
	// Do not authorize from parseSubsdkToken/account_bytes: its HMAC key is shared with the
	// client, so an emulator user can extract the key and forge account.dat bytes for any PID.
	// Account existence is not proof of account ownership. Login must carry the server-signed
	// Nextendo `nnex` token below.
	// [Nextendo] Chemin ÉMULATEUR : le id_token BAAS que Ryujinx fabrique porte, dans son claim
	// "nnex", le jeton nx2 signé HMAC par nextendo-account (payload "pid.username.expiry").
	// C'est la MÊME preuve que valident les serveurs NEX (server/NEXtendo/authbinding.go).
	// Indispensable ici parce que le claim `sub` de ce id_token est 16 octets ALÉATOIRES régénérés
	// à chaque émission (ManagerServer.cs :
	// RandomNumberGenerator.Fill(rawUserId)) : il ne peut structurellement pas être relié au
	// baas_id 64 bits du compte, d'où le pid=0 et la liste d'amis de capture servie à tout le monde.
	if pid, ok := pidFromNnex(ext); ok {
		log.Printf("[NPLN Auth] nnex prouve pid=%d", pid)
		if err := playerBans.requireAllowed(pid, ""); err != nil {
			return 0, "", err
		}
		acc, err := accountFriends(pid)
		if err != nil {
			return fallback("nnex pid sans compte Nextendo joignable")
		}
		if acc.Disabled {
			return 0, "", status.Error(codes.PermissionDenied, "Nextendo account is disabled")
		}
		if !acc.Verified && !allowUnverified() {
			return 0, "", status.Error(codes.PermissionDenied, "Nextendo account not verified — verify your e-mail to play online")
		}
		if err := playerBans.requireAllowed(pid, acc.UserID); err != nil {
			return 0, "", err
		}
		if acc.PID != pid || !validBanUID(acc.UserID) {
			return fallback("nnex pid/account mapping mismatch")
		}
		rememberAccountIdentity(acc)
		return pid, tenant + "/users/" + cleanBanUID(acc.UserID), nil
	}

	// The external Nintendo NSA JWT is not signed with a key we control, so its `sub` is
	// client-controlled. Never turn it into a Nextendo PID, even when email verification is
	// disabled for development. The client-shared subsdk HMAC is not identity proof; only the
	// Nextendo-signed nnex token above can authorize this client flow.
	return fallback("unverified external NSA token")
}

// newTokenPID mints an access token that embeds the account PID so later calls
// (friends/presence) can identify the player without shared state.
//
// The access token is a REAL ES256 JWT (see token_jwt.go), not the opaque string we used
// until 2026-07-16. The 2026-06-28 capture shows Nintendo's token carries the player's online
// rights in its claims (npln.authorization: allow ["**"], nso_restricted false) — npl1 reads
// them to know what it may do, so an undecodable token reads as "no rights" and the game
// declares itself offline (no stages) even though every RPC succeeds.
// identitesParUid retient la correspondance uid -> PID, etablie a l'authentification.
//
// POURQUOI ELLE EXISTE. Le PID voyage dans le JETON ; l'uid voyage dans un EN-TETE de metadonnees
// (uidFromCtx). Ce sont deux canaux independants, et certains flux longs — KeepAlive notamment —
// nous parviennent avec l'uid mais sans jeton exploitable. Le serveur connaissait alors le joueur
// par son uid tout en ignorant son PID, et le declarait au suivi sous « # 0 » : ni pseudo, ni photo
// de profil, ni mode, ni NAT, ni ping, pour toute la duree de la session.
//
// L'authentification, elle, voit LES DEUX en meme temps. On note donc l'appariement au seul endroit
// ou tout jeton est frappe, et on s'en sert de recours quand le jeton manque a l'appel.
var identitesParUid = struct {
	sync.Mutex
	m map[string]uint64
}{m: map[string]uint64{}}

func retenirIdentite(userPath string, pid uint64) {
	uid := userPath
	if i := strings.LastIndexByte(uid, '/'); i >= 0 {
		uid = uid[i+1:]
	}
	if uid == "" || pid == 0 {
		return
	}
	identitesParUid.Lock()
	identitesParUid.m[uid] = pid
	identitesParUid.Unlock()
}

func pidPourUid(uid string) uint64 {
	identitesParUid.Lock()
	defer identitesParUid.Unlock()
	return identitesParUid.m[uid]
}

// pidDeLAppelant rend le PID de l'appelant : par le jeton signe, sinon par une correspondance
// uid -> PID deja etablie a l'authentification.
//
// Le recours n'affaiblit rien : il ne sert qu'a NOMMER un joueur pour le suivi, jamais a lui
// accorder un droit. Les chemins qui accordent quelque chose exigent toujours callerPID.
func pidDeLAppelant(ctx context.Context) uint64 {
	if pid, ok := callerPID(ctx); ok && pid != 0 {
		return pid
	}
	return pidPourUid(uidFromCtx(ctx))
}

func newTokenPID(pid uint64, userPath string) *authpb.Token {
	retenirIdentite(userPath, pid)
	access := mintNplnAccessToken(pid, userPath, npnTenant)
	if access == "" {
		return &authpb.Token{}
	}

	return &authpb.Token{
		User:         userPath,
		AccessToken:  access,
		RefreshToken: jetonRafraichissement(pid),
		Ttl:          durationpb.New(nplnTokenTTL),
	}
}

// jetonRafraichissement fabrique « nextendo-npln-refresh.<pid>.<signature> ».
//
// ⚠️ L'ANCIENNE FORME NE PORTAIT QU'UN NUMERO. Elle etait donc trivialement forgeable — il
// suffisait d'ecrire le PID de quelqu'un d'autre — et, pire, elle ne permettait PAS de prouver
// une identite au rafraichissement, ce qui a produit le bug ci-dessous. On la signe avec le meme
// secret partage que le reste, pour qu'un rafraichissement puisse etablir QUI le demande.
func jetonRafraichissement(pid uint64) string {
	secret := loadNextendoSecret()
	if pid == 0 || len(secret) == 0 {
		return ""
	}
	corps := fmt.Sprintf("nextendo-npln-refresh.%d", pid)

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(corps))

	return corps + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// pidDuJetonRafraichissement rend le PID prouve par un jeton de rafraichissement signe.
func pidDuJetonRafraichissement(tok string) (uint64, bool) {
	secret := loadNextendoSecret()
	if len(secret) == 0 {
		return 0, false
	}
	i := strings.LastIndexByte(tok, '.')
	if i <= 0 {
		return 0, false
	}
	corps, sig := tok[:i], tok[i+1:]

	const prefix = "nextendo-npln-refresh."
	if !strings.HasPrefix(corps, prefix) {
		return 0, false
	}
	pid, err := strconv.ParseUint(strings.TrimPrefix(corps, prefix), 10, 64)
	if err != nil || pid == 0 {
		return 0, false
	}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(corps))
	attendu := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(attendu), []byte(sig)) {
		return 0, false
	}

	return pid, true
}

func (s *authServer) CreateUser(ctx context.Context, req *authpb.CreateUserRequest) (*authpb.User, error) {
	pid, userPath, err := gatedIdentity(req.GetExternalIdToken(), req.GetParent())
	if err != nil {
		return nil, err
	}
	log.Printf("[NPLN Auth] CreateUser parent=%q pid=%d -> %s", req.GetParent(), pid, userPath)
	return &authpb.User{Name: userPath, Account: req.GetParent(), ShortId: 1}, nil
}

func (s *authServer) IssueToken(ctx context.Context, req *authpb.IssueTokenRequest) (*authpb.IssueTokenResponse, error) {
	pid, userPath, err := gatedIdentity(req.GetExternalIdToken(), "")
	if err != nil {
		log.Printf("[NPLN Auth] IssueToken DENIED ext=%s: %v", describe(req.GetExternalIdToken()), err)
		return nil, err
	}
	log.Printf("[NPLN Auth] IssueToken pid=%d user=%s", pid, userPath)
	return &authpb.IssueTokenResponse{Token: newTokenPID(pid, userPath)}, nil
}

// RefreshToken re-emet un jeton pour une identite DEJA PROUVEE.
//
// ⚠️ LE TROU QUI RENDAIT TOUTE UNE SESSION ANONYME (mesure du 2026-08-16).
//
// Cette methode ecrivait « pid, _ := callerPID(ctx) » : l'echec etait JETE. Quand l'appel arrivait
// sans jeton d'acces exploitable — ce qui est le cas normal d'un rafraichissement, le client
// presentant son jeton de RAFRAICHISSEMENT — callerPID rendait (0, false), et nous frappions un
// jeton neuf, parfaitement signe, portant « ext_id: 0 ».
//
// Le client s'en servait ensuite pour TOUT. L'authentification affichait pourtant le bon PID :
//
//	[NPLN Auth]     IssuePrearrangedUserToken pid=1800000042
//	[NPLN Friends]  Subscribe                 pid=1800000042 -> 43 ami(s)
//	[NPLN Presence] pid=0: npln-friends pid=0: 400 Bad Request   <- toutes les 20 s, jusqu'a la fin
//
// La presence tournait donc sans identite : ni pseudo, ni photo de profil, ni mode, ni NAT, ni
// ping dans le suivi — le joueur s'affichait « # 0 ». Tous les autres emetteurs passent par
// gatedIdentity, qui refuse ; celui-ci etait le seul a ne rien exiger.
//
// On accepte desormais deux preuves, et on REFUSE a defaut : un jeton anonyme ne vaut pas mieux
// que pas de jeton du tout, il coute juste une session entiere a diagnostiquer.
func (s *authServer) RefreshToken(ctx context.Context, req *authpb.RefreshTokenRequest) (*authpb.RefreshTokenResponse, error) {
	pid, _, ok := callerIdentityFromJWT(ctx)

	if !ok || pid == 0 {
		pid, ok = pidDuJetonRafraichissement(req.GetRefreshToken())
	}

	if !ok || pid == 0 {
		log.Printf("[NPLN Auth] RefreshToken REFUSE : aucune identite prouvee (user=%q) — le client doit se reauthentifier", req.GetUser())

		return nil, status.Error(codes.Unauthenticated, "refresh token without a proven identity")
	}
	if err := playerBans.requireAllowed(pid, ""); err != nil {
		return nil, err
	}
	acc, err := accountFriends(pid)
	if err != nil {
		if accountLookupNotFound(err) {
			return nil, status.Error(codes.PermissionDenied, "Nextendo account not found")
		}
		log.Printf("[NPLN Auth] RefreshToken pid=%d : account lookup failed: %v", pid, err)
		return nil, status.Error(codes.Unavailable, "Nextendo account lookup unavailable")
	}
	uid := cleanBanUID(acc.UserID)
	if acc.PID != pid || !validBanUID(uid) {
		return nil, status.Error(codes.PermissionDenied, "Nextendo account identity mismatch")
	}
	if acc.Disabled {
		return nil, status.Error(codes.PermissionDenied, "Nextendo account is disabled")
	}
	if !acc.Verified && !allowUnverified() {
		return nil, status.Error(codes.PermissionDenied, "Nextendo account not verified")
	}
	if requestedUID := userIDFromPath(req.GetUser()); requestedUID != "" && requestedUID != uid {
		return nil, status.Error(codes.PermissionDenied, "refresh token user does not match the Nextendo account")
	}
	if err := playerBans.requireAllowed(pid, uid); err != nil {
		return nil, err
	}
	rememberAccountIdentity(acc)

	userPath := tenantOr("") + "/users/" + uid
	log.Printf("[NPLN Auth] RefreshToken pid=%d user=%s", pid, userPath)

	return &authpb.RefreshTokenResponse{Token: newTokenPID(pid, userPath)}, nil
}

func (s *authServer) IssuePrearrangedUserToken(ctx context.Context, req *authpb.IssuePrearrangedUserTokenRequest) (*authpb.IssuePrearrangedUserTokenResponse, error) {
	pid, userPath, err := gatedIdentity(req.GetExternalIdToken(), req.GetTenant())
	if err != nil {
		log.Printf("[NPLN Auth] IssuePrearrangedUserToken DENIED ext=%s: %v", describe(req.GetExternalIdToken()), err)
		return nil, err
	}
	user := &authpb.User{Name: userPath, ShortId: int64(req.GetUserIndex())}
	log.Printf("[NPLN Auth] IssuePrearrangedUserToken tenant=%q pid=%d user=%s", req.GetTenant(), pid, userPath)
	return &authpb.IssuePrearrangedUserTokenResponse{User: user, Token: newTokenPID(pid, userPath)}, nil
}

func (s *authServer) IssueAnonymousUserToken(ctx context.Context, req *authpb.IssueAnonymousUserTokenRequest) (*authpb.IssueAnonymousUserTokenResponse, error) {
	pid, userPath, err := gatedIdentity(req.GetExternalIdToken(), req.GetTenant())
	if err != nil {
		log.Printf("[NPLN Auth] IssueAnonymousUserToken DENIED: %v", err)
		return nil, err
	}
	log.Printf("[NPLN Auth] IssueAnonymousUserToken pid=%d", pid)
	return &authpb.IssueAnonymousUserTokenResponse{Token: newTokenPID(pid, userPath)}, nil
}

// ValidateToken enforces the online gate on every check: the signed bearer token must map to an
// existing Nextendo account. NPLN_ALLOW_UNVERIFIED only skips email verification.
// ⚠️ CETTE PORTE NE JOURNALISAIT QUE SES SUCCES. Un refus partait en PermissionDenied SANS une
// ligne, si bien que l'absence de trace pour un joueur etait ambigue : « son client n'appelle pas »
// et « il est refuse ici » se ressemblaient. Mesure du 2026-08-13 : trois joueurs, seul celui qui
// passe en ligne produisait « verified -> OK » ; impossible de savoir si les deux autres etaient
// refuses. Chaque issue est desormais tracee, y compris les deux replis silencieux.
func (s *authServer) ValidateToken(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	pid, tokenUID, ok := callerIdentityFromJWT(ctx)
	if !ok || pid == 0 {
		log.Printf("[NPLN Auth] ValidateToken refused: missing or invalid signed identity (ok=%v)", ok)
		return nil, status.Error(codes.Unauthenticated, "valid Nextendo identity required")
	}
	if err := playerBans.requireAllowed(pid, ""); err != nil {
		return nil, err
	}

	acc, err := validateAccountIdentity(pid, tokenUID)
	if err != nil {
		log.Printf("[NPLN Auth] ValidateToken pid=%d refused: %v", pid, err)
		return nil, err
	}
	uid := acc.UserID
	if err := playerBans.requireAllowed(pid, uid); err != nil {
		return nil, err
	}

	log.Printf("[NPLN Auth] ValidateToken pid=%d uid=%s verified -> OK", pid, uid)

	return &emptypb.Empty{}, nil
}
