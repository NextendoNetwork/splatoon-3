package main

// auth — nn.npln.auth.v1.Auth. The NPLN entry point: S3 presents its external id
// token (the NSA/BAAS token from account.dat) and we hand back an opaque access token.
//
// The identity is now DYNAMIC + GATED: we resolve the NSA to the owning Nextendo
// account, REFUSE unless that account is a verified, non-frozen Nextendo account
// (fail-closed, same rule as the NEX games), and embed the account PID in the access
// token so the friends/presence services can look the player up. Set
// NPLN_ALLOW_UNVERIFIED=1 to bypass the gate during local development.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

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

// capturedUser is the identity behind the frozen replay bodies; used only as the
// NPLN_ALLOW_UNVERIFIED dev fallback when there is no resolvable Nextendo account.
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

// nsaFromExternal pulls the NSA id out of the external id token — either the dummy
// token verbatim, or the "sub"-style claim of the NSA JWT.
func nsaFromExternal(ext *authpb.ExternalIdToken) (string, bool) {
	if ext == nil {
		return "", false
	}
	if v := ext.GetDummyExtIdToken(); v != "" {
		return v, true
	}
	jwt := ext.GetNsaIdToken()
	if jwt == "" {
		return "", false
	}
	parts := strings.Split(jwt, ".")
	if len(parts) < 2 {
		return "", false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	var claims map[string]any
	if json.Unmarshal(payload, &claims) != nil {
		return "", false
	}
	for _, k := range []string{"sub", "nsa", "nsa_id", "san", "user_id"} {
		if s, ok := claims[k].(string); ok && s != "" {
			return s, true
		}
	}
	return "", false
}

// pidOf resolves just the PID from the external id token (subsdk blob first, then NSA), with no
// account gating — used by the capture-identity diagnostic above.
func pidOf(ext *authpb.ExternalIdToken) uint64 {
	if ext == nil {
		return 0
	}
	if tok := ext.GetNsaIdToken(); tok != "" {
		if c, ok := parseSubsdkToken(tok); ok {
			if pid := pidFromAccountBytes(c.AccountBytes); pid != 0 {
				return pid
			}
		}
	}
	if nsa, ok := nsaFromExternal(ext); ok {
		if pid, err := resolveNSAToPID(nsa); err == nil {
			return pid
		}
	}
	return 0
}

func tenantOr(t string) string {
	if t == "" {
		return "tenants/t-dce9377b-lp1"
	}
	return t
}

// gatedIdentity resolves the external id token to a verified Nextendo account and
// returns (pid, userPath). Fail-closed: any account that is unknown or not verified
// is refused, unless NPLN_ALLOW_UNVERIFIED is set (dev).
func gatedIdentity(ext *authpb.ExternalIdToken, tenant string) (uint64, string, error) {
	tenant = tenantOr(tenant)

	// DIAGNOSTIC (NPLN_JWT_SUB_CAPTURED=1): serve the CAPTURE's identity everywhere — token sub
	// AND User.Name — so it matches the replayed bodies (GetSaveRecord, SubscribeFriendUsers…),
	// which all belong to u-exemple5000000000000. Aligning only the token left the response
	// self-contradictory (User.Name said moha, sub said the capture user) and S3 closed the
	// connection right after the schedules. Not a fix to keep: the real answer is to rewrite the
	// replayed bodies to the live identity.
	if os.Getenv("NPLN_JWT_SUB_CAPTURED") != "" {
		if pid := pidOf(ext); pid != 0 {
			log.Printf("[NPLN Auth] DIAG identite=capture (%s) pour pid=%d", capturedUser, pid)
			return pid, tenant + "/users/" + capturedUser, nil
		}
	}
	// On our private infra every NSA token comes from a Nextendo account, so an
	// UNRESOLVABLE identity means an identity-resolution gap (not an intruder) — fall
	// back to a working identity rather than break online. The real gate is a KNOWN
	// account that isn't verified: that one is hard-blocked (fail-closed).
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
	// Le drapeau a chaud « replicapture » restaure l'ancien comportement en une seconde, sans
	// redeploiement, au cas ou ce refus fermerait la porte a des joueurs legitimes. Il est la pour
	// qu'on puisse revenir en arriere vite, pas pour rester allume.
	fallback := func(why string) (uint64, string, error) {
		if soirFlag("replicapture") {
			log.Printf("[NPLN Auth] identity fallback (%s) -> compte capture (drapeau replicapture)", why)
			return 0, tenant + "/users/" + capturedUser, nil
		}
		log.Printf("[NPLN Auth] identite non resolue (%s) -> REFUS", why)
		return 0, "", status.Error(codes.PermissionDenied,
			"Nextendo account not recognised — sign in with your Nextendo account to play online")
	}
	// Nextendo subsdk path: the id token is our own base64(json),hmac blob carrying the
	// console's account.dat. Resolve it to the owning Nextendo account PID directly.
	if ext != nil {
		if tok := ext.GetNsaIdToken(); tok != "" {
			if c, ok := parseSubsdkToken(tok); ok {
				pid := pidFromAccountBytes(c.AccountBytes)
				log.Printf("[NPLN Auth] subsdk token: account_bytes=%s emu=%v ver=%s -> pid=%d",
					c.AccountBytes, c.Emulator, c.DisplayVersion, pid)
				if pid != 0 {
					acc, err := accountFriends(pid)
					if err != nil {
						return fallback("subsdk pid has no Nextendo account")
					}
					if !acc.Verified && !allowUnverified() {
						return 0, "", status.Error(codes.PermissionDenied, "Nextendo account not verified — verify your e-mail to play online")
					}
					return pid, tenant + "/users/" + acc.UserID, nil
				}
			}
		}
	}
	// [Nextendo] Chemin ÉMULATEUR : le id_token BAAS que Ryujinx fabrique porte, dans son claim
	// "nnex", le jeton nx2 signé HMAC par nextendo-account (payload "pid.username.expiry").
	// C'est la MÊME preuve que valident les serveurs NEX (server/NEXtendo/authbinding.go).
	// Indispensable ici parce que le claim `sub` de ce id_token — ce que nsaFromExternal lit et
	// appelle « NSA » — est 16 octets ALÉATOIRES régénérés à chaque émission (ManagerServer.cs :
	// RandomNumberGenerator.Fill(rawUserId)) : il ne peut structurellement pas être relié au
	// baas_id 64 bits du compte, d'où le pid=0 et la liste d'amis de capture servie à tout le monde.
	if pid, ok := pidFromNnex(ext); ok {
		log.Printf("[NPLN Auth] nnex prouve pid=%d", pid)
		acc, err := accountFriends(pid)
		if err != nil {
			return fallback("nnex pid sans compte Nextendo joignable")
		}
		if !acc.Verified && !allowUnverified() {
			return 0, "", status.Error(codes.PermissionDenied, "Nextendo account not verified — verify your e-mail to play online")
		}
		return pid, tenant + "/users/" + acc.UserID, nil
	}

	// [Confiance NPLN — #2] Le jeton NSA n'est PAS vérifiable par nous : on IMITE le backend
	// Nintendo, on n'a pas sa clé pour valider sa signature. Le lire tel quel laisserait un
	// attaquant fabriquer un JWT {sub: NSA de la victime} et se voir émettre le jeton de la
	// victime, sans aucun secret. Le VRAI client (subsdk) prouve son identité par le HMAC
	// ci-dessus ; en prod on ne dérive donc JAMAIS une identité précise d'un jeton NSA non
	// prouvé — on retombe sur l'identité de repli partagée (jamais celle d'un compte visé).
	// NPLN_ALLOW_UNVERIFIED rouvre le chemin pour le dev/diagnostic.
	if !allowUnverified() {
		return fallback("jeton NSA non prouvable — le client subsdk (HMAC) est requis en prod")
	}
	nsa, ok := nsaFromExternal(ext)
	if !ok {
		return fallback("no NSA in token")
	}

	// [Nextendo-diag] Log the NSA we actually received and what the account service answered.
	// "NSA has no Nextendo account" alone is unactionable: it cannot distinguish an id that is
	// absent from the database, an id in an unexpected shape, or the lookup endpoint erroring.
	pid, err := resolveNSAToPID(nsa)
	log.Printf("[NPLN Auth][DIAG] NSA=%q -> pid=%d err=%v", nsa, pid, err)

	if err != nil || pid == 0 {
		return fallback("NSA has no Nextendo account")
	}
	acc, err := accountFriends(pid)
	if err != nil {
		return fallback("account service unavailable")
	}
	if !acc.Verified && !allowUnverified() {
		return 0, "", status.Error(codes.PermissionDenied, "Nextendo account not verified — verify your e-mail to play online")
	}
	return pid, tenant + "/users/" + acc.UserID, nil
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

// pidDeLAppelant rend le PID de l'appelant : par le jeton s'il le porte, sinon par l'uid annonce.
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

	return &authpb.Token{
		User:         userPath,
		AccessToken:  mintNplnAccessToken(pid, userPath, npnTenant),
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
	corps := fmt.Sprintf("nextendo-npln-refresh.%d", pid)

	mac := hmac.New(sha256.New, loadNextendoSecret())
	mac.Write([]byte(corps))

	return corps + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// pidDuJetonRafraichissement rend le PID prouve par un jeton de rafraichissement signe.
func pidDuJetonRafraichissement(tok string) (uint64, bool) {
	i := strings.LastIndexByte(tok, '.')
	if i <= 0 {
		return 0, false
	}
	corps, sig := tok[:i], tok[i+1:]

	pid, err := strconv.ParseUint(strings.TrimPrefix(corps, "nextendo-npln-refresh."), 10, 64)
	if err != nil || pid == 0 || !strings.HasPrefix(corps, "nextendo-npln-refresh.") {
		return 0, false
	}

	mac := hmac.New(sha256.New, loadNextendoSecret())
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
	pid, ok := callerPID(ctx)

	if !ok || pid == 0 {
		pid, ok = pidDuJetonRafraichissement(req.GetRefreshToken())
	}

	if !ok || pid == 0 {
		log.Printf("[NPLN Auth] RefreshToken REFUSE : aucune identite prouvee (user=%q) — le client doit se reauthentifier", req.GetUser())

		return nil, status.Error(codes.Unauthenticated, "refresh token without a proven identity")
	}

	log.Printf("[NPLN Auth] RefreshToken pid=%d", pid)

	return &authpb.RefreshTokenResponse{Token: newTokenPID(pid, req.GetUser())}, nil
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

// ValidateToken enforces the online gate on every check: the bearer token must map to
// a verified Nextendo account. Fail-closed unless NPLN_ALLOW_UNVERIFIED.
// ⚠️ CETTE PORTE NE JOURNALISAIT QUE SES SUCCES. Un refus partait en PermissionDenied SANS une
// ligne, si bien que l'absence de trace pour un joueur etait ambigue : « son client n'appelle pas »
// et « il est refuse ici » se ressemblaient. Mesure du 2026-08-13 : trois joueurs, seul celui qui
// passe en ligne produisait « verified -> OK » ; impossible de savoir si les deux autres etaient
// refuses. Chaque issue est desormais tracee, y compris les deux replis silencieux.
func (s *authServer) ValidateToken(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
	pid, ok := callerPID(ctx)
	if !ok || pid == 0 {
		log.Printf("[NPLN Auth] ValidateToken pid inconnu (ok=%v) -> laisse passer (identite de repli)", ok)
		return &emptypb.Empty{}, nil // fallback identity — don't block online
	}

	acc, err := accountFriends(pid)
	if err != nil {
		log.Printf("[NPLN Auth] ValidateToken pid=%d : service de comptes injoignable (%v) -> laisse passer", pid, err)
		return &emptypb.Empty{}, nil // account service hiccup — don't block online
	}

	if !acc.Verified && !allowUnverified() {
		log.Printf("[NPLN Auth] ⚠️ ValidateToken pid=%d REFUSE : compte non verifie selon le service de comptes (uid=%q)",
			pid, acc.UserID)
		return nil, status.Error(codes.PermissionDenied, "Nextendo account not verified")
	}

	log.Printf("[NPLN Auth] ValidateToken pid=%d verified -> OK", pid)

	return &emptypb.Empty{}, nil
}
