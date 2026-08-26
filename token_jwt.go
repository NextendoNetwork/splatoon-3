package main

// token_jwt — mint the NPLN access token as a REAL signed JWT, the way Nintendo does.
//
// Why this exists (2026-07-16): we used to hand S3 the opaque string
// "nextendo-npln-access.<pid>". The MITM capture of 2026-06-28
// (dist/Nextendo-MITM-Splatoon3v3/capture_log.txt — decrypted HTTP/2, headers in clear)
// shows Nintendo's real answer is an ES256 JWT whose payload carries the player's ONLINE
// RIGHTS:
//
//	header  {"alg":"ES256","jku":"jwkSets/nplnAccessToken","kid":"<uuid>"}
//	payload {"exp":…, "iat":…, "iss":"default iss", "sub":"u-…",
//	         "npln":{"aid":"a-…","app_id":"0100c2500fc20000",
//	                 "authorization":{"allow":["**"],"deny":[],"nso_restricted":false},
//	                 "ext_id":"…","ext_id_type":1,"tid":"t-dce9377b-lp1"}}
//
// npl1 reads those claims to know what it may do. Handed a token it cannot decode, it finds
// no rights at all — which matches every symptom we chased: auth "succeeds", the schedules
// arrive with no error, and the game still declares itself offline ("Stage information is not
// available offline"). exp-iat in the capture is exactly 28800s = the 8h TTL we already use.

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	nplnJKU      = "jwkSets/nplnAccessToken"
	nplnIssuer   = "default iss"
	nplnAppID    = "0100c2500fc20000" // Splatoon 3
	nplnTokenTTL = 8 * time.Hour      // matches Nintendo's exp-iat = 28800s
	// Le jeton `gss` d'une SESSION vit moins longtemps : la capture donne exp - iat = 3600 s.
	gssTokenTTL = 1 * time.Hour
)

var (
	jwtOnce sync.Once
	jwtKey  *ecdsa.PrivateKey
	jwtKID  string
)

// nplnSigningKey lazily creates the ES256 key we sign access tokens with. It is generated at
// startup rather than pinned: the game gets the key id via the header's `kid`, and we serve the
// matching JWK set (see jwks.go) — nothing external needs to know it in advance.
func nplnSigningKey() (*ecdsa.PrivateKey, string) {
	jwtOnce.Do(func() {
		jwtKID = "359180bf-c2d5-493a-baa0-5f289ebbf566" // same shape as Nintendo's (a uuid)
		// La clé DOIT survivre à un redémarrage. Depuis qu'on VÉRIFIE la signature des jetons
		// d'accès (friends.go pidFromJWT), une clé régénérée à chaque boot invaliderait tous les
		// jetons en cours et déconnecterait tout le monde de NPLN. On la persiste donc.
		path := envOr("NPLN_JWT_KEY", "npln_jwt_es256.key")
		if b, err := os.ReadFile(path); err == nil {
			if blk, _ := pem.Decode(b); blk != nil {
				if k, err := x509.ParseECPrivateKey(blk.Bytes); err == nil {
					jwtKey = k
					return
				}
			}
		}
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			log.Printf("[NPLN Auth] ecdsa key: %v", err)
			return
		}
		jwtKey = k
		if der, err := x509.MarshalECPrivateKey(k); err == nil {
			if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o600); err != nil {
				log.Printf("[NPLN Auth] clé ES256 non persistée (%v) : elle changera au prochain boot", err)
			}
		}
	})
	return jwtKey, jwtKID
}

// verifyNplnAccessToken vérifie la signature ES256 de "<headerB64>.<payloadB64>.<sigB64>" avec
// NOTRE clé — celle qui a servi à l'émettre. C'est ce qui empêche un tiers de forger un jeton
// portant l'ext_id (donc l'identité) de quelqu'un d'autre et d'agir sous son compte.
func verifyNplnAccessToken(headerB64, payloadB64, sigB64 string) bool {
	key, _ := nplnSigningKey()
	if key == nil {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil || len(sig) != 64 {
		return false
	}
	sum := sha256.Sum256([]byte(headerB64 + "." + payloadB64))
	r := new(big.Int).SetBytes(sig[:32])
	s := new(big.Int).SetBytes(sig[32:])
	return ecdsa.Verify(&key.PublicKey, sum[:], r, s)
}

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// accountIDFromUser derives the "aid" from the user id. In the capture the two share every
// character but the first: sub="u-exemple5000000000000" vs aid="a-aoahvkaf4bclq6uqu6in". We
// mirror that shape; npl1 only needs a well-formed value here, the rights live in
// npln.authorization.
func accountIDFromUser(userID string) string {
	body := strings.TrimPrefix(userID, "u-")
	if body == "" {
		return "a-nextendo"
	}
	return "a-a" + body[1:]
}

// userIDFromPath turns "tenants/t-dce9377b-lp1/users/u-4k5…" into "u-4k5…".
func userIDFromPath(userPath string) string {
	if i := strings.LastIndex(userPath, "/"); i >= 0 {
		return userPath[i+1:]
	}
	return userPath
}

// mintNplnAccessToken builds the ES256 JWT S3 expects, granting full rights (allow ["**"],
// nso_restricted false) exactly as Nintendo's own token does for a licensed player.
func mintNplnAccessToken(pid uint64, userPath, tenant string) string {
	key, kid := nplnSigningKey()
	if key == nil {
		return "nextendo-npln-access." + itoa(pid) // last-resort fallback: never silently fail
	}

	uid := userIDFromPath(userPath)

	// DIAGNOSTIC (NPLN_JWT_SUB_CAPTURED=1): mint the token for the CAPTURE's user instead of the
	// Nextendo one. The opaque token left the game without a firm identity; the JWT gives it one
	// via `sub`, and every replayed body we serve (GetSaveRecord, SubscribeFriendUsers…) belongs
	// to the capture user u-exemple5000000000000. If S3 is waiting on data about *itself* that
	// never arrives, aligning sub with the replays unblocks it — and proves the mismatch is the
	// cause. Not a fix to keep: the real answer is to rewrite the replays to the live identity.
	if os.Getenv("NPLN_JWT_SUB_CAPTURED") != "" {
		uid = capturedUser
	}
	now := time.Now()

	header := map[string]any{"alg": "ES256", "jku": nplnJKU, "kid": kid}
	payload := map[string]any{
		"exp": now.Add(nplnTokenTTL).Unix(),
		"iat": now.Unix(),
		"iss": nplnIssuer,
		"sub": uid,
		"npln": map[string]any{
			"aid":    accountIDFromUser(uid),
			"app_id": nplnAppID,
			"authorization": map[string]any{
				"allow":          []string{"**"},
				"deny":           []string{},
				"nso_restricted": false,
			},
			// ext_id ties the token to the console account; the PID is our equivalent and
			// keeps friends/presence able to identify the caller from the token alone.
			"ext_id":      hex16(pid),
			"ext_id_type": 1,
			"tid":         strings.TrimPrefix(tenant, "tenants/"),
		},
	}

	hj, _ := json.Marshal(header)
	pj, _ := json.Marshal(payload)
	signing := b64u(hj) + "." + b64u(pj)

	sum := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, key, sum[:])
	if err != nil {
		log.Printf("[NPLN Auth] sign: %v", err)
		return "nextendo-npln-access." + itoa(pid)
	}
	// JWS ES256: r||s, each left-padded to 32 bytes (NOT the ASN.1 DER form).
	sig := make([]byte, 64)
	copyRightAligned(sig[:32], r)
	copyRightAligned(sig[32:], s)

	return signing + "." + b64u(sig)
}

// mintSessionToken issues the ES256 "gss" (game-session) JWT that goes into
// MatchedUserSession.matchmaking_id_token when a room is created/matched. Nintendo's client
// (Pia over NPLN) PARSES and verifies this as a real JWT — the opaque placeholder we used
// before ("nextendo-gss.<b64>", not even 3 dot-separated parts) failed that parse, so MP
// Jamboree's host rejected the SUCCEEDED creation ticket synchronously and bounced back to
// the online menu (no STUN, no further RPC — a pure client-side reject). We sign it with the
// SAME key + jku/kid as the access token (jwks.go already serves that JWK set), so it both
// decodes and verifies. Claims mirror the access token plus a `gss` block binding the token
// to this game session + user session.
// appIDForTenant maps an NPLN tenant to the game's title id. The session JWT's app_id must match
// the calling GAME (S3's 0100c2500fc20000 is wrong for MP Jamboree); a mismatch can make the host
// reject its own session token.
func appIDForTenant(tenant string) string {
	switch {
	case strings.Contains(tenant, "adf89f68"): // Mario Party Jamboree
		return "0100965017338000"
	case strings.Contains(tenant, "dce9377b"): // Splatoon 3
		return "0100c2500fc20000"
	}
	return nplnAppID
}

func mintSessionToken(uid, tenant, gsName, userSess string) string {
	key, kid := nplnSigningKey()
	if key == nil {
		return "nextendo-gss.fallback" // never silently fail
	}
	if uid == "" {
		uid = capturedUser
	}
	now := time.Now()

	header := map[string]any{"alg": "ES256", "jku": nplnJKU, "kid": kid}
	payload := map[string]any{
		"exp": now.Add(nplnTokenTTL).Unix(),
		"iat": now.Unix(),
		"iss": nplnIssuer,
		"sub": uid,
		"npln": map[string]any{
			"aid":    accountIDFromUser(uid),
			"app_id": appIDForTenant(tenant),
			"authorization": map[string]any{
				"allow":          []string{"**"},
				"deny":           []string{},
				"nso_restricted": false,
			},
			"ext_id_type": 1,
			"tid":         strings.TrimPrefix(tenant, "tenants/"),
		},
		// Session binding: ties this token to the room + the host's user session.
		"gss": map[string]any{
			"game_session": gsName,
			"user_session": userSess,
		},
	}

	hj, _ := json.Marshal(header)
	pj, _ := json.Marshal(payload)
	signing := b64u(hj) + "." + b64u(pj)

	sum := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, key, sum[:])
	if err != nil {
		log.Printf("[NPLN MM] session token sign: %v", err)
		return "nextendo-gss.fallback"
	}
	sig := make([]byte, 64)
	copyRightAligned(sig[:32], r)
	copyRightAligned(sig[32:], s)

	return signing + "." + b64u(sig)
}

// mintGssMatchToken issues the ES256 "gss" JWT that a PUBLIC matchmade session carries in
// MatchedUserSession.matchmaking_id_token. Modelled byte-for-byte on a real S3 Turf War capture
// (le corpus de captures NPLN, 2026-08-07): unlike the access token, iss is literally "gss" and the
// claims live under a `gamesync` block (attr = the match attributes as a typed-value JSON string,
// gsid = game-session id, ltcy = per-region latencies JSON, usid = user-session id, tid/typ/uid/team).
// The old opaque "nextendo-gss.<b64>" failed the client's JWT parse; this decodes+verifies via our
// jku/kid (jwks.go). attrJSON/ltcyJSON are built by the caller from the ticket's UserDefinition.
func mintGssMatchToken(uid, tenant, gsName, userSess, team, attrJSON, ltcyJSON string) string {
	key, kid := nplnSigningKey()
	if key == nil {
		return "nextendo-gss.fallback"
	}
	if uid == "" {
		uid = capturedUser
	}
	now := time.Now()
	tid := strings.TrimPrefix(tenant, "tenants/")
	gsid := userIDFromPath(gsName)   // last path segment (the uuid)
	usid := userIDFromPath(userSess) // last path segment (the uuid)
	if attrJSON == "" {
		attrJSON = "{}"
	}
	if ltcyJSON == "" {
		ltcyJSON = "{\"latencies\":{}}"
	}

	// Le jeton `gss` d'une session n'a PAS la forme du jeton d'ACCÈS. La capture
	// (captured/TrackGameSessionCreationTicket.bin, MatchedUserSession champ 3, 952 o) porte un
	// en-tête à DEUX membres — {"alg":"ES256","kid":"754fe17e-…"} — sans `jku`, et une durée de vie
	// d'une heure (exp 1786143269 − iat 1786139669 = 3600). Nous ajoutions `jku` et signions pour 8 h.
	header := map[string]any{"alg": "ES256", "kid": kid}
	payload := map[string]any{
		"exp": now.Add(gssTokenTTL).Unix(),
		"iat": now.Unix(),
		"iss": "gss",
		"sub": uid,
		"gamesync": map[string]any{
			"attr": attrJSON,
			"gsid": gsid,
			"ltcy": ltcyJSON,
			"team": team,
			"tid":  tid,
			"typ":  1,
			"uid":  uid,
			"usid": usid,
		},
	}

	hj, _ := json.Marshal(header)
	pj, _ := json.Marshal(payload)
	signing := b64u(hj) + "." + b64u(pj)

	sum := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, key, sum[:])
	if err != nil {
		log.Printf("[NPLN MM] gss match token sign: %v", err)
		return "nextendo-gss.fallback"
	}
	sig := make([]byte, 64)
	copyRightAligned(sig[:32], r)
	copyRightAligned(sig[32:], s)
	return signing + "." + b64u(sig)
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

// hex16 renders the PID as a 16-hex-digit id, the shape Nintendo's ext_id has
// ("c481d0cfa1569241").
func hex16(v uint64) string { return fmt.Sprintf("%016x", v) }

func copyRightAligned(dst []byte, v *big.Int) {
	b := v.Bytes()
	if len(b) > len(dst) {
		b = b[len(b)-len(dst):]
	}
	copy(dst[len(dst)-len(b):], b)
}
