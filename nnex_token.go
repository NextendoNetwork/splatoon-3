package main

// nnex_token — résout l'identité NPLN depuis la PREUVE cryptographique que
// l'émulateur Nextendo glisse déjà dans le id_token BAAS.
//
// POURQUOI : le "NSA" que S3 présente à Auth.IssueToken est le claim `sub` du
// id_token BAAS fabriqué par l'émulateur. Or ce sub est 16 octets ALÉATOIRES
// régénérés à chaque émission du jeton :
//
//	Ryujinx-Nextendo : src/Ryujinx.HLE/HOS/Services/Account/Acc/AccountService/ManagerServer.cs
//	  byte[] rawUserId = new byte[0x10]; RandomNumberGenerator.Fill(rawUserId);
//	  Subject = new ClaimsIdentity([new Claim(Sub, Convert.ToHexString(rawUserId).ToLower())]),
//
// Il ne peut donc JAMAIS correspondre au baas_id du compte (16 hex = 64 bits,
// HMAC(secret,"baas:"+pid)) : aucune conversion hex->décimal, aucune troncature
// basse ou haute ne peut relier un aléa 128 bits à une dérivation 64 bits.
// /api/nsa n'est pas en cause : interrogé avec la bonne valeur il répond.
//
// La vraie liaison console/émulateur -> compte existe déjà et est PROUVABLE :
// le claim "nnex" du MÊME id_token porte le jeton « nx2. » signé HMAC-SHA256 par
// nextendo-account (payload "pid.username.expiry"). C'est exactement ce que les
// serveurs NEX valident (server/NEXtendo/authbinding.go + nextendoPIDFromToken).
// On fait pareil ici. Comme le nnex est prouvé, ce chemin est sûr EN PRODUCTION,
// contrairement au sub NSA non signable.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	authpb "npln.nintendo.net/npln-practice/proto/auth/v1"
)

// nextendoSecret : chargé EXACTEMENT comme nextendo-account (loadSecret) et comme
// les serveurs NEX — la variable NEXTENDO_SECRET telle quelle, sinon le fichier de
// clé partagé décodé depuis l'hexadécimal. Un chargement différent = empreinte
// différente = tous les jetons refusés.
var nextendoSecret = loadNextendoSecret()

func loadNextendoSecret() []byte {
	if v := os.Getenv("NEXTENDO_SECRET"); v != "" {
		return []byte(v)
	}
	path := envOr("NEXTENDO_SECRET_FILE", "/data/nextendo_secret.key")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	dec, derr := hex.DecodeString(strings.TrimSpace(string(b)))
	if derr != nil || len(dec) < 16 {
		return nil
	}
	return dec
}

// revokedNexPayloads : jetons fuités à refuser malgré une signature valide.
//
// Un jeton signé reste valable jusqu'à son expiration : si l'un d'eux fuit, le refuser
// nommément est le seul recours immédiat. La liste est vide ici — elle décrit des incidents
// propres à un déploiement, et les identités qu'elle contient n'ont rien à faire dans un dépôt
// public. Renseignez la vôtre par « NPLN_REVOKED_TOKENS=<charge>[,<charge>…] », où chaque charge
// est le « <pid>.<pseudo>.<expiration> » à refuser, et gardez-la synchronisée avec
// nextendo-account et chacun de vos serveurs de jeu.
var revokedNexPayloads = chargerJetonsRevoques()

func chargerJetonsRevoques() map[string]bool {
	out := map[string]bool{}
	for _, e := range strings.Split(os.Getenv("NPLN_REVOKED_TOKENS"), ",") {
		if e = strings.TrimSpace(e); e != "" {
			out[e] = true
		}
	}
	if len(out) > 0 {
		log.Printf("[NPLN Auth] %d jeton(s) revoque(s) charge(s) depuis NPLN_REVOKED_TOKENS", len(out))
	}

	return out
}

// nextendoPIDFromNexToken valide « nx2.<b64url(pid.username.expiry)>.<b64url(hmac)> »
// et renvoie le PID prouvé.
func nextendoPIDFromNexToken(s string) (uint64, bool) {
	if len(nextendoSecret) == 0 || !strings.HasPrefix(s, "nx2.") {
		return 0, false
	}
	parts := strings.Split(s[len("nx2."):], ".")
	if len(parts) != 2 {
		return 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return 0, false
	}
	mac := hmac.New(sha256.New, nextendoSecret)
	mac.Write([]byte("nex:" + string(raw)))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(parts[1])) {
		return 0, false
	}
	if revokedNexPayloads[string(raw)] {
		return 0, false
	}
	f := strings.SplitN(string(raw), ".", 3) // pid.username.expiry
	if len(f) != 3 {
		return 0, false
	}
	pid, err := strconv.ParseUint(f[0], 10, 64)
	if err != nil || pid == 0 {
		return 0, false
	}
	if exp, eerr := strconv.ParseInt(f[2], 10, 64); eerr != nil || time.Now().Unix() > exp {
		return 0, false
	}
	return pid, true
}

// nnexFromIDToken lit le claim "nnex" de la charge utile d'un JWT compact, sans
// vérifier la signature RS256 du id_token (on ne détient pas la clé publique côté
// NPLN) — la preuve d'identité, c'est le HMAC du nx2, pas le JWT qui le transporte.
func nnexFromIDToken(jwt string) (string, bool) {
	segments := strings.Split(jwt, ".")
	if len(segments) < 2 {
		return "", false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(segments[1], "="))
	if err != nil {
		return "", false
	}
	var claims struct {
		Nnex string `json:"nnex"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Nnex == "" {
		return "", false
	}
	return claims.Nnex, true
}

// pidFromNnex : PID PROUVÉ porté par le id_token externe, ou 0.
func pidFromNnex(ext *authpb.ExternalIdToken) (uint64, bool) {
	if ext == nil {
		return 0, false
	}
	tok := ext.GetNsaIdToken()
	if tok == "" {
		return 0, false
	}
	nnex, ok := nnexFromIDToken(tok)
	if !ok {
		return 0, false
	}
	return nextendoPIDFromNexToken(nnex)
}
