package main

// subsdk_token — decode the identity token the Nextendo S3 subsdk presents to Auth.
//
// The subsdk does NOT send a standard JWT. Its NSA id token is:
//
//	base64(json) + "," + hmac_sha256_hex(base64_part)
//
// where json is the identity payload (BuildAuthJson in the subsdk) and the HMAC is
// keyed with the subsdk's shared secret (kAuthHmacSecret). Since the key is shipped to the
// client, this parser can diagnose token format but MUST NOT authorize an account from
// "account_bytes". Online identity is proven by the server-signed Nextendo nnex token.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
)

// subsdkAuthSecret est la cle HMAC dont le client signe son jeton d'identite.
//
// ⚠️ AUCUNE VALEUR PAR DEFAUT, ET C'EST VOULU. Cette cle EST l'identite : qui la detient peut
// forger un jeton valide pour n'importe quel joueur. Elle n'a donc rien a faire dans un depot,
// et un deploiement qui la laisse vide REFUSE les jetons plutot que de les accepter sans preuve
// (voir verifierSignatureSubsdk). Reglez NPLN_AUTH_HMAC_SECRET, cote serveur ET cote client, sur
// la meme valeur — et changez-la si vous la soupconnez exposee.
var subsdkAuthSecret = os.Getenv("NPLN_AUTH_HMAC_SECRET")

// subsdkClaims mirrors the JSON payload the subsdk base64-encodes in its token.
type subsdkClaims struct {
	Timestamp       int64  `json:"timestamp"`
	TenantID        string `json:"tenant_id"`
	Emulator        bool   `json:"emulator"`
	DisplayVersion  string `json:"display_version"`
	NextendoVersion string `json:"nextendo_version"`
	PseudoID        string `json:"pseudo_id"`
	AccountBytes    string `json:"account_bytes"`
	FestSHA         string `json:"fest_config_sha256"`
	UserSHA         string `json:"user_config_sha256"`
}

// parseSubsdkToken decodes the client-shared format for diagnostics. A valid HMAC here does not
// establish account ownership and must never be used as an authorization decision.
func parseSubsdkToken(tok string) (*subsdkClaims, bool) {
	i := strings.IndexByte(tok, ',')
	if i <= 0 || i+1 >= len(tok) {
		return nil, false
	}
	b64, mac := tok[:i], tok[i+1:]
	if subsdkAuthSecret == "" {
		return nil, false
	}
	h := hmac.New(sha256.New, []byte(subsdkAuthSecret))
	h.Write([]byte(b64))
	want := hex.EncodeToString(h.Sum(nil))
	if !hmac.Equal([]byte(strings.ToLower(mac)), []byte(want)) {
		return nil, false
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, false
	}
	var c subsdkClaims
	if json.Unmarshal(raw, &c) != nil {
		return nil, false
	}
	return &c, true
}

// pidFromAccountBytes extracts the Nextendo account PID from the hex-encoded
// account.dat bytes. Nextendo provisions account.dat so its first 8 bytes are the
// account PID (little-endian uint64); the rest is padding. Returns 0 if the bytes
// don't look like a provisioned Nextendo identity (e.g. a legacy/random account.dat).
func pidFromAccountBytes(h string) uint64 {
	b, err := hex.DecodeString(strings.TrimSpace(h))
	if err != nil || len(b) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b[:8])
}
