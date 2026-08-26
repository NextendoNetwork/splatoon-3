package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Cœur du modèle de confiance NPLN : l'identité d'un appelant vient de son jeton d'accès. Si le
// serveur ne VÉRIFIE pas la signature, n'importe qui forge un jeton portant l'ext_id d'un autre
// et agit sous son compte (friends/présence). Ce test fige : un jeton qu'on a signé est accepté,
// un jeton forgé ou trafiqué est refusé.
func TestAccessTokenMustBeSigned(t *testing.T) {
	// Clé de test dans un dossier temporaire — ne pas semer de clé dans l'arbre source.
	os.Setenv("NPLN_JWT_KEY", filepath.Join(t.TempDir(), "k.pem"))

	const pid = uint64(1800001234)
	tok := mintNplnAccessToken(pid, nplnTenant+"/users/u-legit", nplnTenant)

	// 1) Jeton légitime (qu'on vient de signer) -> accepté, bon PID.
	if got, ok := pidFromJWT(tok); !ok || got != pid {
		t.Fatalf("jeton légitime rejeté ou mauvais PID : ok=%v got=%d (attendu %d)", ok, got, pid)
	}

	// 2) Signature remplacée par des zéros -> refus.
	if i := lastDot(tok); i > 0 {
		tampered := tok[:i+1] + b64u(make([]byte, 64))
		if _, ok := pidFromJWT(tampered); ok {
			t.Fatal("jeton à signature nulle accepté (usurpation possible)")
		}
	}

	// 3) Jeton entièrement forgé : header+payload arbitraires avec l'ext_id d'une victime, aucune
	// vraie signature. C'est l'attaque exacte -> doit être refusé.
	forged := b64u([]byte(`{"alg":"ES256"}`)) + "." +
		b64u([]byte(`{"npln":{"ext_id":"0000000000000001"}}`)) + "." +
		b64u(make([]byte, 64))
	if _, ok := pidFromJWT(forged); ok {
		t.Fatal("jeton forgé (identité usurpée) accepté")
	}
}

func lastDot(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}
