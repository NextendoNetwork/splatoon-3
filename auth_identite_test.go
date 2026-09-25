package main

// Verrou sur le REFUS des identites non resolues.
//
// L'ancien repli rendait pid=0 et l'identite du compte CAPTURE — un compte qui porte une vraie
// progression. Le 2026-08-21 un joueur s'est connecte et a trouve la sauvegarde de quelqu'un
// d'autre ; deux joueurs tombes dedans en meme temps s'ecrasent mutuellement. Mesure du 22/08 : le
// repli se declenchait environ deux fois par heure.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	authpb "npln.nintendo.net/npln-practice/proto/auth/v1"
)

func TestIdentiteNonResolueEstRefusee(t *testing.T) {
	t.Setenv("NPLN_ALLOW_UNVERIFIED", "")
	pid, chemin, err := gatedIdentity(nil, "tenants/t-dce9377b-lp1")

	if err == nil {
		t.Fatal("une identite non resolue doit etre REFUSEE, pas servie sous une autre identite")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("code %s, attendu PermissionDenied", status.Code(err))
	}
	if pid != 0 || chemin != "" {
		t.Errorf("refus mais pid=%d chemin=%q : rien ne doit etre rendu", pid, chemin)
	}
	// Et surtout : le compte capture ne doit apparaitre nulle part.
	if strings.Contains(chemin, capturedUserGameRecord) || strings.Contains(chemin, capturedUserFriends) {
		t.Error("le compte capture est encore servi : c'est exactement le defaut qu'on ferme")
	}
	if !strings.Contains(err.Error(), "Nextendo") {
		t.Errorf("message %q : il doit dire au joueur quoi faire", err.Error())
	}
}

func TestUnverifiedExternalIdentityStaysDeniedWithDevFlags(t *testing.T) {
	t.Setenv("NPLN_ALLOW_UNVERIFIED", "1")
	t.Setenv("NPLN_JWT_SUB_CAPTURED", "1")
	ext := &authpb.ExternalIdToken{Token: &authpb.ExternalIdToken_DummyExtIdToken{DummyExtIdToken: "forged-user-id"}}
	pid, path, err := gatedIdentity(ext, "tenants/t-dce9377b-lp1")
	if err == nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unverified identity was accepted with development flags: pid=%d path=%q err=%v", pid, path, err)
	}
	if pid != 0 || path != "" {
		t.Fatalf("rejected identity returned usable identity: pid=%d path=%q", pid, path)
	}
}

func TestClientSharedSubsdkHMACDoesNotAuthorizePID(t *testing.T) {
	t.Setenv("NPLN_ALLOW_UNVERIFIED", "1")
	oldSecret := subsdkAuthSecret
	subsdkAuthSecret = "client-extractable-test-key"
	t.Cleanup(func() { subsdkAuthSecret = oldSecret })

	payload, err := json.Marshal(subsdkClaims{AccountBytes: "0100000000000000"})
	if err != nil {
		t.Fatal(err)
	}
	b64 := base64.StdEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(subsdkAuthSecret))
	_, _ = mac.Write([]byte(b64))
	token := b64 + "," + hex.EncodeToString(mac.Sum(nil))
	ext := &authpb.ExternalIdToken{Token: &authpb.ExternalIdToken_NsaIdToken{NsaIdToken: token}}

	pid, path, err := gatedIdentity(ext, nplnTenant)
	if err == nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("client-shared HMAC token authorized PID %d: path=%q err=%v", pid, path, err)
	}
	if pid != 0 || path != "" {
		t.Fatalf("rejected client HMAC returned identity pid=%d path=%q", pid, path)
	}
}
