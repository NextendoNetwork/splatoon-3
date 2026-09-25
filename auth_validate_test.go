package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestValidateTokenRequiresSignedExistingMatchingAccount(t *testing.T) {
	clearAccountIdentityCache()
	t.Cleanup(clearAccountIdentityCache)
	t.Setenv("NPLN_BAN_FILE", filepath.Join(t.TempDir(), "bans.json"))
	t.Setenv("NPLN_ALLOW_UNVERIFIED", "")
	const pid = uint64(1800004321)
	const uid = "u-abcdefghijklmnopqrst"
	access := mintNplnAccessToken(pid, nplnTenant+"/users/"+uid, nplnTenant)

	oldBaseURL, oldHTTP := accountBaseURL, accountHTTP
	t.Cleanup(func() { accountBaseURL, accountHTTP = oldBaseURL, oldHTTP })
	accountServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pid") == "1800004321" {
			_, _ = fmt.Fprintf(w, `{"pid":%d,"user_id":%q,"verified":true,"friends":[]}`, pid, uid)
			return
		}
		if r.URL.Query().Get("pid") == "1800004322" {
			_, _ = fmt.Fprint(w, `{"pid":1800004322,"user_id":"u-disabledabcdefghijklm","verified":false,"disabled":true,"friends":[]}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer accountServer.Close()
	accountBaseURL, accountHTTP = accountServer.URL, accountServer.Client()

	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+access, "uid", "u-spoofedmetadataabcdef"))
	if got := uidFromCtx(ctx); got != uid {
		t.Fatalf("uid metadata overrode the signed subject: got %q, want %q", got, uid)
	}
	if _, err := (&authServer{}).ValidateToken(ctx, &emptypb.Empty{}); err != nil {
		t.Fatalf("valid existing account rejected: %v", err)
	}

	unknown := mintNplnAccessToken(pid+1, nplnTenant+"/users/u-unknownabcdefghijklm", nplnTenant)
	unknownCtx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+unknown))
	if _, err := (&authServer{}).ValidateToken(unknownCtx, &emptypb.Empty{}); err == nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unknown PID was not denied: %v", err)
	}
	if err := requireSignedCaller("/nn.npln.toyohr.v1.CloudSave/GetSaveRecord", unknownCtx); err == nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ordinary RPC accepted a signed token without an account: %v", err)
	}

	spoofed := mintNplnAccessToken(pid, nplnTenant+"/users/u-someoneelseabcdefgh", nplnTenant)
	spoofedCtx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+spoofed))
	if _, err := (&authServer{}).ValidateToken(spoofedCtx, &emptypb.Empty{}); err == nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("signed PID/UID mismatch was not denied: %v", err)
	}

	legacyCtx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer nextendo-npln-access.1800004321"))
	t.Setenv("NPLN_ALLOW_UNVERIFIED", "1")
	if _, err := (&authServer{}).ValidateToken(legacyCtx, &emptypb.Empty{}); err == nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unsigned legacy token was accepted: %v", err)
	}

	disabled := mintNplnAccessToken(1800004322, nplnTenant+"/users/u-disabledabcdefghijklm", nplnTenant)
	disabledCtx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+disabled))
	if _, err := (&authServer{}).ValidateToken(disabledCtx, &emptypb.Empty{}); err == nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("disabled account was accepted with NPLN_ALLOW_UNVERIFIED=1: %v", err)
	}
}
