package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPlayerBanPersistsAndCancelsActiveStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bans.json")
	registry := newBanRegistry(func() string { return path })
	cancelled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	stop, err := registry.trackStream(1800000042, []string{"u-test-player"}, func() {
		cancel()
		close(cancelled)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	// The operator CLI runs in a separate process. Persist there, then simulate the
	// running server's hot-reload tick against its own registry.
	operator := newBanRegistry(func() string { return path })
	if err := operator.ban(playerBan{PID: 1800000042, UID: "tenants/current/users/u-test-player", Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.refresh(true); err != nil {
		t.Fatalf("running server failed to reload ban: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("ban did not cancel the active stream")
	}
	if ctx.Err() == nil {
		t.Fatal("stream context is still live after ban")
	}

	reloaded := newBanRegistry(func() string { return path })
	if _, banned, err := reloaded.find(1800000042, ""); err != nil || !banned {
		t.Fatalf("persisted ban missing after reload: banned=%v err=%v", banned, err)
	}
	if _, banned, err := reloaded.find(0, "u-test-player"); err != nil || !banned {
		t.Fatalf("UID ban lookup failed: banned=%v err=%v", banned, err)
	}

	if removed, err := reloaded.unban(1800000042); err != nil || !removed {
		t.Fatalf("unban failed: removed=%v err=%v", removed, err)
	}
	// A second process observes file edits through the same reload path used by the
	// running server's monitor. Force that reload here so the assertion is deterministic.
	if err := registry.refresh(true); err != nil {
		t.Fatalf("reload after unban failed: %v", err)
	}
	if _, banned, err := registry.find(1800000042, "u-test-player"); err != nil || banned {
		t.Fatalf("unban was not observed: banned=%v err=%v", banned, err)
	}
}

func TestPlayerBanCanTargetSaveUIDWithoutPID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bans.json")
	registry := newBanRegistry(func() string { return path })
	if err := registry.ban(playerBan{UID: "1x7aknus9twl5bok77cu", Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	if _, banned, err := registry.find(0, "tenants/current/users/1x7aknus9twl5bok77cu"); err != nil || !banned {
		t.Fatalf("UID-only ban did not match canonical resource path: banned=%v err=%v", banned, err)
	}
	if _, banned, err := registry.find(1800000999, "1x7aknus9twl5bok77cu"); err != nil || !banned {
		t.Fatalf("UID-only ban did not match when PID is also known: banned=%v err=%v", banned, err)
	}
	if removed, err := registry.unbanTarget(0, "1x7aknus9twl5bok77cu"); err != nil || !removed {
		t.Fatalf("UID unban failed: removed=%v err=%v", removed, err)
	}
}

func TestPlayerBanRegistryFailsClosedOnCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bans.json")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	registry := newBanRegistry(func() string { return path })
	if err := registry.requireAllowed(1, ""); err == nil {
		t.Fatal("corrupt ban registry should not allow the request through")
	}
}
