package main

// account_client — the npln-s3 (Splatoon 3 / NPLN) bridge to nextendo-account.
// It reads the SAME unified Nextendo identity + friend graph the NEX games use, so
// S3 shows the same friends as S2, and so online is gated on a verified account.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// accountBaseURL is nextendo-account's internal base URL (server-to-server). Default
// is the coolify docker-network address on the VPS; local dev overrides via the env.
var accountBaseURL = envOr("NEXTENDO_ACCOUNT_URL", "http://nextendo-account:8080")

var accountHTTP = &http.Client{Timeout: 5 * time.Second}

// nplnFriendData mirrors one entry of /internal/npln-friends.
type nplnFriendData struct {
	PID        uint64         `json:"pid"`
	UserID     string         `json:"user_id"`
	AccountHex string         `json:"account_hex"`
	Name       string         `json:"name"`
	Presence   map[string]any `json:"presence"`
}

// nplnAccountData mirrors the /internal/npln-friends response: the account's NPLN
// identity, the verified-account gate signal, and its friends.
type nplnAccountData struct {
	PID        uint64           `json:"pid"`
	UserID     string           `json:"user_id"`
	AccountHex string           `json:"account_hex"`
	Verified   bool             `json:"verified"`
	Friends    []nplnFriendData `json:"friends"`
}

// accountFriends fetches an account's NPLN identity + friend graph by PID.
func accountFriends(pid uint64) (*nplnAccountData, error) {
	resp, err := accountHTTP.Get(fmt.Sprintf("%s/internal/npln-friends?pid=%d", accountBaseURL, pid))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("npln-friends pid=%d: %s", pid, resp.Status)
	}
	var out nplnAccountData
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// resolveNSAToPID maps an NSA id (as the console presents it) to the owning Nextendo
// account PID via /api/nsa — the same reverse-lookup the NEX auth uses.
// [Nextendo] /api/nsa expects the id as a DECIMAL uint64 (it does ParseUint(baas,16,64) on the
// stored side and compares numerically). NPLN hands us the NSA as raw HEX, and a 32-hex-char one at
// that -- 128 bits, twice the width the endpoint was written for (it predates NPLN: it was built for
// a CFW Switch's 64-bit baasUserID over NEX). Sending the hex string verbatim produced a flat
// 400 Bad Request, which our caller then reported as "NSA has no Nextendo account" -- a misleading
// message that hid a format mismatch behind a "not found". Convert here, and for an over-wide id
// match on the LOW 64 bits, which is the part a baasUserID corresponds to.
func nsaToDecimal(nsa string) (string, bool) {
	h := strings.TrimPrefix(strings.TrimPrefix(strings.ToLower(strings.TrimSpace(nsa)), "0x"), "nsa:")

	// Already decimal? pass it through untouched.
	if v, err := strconv.ParseUint(h, 10, 64); err == nil {
		return strconv.FormatUint(v, 10), true
	}
	if h == "" {
		return "", false
	}
	if len(h) > 16 {
		h = h[len(h)-16:] // low 64 bits
	}

	v, err := strconv.ParseUint(h, 16, 64)
	if err != nil {
		return "", false
	}
	return strconv.FormatUint(v, 10), true
}

func resolveNSAToPID(nsa string) (uint64, error) {
	q, ok := nsaToDecimal(nsa)
	if !ok {
		return 0, fmt.Errorf("nsa %q: format non convertible en uint64 decimal", nsa)
	}

	resp, err := accountHTTP.Get(accountBaseURL + "/api/nsa?id=" + url.QueryEscape(q))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("nsa %s: %s", nsa, resp.Status)
	}
	var out struct {
		PID uint64 `json:"pid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.PID, nil
}
