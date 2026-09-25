package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// playerBan can be keyed by a stable Nextendo PID, a CloudSave user UID, or both.
type playerBan struct {
	PID      uint64    `json:"pid"`
	UID      string    `json:"uid,omitempty"`
	Reason   string    `json:"reason,omitempty"`
	BannedAt time.Time `json:"bannedAt"`
}

type playerBanFile struct {
	Version int         `json:"version"`
	Bans    []playerBan `json:"bans"`
}

type trackedBanStream struct {
	pid    uint64
	uids   []string
	cancel context.CancelFunc
}

type banRegistry struct {
	path func() string

	mu          sync.Mutex
	bans        map[string]playerBan
	streams     map[uint64]trackedBanStream
	nextStream  uint64
	loaded      bool
	fileExists  bool
	fileModTime time.Time
	fileSize    int64
	lastCheck   time.Time
}

func newBanRegistry(path func() string) *banRegistry {
	return &banRegistry{
		path:    path,
		bans:    map[string]playerBan{},
		streams: map[uint64]trackedBanStream{},
	}
}

var playerBans = newBanRegistry(func() string {
	if path := strings.TrimSpace(os.Getenv("NPLN_BAN_FILE")); path != "" {
		return path
	}
	return filepath.Join(saveDir(), "bans.json")
})

var banMonitorOnce sync.Once

func startBanMonitor() {
	banMonitorOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for range ticker.C {
				if err := playerBans.refresh(true); err != nil {
					log.Printf("[NPLN bans] reload %s: %v", playerBans.path(), err)
				}
			}
		}()
	})
}

func cleanBanUID(uid string) string {
	uid = strings.TrimSpace(uid)
	if i := strings.LastIndexByte(uid, '/'); i >= 0 {
		uid = uid[i+1:]
	}
	uid = strings.TrimSuffix(uid, ".record.pb")
	uid = strings.TrimSuffix(uid, ".save")
	if uid != "" && !strings.HasPrefix(uid, "u-") {
		uid = "u-" + uid
	}
	return uid
}

func validBanUID(uid string) bool {
	uid = cleanBanUID(uid)
	if len(uid) == 0 || len(uid) > 128 {
		return false
	}
	for _, r := range uid {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func banEntryKey(b playerBan) string {
	if b.PID != 0 {
		return "pid:" + strconv.FormatUint(b.PID, 10)
	}
	return "uid:" + cleanBanUID(b.UID)
}

func (b playerBan) matches(pid uint64, uids ...string) bool {
	if pid != 0 && b.PID == pid {
		return true
	}
	banUID := cleanBanUID(b.UID)
	if banUID == "" {
		return false
	}
	for _, uid := range uids {
		if cleanBanUID(uid) == banUID {
			return true
		}
	}
	return false
}

func (r *banRegistry) matchingLocked(pid uint64, uids ...string) (playerBan, bool) {
	for _, b := range r.bans {
		if b.matches(pid, uids...) {
			return b, true
		}
	}
	return playerBan{}, false
}

// refresh notices atomic replacements made by a separate server process or the CLI. The normal
// request path stats the file at most four times per second; the monitor forces a reload twice a
// second so streams opened in another process are cancelled promptly too.
func (r *banRegistry) refresh(force bool) error {
	now := time.Now()
	r.mu.Lock()
	if !force && now.Sub(r.lastCheck) < 250*time.Millisecond {
		r.mu.Unlock()
		return nil
	}
	r.lastCheck = now
	path := r.path()
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		if r.fileExists || !r.loaded {
			r.bans = map[string]playerBan{}
			r.loaded = true
			r.fileExists = false
			r.fileModTime = time.Time{}
			r.fileSize = 0
		}
		r.mu.Unlock()
		return nil
	}
	if err != nil {
		r.mu.Unlock()
		return fmt.Errorf("stat ban file: %w", err)
	}
	if r.loaded && r.fileExists && info.ModTime().Equal(r.fileModTime) && info.Size() == r.fileSize {
		r.mu.Unlock()
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		r.mu.Unlock()
		return fmt.Errorf("read ban file: %w", err)
	}
	var disk playerBanFile
	if err := json.Unmarshal(data, &disk); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("parse ban file: %w", err)
	}
	next := make(map[string]playerBan, len(disk.Bans))
	for _, b := range disk.Bans {
		b.UID = cleanBanUID(b.UID)
		if b.PID == 0 && !validBanUID(b.UID) || b.UID != "" && !validBanUID(b.UID) {
			r.mu.Unlock()
			return fmt.Errorf("ban file contains an entry without a valid PID or UID")
		}
		next[banEntryKey(b)] = b
	}
	r.bans = next
	r.loaded = true
	r.fileExists = true
	r.fileModTime = info.ModTime()
	r.fileSize = info.Size()
	cancels := r.cancellationsForCurrentBansLocked()
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return nil
}

func (r *banRegistry) cancellationsForCurrentBansLocked() []context.CancelFunc {
	var cancels []context.CancelFunc
	for id, stream := range r.streams {
		if _, banned := r.matchingLocked(stream.pid, stream.uids...); banned {
			cancels = append(cancels, stream.cancel)
			delete(r.streams, id)
		}
	}
	return cancels
}

func (r *banRegistry) find(pid uint64, uids ...string) (playerBan, bool, error) {
	if err := r.refresh(false); err != nil {
		return playerBan{}, false, err
	}
	r.mu.Lock()
	b, ok := r.matchingLocked(pid, uids...)
	r.mu.Unlock()
	return b, ok, nil
}

func (r *banRegistry) requireAllowed(pid uint64, uids ...string) error {
	b, banned, err := r.find(pid, uids...)
	if err != nil {
		return status.Error(codes.Unavailable, "ban registry unavailable")
	}
	if banned {
		log.Printf("[NPLN bans] refuse pid=%d uids=%q reason=%q", pid, cleanedUIDs(uids), b.Reason)
		return status.Error(codes.PermissionDenied, "Nextendo account is banned from this server")
	}
	return nil
}

func cleanedUIDs(uids []string) []string {
	out := make([]string, 0, len(uids))
	seen := map[string]bool{}
	for _, uid := range uids {
		uid = cleanBanUID(uid)
		if uid != "" && !seen[uid] {
			seen[uid] = true
			out = append(out, uid)
		}
	}
	return out
}

func banIdentity(ctx context.Context) (uint64, []string) {
	// Only the server-signed bearer token is an authorization identity. A UID metadata header is
	// client-controlled and must not be able to claim another player's ban state or save identity.
	pid, uid, ok := callerIdentityFromJWT(ctx)
	if !ok {
		return 0, nil
	}
	return pid, cleanedUIDs([]string{uid})
}

func checkBanContext(ctx context.Context) error {
	pid, uids := banIdentity(ctx)
	return playerBans.requireAllowed(pid, uids...)
}

func authBootstrapMethod(method string) bool {
	switch method {
	case "/nn.npln.auth.v1.Auth/CreateUser",
		"/nn.npln.auth.v1.Auth/IssueToken",
		"/nn.npln.auth.v1.Auth/IssuePrearrangedUserToken",
		"/nn.npln.auth.v1.Auth/IssueAnonymousUserToken",
		"/nn.npln.auth.v1.Auth/RefreshToken",
		"/nn.npln.auth.v1.Auth/RefreshAnonymousUserToken":
		return true
	default:
		return false
	}
}

func requireSignedCaller(method string, ctx context.Context) error {
	if authBootstrapMethod(method) {
		return nil
	}
	pid, uid, ok := callerIdentityFromJWT(ctx)
	if !ok || pid == 0 || !validBanUID(uid) {
		return status.Error(codes.Unauthenticated, "valid signed Nextendo identity required")
	}
	if err := playerBans.requireAllowed(pid, uid); err != nil {
		return err
	}
	_, err := validateAccountIdentity(pid, uid)
	return err
}

func banUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := requireSignedCaller(info.FullMethod, ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func chaineFlux(first, second grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return first(srv, stream, info, func(inner any, innerStream grpc.ServerStream) error {
			return second(inner, innerStream, info, handler)
		})
	}
}

type banContextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *banContextServerStream) Context() context.Context { return s.ctx }

func banStreamInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := requireSignedCaller(info.FullMethod, stream.Context()); err != nil {
		return err
	}
	pid, uids := banIdentity(stream.Context())
	ctx, cancel := context.WithCancel(stream.Context())
	remove, err := playerBans.trackStream(pid, uids, cancel)
	if err != nil {
		cancel()
		if status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.Unavailable {
			return err
		}
		return status.Error(codes.Unavailable, "ban registry unavailable")
	}
	defer func() {
		remove()
		cancel()
	}()
	return handler(srv, &banContextServerStream{ServerStream: stream, ctx: ctx})
}

func (r *banRegistry) trackStream(pid uint64, uids []string, cancel context.CancelFunc) (func(), error) {
	if err := r.refresh(false); err != nil {
		return nil, status.Error(codes.Unavailable, "ban registry unavailable")
	}
	r.mu.Lock()
	uids = cleanedUIDs(uids)
	if _, banned := r.matchingLocked(pid, uids...); banned {
		r.mu.Unlock()
		return nil, status.Error(codes.PermissionDenied, "Nextendo account is banned from this server")
	}
	if pid == 0 && len(uids) == 0 {
		r.mu.Unlock()
		return func() {}, nil
	}
	r.nextStream++
	id := r.nextStream
	r.streams[id] = trackedBanStream{pid: pid, uids: uids, cancel: cancel}
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.streams, id)
		r.mu.Unlock()
	}, nil
}

func (r *banRegistry) ban(entry playerBan) error {
	entry.UID = cleanBanUID(entry.UID)
	if entry.PID == 0 && !validBanUID(entry.UID) || entry.UID != "" && !validBanUID(entry.UID) {
		return fmt.Errorf("ban requires a positive PID or a valid user UID")
	}
	if err := r.refresh(true); err != nil {
		return err
	}
	if entry.BannedAt.IsZero() {
		entry.BannedAt = time.Now().UTC()
	}
	r.mu.Lock()
	r.bans[banEntryKey(entry)] = entry
	if err := r.persistLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	cancels := r.cancellationsForCurrentBansLocked()
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return nil
}

func (r *banRegistry) unban(pid uint64) (bool, error) { return r.unbanTarget(pid, "") }

func (r *banRegistry) unbanTarget(pid uint64, uid string) (bool, error) {
	uid = cleanBanUID(uid)
	if pid == 0 && !validBanUID(uid) {
		return false, fmt.Errorf("unban requires a positive PID or a valid user UID")
	}
	if err := r.refresh(true); err != nil {
		return false, err
	}
	r.mu.Lock()
	existed := false
	for key, entry := range r.bans {
		matches := pid != 0 && entry.PID == pid
		matches = matches || uid != "" && cleanBanUID(entry.UID) == uid
		if matches {
			delete(r.bans, key)
			existed = true
		}
	}
	if existed {
		if err := r.persistLocked(); err != nil {
			r.mu.Unlock()
			return false, err
		}
	}
	r.mu.Unlock()
	return existed, nil
}

func (r *banRegistry) list() ([]playerBan, error) {
	if err := r.refresh(false); err != nil {
		return nil, err
	}
	r.mu.Lock()
	out := make([]playerBan, 0, len(r.bans))
	for _, b := range r.bans {
		out = append(out, b)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].PID != out[j].PID {
			return out[i].PID < out[j].PID
		}
		return out[i].UID < out[j].UID
	})
	return out, nil
}

func (r *banRegistry) persistLocked() error {
	path := r.path()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create ban directory: %w", err)
	}
	entries := make([]playerBan, 0, len(r.bans))
	for _, b := range r.bans {
		entries = append(entries, b)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].PID != entries[j].PID {
			return entries[i].PID < entries[j].PID
		}
		return entries[i].UID < entries[j].UID
	})
	tmp, err := os.CreateTemp(dir, ".bans-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary ban file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("protect temporary ban file: %w", err)
	}
	if err := json.NewEncoder(tmp).Encode(playerBanFile{Version: 1, Bans: entries}); err != nil {
		tmp.Close()
		return fmt.Errorf("encode ban file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync ban file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close ban file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace ban file: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat saved ban file: %w", err)
	}
	r.loaded = true
	r.fileExists = true
	r.fileModTime = info.ModTime()
	r.fileSize = info.Size()
	r.lastCheck = time.Now()
	return nil
}

func parseBanTarget(raw string) (uint64, string, error) {
	if pid, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64); err == nil {
		if pid == 0 {
			return 0, "", fmt.Errorf("PID must be positive")
		}
		return pid, "", nil
	}
	if strings.Trim(raw, "0123456789") == "" {
		return 0, "", fmt.Errorf("numeric PID is out of range")
	}
	uid := cleanBanUID(raw)
	if !validBanUID(uid) {
		return 0, "", fmt.Errorf("target must be a positive PID or a valid save UID")
	}
	return 0, uid, nil
}

// runBanCLI handles operator-only subcommands before the gRPC server starts. The running server
// hot-reloads the same file and cancels matching streams, so this command does not restart it.
func runBanCLI(args []string) int {
	if len(args) == 0 {
		return 0
	}
	usage := func() {
		fmt.Fprintln(os.Stderr, "Usage: npln-server ban <PID|UID> [reason...] | unban <PID|UID> | bans | findsave <UserName|Identifier>")
		fmt.Fprintf(os.Stderr, "Ban file: %s\n", playerBans.path())
	}
	switch args[0] {
	case "findsave":
		if len(args) != 2 {
			usage()
			return 2
		}
		return printSaveSearch(args[1])
	case "bans":
		if len(args) != 1 {
			usage()
			return 2
		}
		entries, err := playerBans.list()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if len(entries) == 0 {
			fmt.Println("No banned accounts.")
			return 0
		}
		for _, b := range entries {
			if b.PID == 0 {
				fmt.Printf("UID %s  since %s  reason: %s\n", b.UID, b.BannedAt.Format(time.RFC3339), b.Reason)
			} else {
				fmt.Printf("PID %d  UID %s  since %s  reason: %s\n", b.PID, b.UID,
					b.BannedAt.Format(time.RFC3339), b.Reason)
			}
		}
		return 0
	case "ban":
		if len(args) < 2 {
			usage()
			return 2
		}
		pid, uid, err := parseBanTarget(args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		entry := playerBan{PID: pid, UID: uid, Reason: strings.TrimSpace(strings.Join(args[2:], " ")), BannedAt: time.Now().UTC()}
		if pid != 0 {
			if account, err := accountFriends(pid); err == nil && account != nil && account.PID == pid {
				entry.UID = cleanBanUID(account.UserID)
			} else {
				log.Printf("[NPLN bans] could not resolve PID %d to UID; PID enforcement remains active: %v", pid, err)
			}
		}
		if err := playerBans.ban(entry); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if pid != 0 {
			fmt.Printf("Banned PID %d (UID %s). Active streams will be cancelled by the running server.\n", pid, entry.UID)
		} else {
			fmt.Printf("Banned save UID %s. Active streams will be cancelled by the running server.\n", uid)
		}
		return 0
	case "unban":
		if len(args) != 2 {
			usage()
			return 2
		}
		pid, uid, err := parseBanTarget(args[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		removed, err := playerBans.unbanTarget(pid, uid)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if removed {
			if pid != 0 {
				fmt.Printf("Unbanned PID %d. New connections are allowed again.\n", pid)
			} else {
				fmt.Printf("Unbanned save UID %s. New connections are allowed again.\n", uid)
			}
		} else {
			fmt.Printf("Target %s was not on the ban list.\n", args[1])
		}
		return 0
	default:
		usage()
		return 2
	}
}
