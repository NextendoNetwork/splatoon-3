package main

// dashboard — expose /api/stats for the Nextendo monitoring aggregator (nexdash), in the SAME
// JSON shape the NEX games publish (see server/NEXtendo/mk8/dashboard.go). The aggregator is fully
// data-driven: it iterates its game list and reads connected / inLobby / activeLobbies / totalRmc /
// peakConnected / players / gatherings / events / methods, so serving that shape is all Splatoon 3
// needs to show up with the current design — no UI change.
//
// The vocabulary is NEX's; the mapping to NPLN is:
//   connected      -> players whose last gRPC call is recent (NPLN has no PRUDP "connection")
//   inLobby        -> players attached to a live game session
//   activeLobbies  -> live game sessions (host rooms + matched sessions)
//   totalRmc       -> gRPC calls served (the NPLN equivalent of an RMC)
//   gatheringsMade -> game sessions created since boot
//
// Everything is fed from the gRPC stats handler and the matchmaker, so nothing extra runs in the
// request path beyond a map write.

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
)

type dashPlayer struct {
	PID        uint64 `json:"pid"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Mode       string `json:"mode"`
	ModeKey    string `json:"modeKey,omitempty"`
	IP         string `json:"ip"`
	State      string `json:"state"`
	Gathering  uint32 `json:"gathering"`
	OnlineSecs int    `json:"onlineSeconds"`
	Calls      int64  `json:"calls"`
	LastAction string `json:"lastAction"`
	IdleSecs   int    `json:"idleSeconds"`
	Country    string `json:"country"`
	CC         string `json:"cc"`
	City       string `json:"city"`
	ISP        string `json:"isp"`
	NatType    string `json:"natType"`
	Ping       int    `json:"ping"`
}

type dashGatheringPlayer struct {
	PID  uint64 `json:"pid"`
	Name string `json:"name"`
	Host bool   `json:"host"`
	// Camp de festival du joueur — « Alpha », « Bravo », « Charlie », ou vide hors festival.
	//
	// Sans lui, un festimatch affiche huit pseudos indiscernables : impossible de voir a l'oeil nu
	// si les deux equipes ont bien ete separees par camp, ce qui est justement la garantie qu'un
	// festival mesure quelque chose. Le tableau de bord s'en sert pour colorer chaque pseudo.
	Camp string `json:"camp,omitempty"`
}

type dashGathering struct {
	ID       uint32                `json:"id"`
	HostPID  uint64                `json:"hostPid"`
	HostName string                `json:"hostName"`
	Type     string                `json:"type"`
	Mode     string                `json:"mode,omitempty"`
	Players  []dashGatheringPlayer `json:"players"`
	Count    int                   `json:"count"`
	Max      uint16                `json:"max"`
	State    string                `json:"state"`
	Code     string                `json:"code,omitempty"`
}

type dashEvent struct {
	Ago    int    `json:"agoSeconds"`
	PID    uint64 `json:"pid"`
	Action string `json:"action"`
}

type dashMethod struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type dashServer struct {
	AccessKey  string `json:"accessKey"`
	NexVersion string `json:"nexVersion"`
	AuthPort   string `json:"authPort"`
	SecurePort int    `json:"securePort"`
	SNIHost    string `json:"sniHost"`
	Stack      string `json:"stack"`
}

type dashStats struct {
	ServerTime     string          `json:"serverTime"`
	UptimeSeconds  int             `json:"uptimeSeconds"`
	Connected      int             `json:"connected"`
	InLobby        int             `json:"inLobby"`
	ActiveLobbies  int             `json:"activeLobbies"`
	TotalSessions  int64           `json:"totalSessions"`
	TotalRMC       int64           `json:"totalRmc"`
	GatheringsMade int64           `json:"gatheringsMade"`
	PeakConnected  int             `json:"peakConnected"`
	Server         dashServer      `json:"server"`
	Players        []dashPlayer    `json:"players"`
	Gatherings     []dashGathering `json:"gatherings"`
	Events         []dashEvent     `json:"events"`
	Methods        []dashMethod    `json:"methods"`
}

// ---- live state ------------------------------------------------------------

// dashSeen is one player we have served at least one RPC for. NPLN has no connection to count:
// the console holds long-lived streams (presence KeepAlive, LobbyMessaging) and calls constantly,
// so "last RPC is recent" is the honest equivalent of a NEX session.
type dashSeen struct {
	pid        uint64
	uid        string
	first      time.Time
	lastSeen   time.Time
	calls      int64
	lastMethod string
	ip         string // client address (x-forwarded-for behind Traefik, else the peer)
	ping       int    // best latency the client MEASURED itself (matchmaking LatencyData), ms
	mode       string // "recherche", "salon", ... — empty when just online
	modeKey    string // la cle stable du mode (voir cleDuMode), pour la page publique
	room       string // session name the player is currently in
}

type dashRoom struct {
	id      uint32
	kind    string
	modeKey string
	requis  int       // effectif avec lequel la partie s'est formee (0 = salon d'hote, pas apparie)
	faible  time.Time // depuis quand le salon est sous cet effectif ; zero s'il est au complet
	code    string
	hostUID string
	hostPID uint64
	members []string // uids, host first
	max     uint16
	created time.Time
}

type dashState struct {
	mu sync.RWMutex

	started   time.Time
	players   map[string]*dashSeen // uid -> player
	rooms     map[string]*dashRoom // game session name -> room
	methods   map[string]int64     // full gRPC method -> count
	events    []dashEvent          // most recent first
	eventAt   []time.Time          // parallel to events
	totalRPC  int64
	roomsMade int64
	peak      int
}

var dash = &dashState{
	started: time.Now(),
	players: map[string]*dashSeen{},
	rooms:   map[string]*dashRoom{},
	methods: map[string]int64{},
}

// dashTTL is how long after its last RPC a player still counts as online. The presence keep-alive
// contract is a 30s ping with a 50s deadline, so 90s absorbs a missed beat without leaving ghosts
// on the dashboard (the phantom-player trap the NEX side already hit once).
const dashTTL = 90 * time.Second

// dashSousEffectifTTL : combien de temps un salon apparie peut rester sous son effectif avant
// d'etre declare mort. Voir le balayage dans dashSnapshot — la marge est large a dessein, un
// faux positif fermerait une partie en cours.
const dashSousEffectifTTL = 60 * time.Second

// dashNoteRPC records one served gRPC call. Runs on every request, so it stays a map write.
func dashNoteRPC(method, uid string, pid uint64, ip string) {
	dash.mu.Lock()
	defer dash.mu.Unlock()

	dash.totalRPC++
	if method != "" {
		dash.methods[method]++
	}
	if uid == "" {
		return
	}

	now := time.Now()
	p := dash.players[uid]
	if p == nil {
		p = &dashSeen{uid: uid, first: now}
		dash.players[uid] = p
		dash.pushEventLocked(pid, "connexion NPLN")
	} else if now.Sub(p.lastSeen) > dashTTL {
		p.first = now // a new online stint: "en ligne depuis" must not count the gap
		dash.pushEventLocked(pid, "reconnexion NPLN")
	}
	p.lastSeen = now
	p.calls++
	if method != "" {
		p.lastMethod = shortMethod(method)
	}
	if ip != "" {
		p.ip = ip
	}
	if pid != 0 {
		p.pid = pid
	}

	// A player who is calling us again but was parked in a room they left keeps no stale mode;
	// the room itself expires on liveness (see dashSnapshot).
	if n := dash.liveCountLocked(now); n > dash.peak {
		dash.peak = n
	}
}

// dashTouch refreshes a player's liveness from stream traffic. It is NOT a new call: the RPC
// counter belongs to dashNoteRPC, this only says "still there".
func dashTouch(uid string, pid uint64, ip string) {
	if uid == "" {
		return
	}
	dash.mu.Lock()
	defer dash.mu.Unlock()
	p := dash.touchLocked(uid, pid)
	if p != nil && ip != "" {
		p.ip = ip
	}
}

// dashModeSearching is the label shown for a player whose matchmaking ticket is open. It is
// displayed verbatim by the aggregator, so it is written for a reader, not as an internal code.
const dashModeSearching = "Recherche de partie"

// dashNoteSearching marks a player as looking for a match (matchmaking ticket open).
func dashNoteSearching(uid string, pid uint64, mode string) {
	dashNoteSearchingMode(uid, pid, mode, "")
}

// dashNoteSearchingMode is dashNoteSearching plus the stable mode key (see cleDuMode).
func dashNoteSearchingMode(uid string, pid uint64, mode, modeKey string) {
	dash.mu.Lock()
	defer dash.mu.Unlock()
	p := dash.touchLocked(uid, pid)
	if p == nil {
		return
	}
	if p.mode != mode {
		dash.pushEventLocked(p.pid, "recherche de partie")
	}
	p.mode = mode
	p.modeKey = modeKey
	p.room = ""
}

// dashNoteRoom records a game session — a host room (private / online lobby) or a formed match —
// and who is in it. Called again on each update; members are replaced, not appended.
func dashNoteRoom(name, kind string, max uint16, hostUID string, hostPID uint64, memberUIDs []string) {
	dashNoteRoomMode(name, kind, "", 0, max, hostUID, hostPID, memberUIDs)
}

// dashNoteRoomMode is dashNoteRoom plus the stable mode key (see cleDuMode). Only the matchmaker
// knows which queue a lobby came out of, so only it can fill this in.
func dashNoteRoomMode(name, kind, modeKey string, requis int, max uint16, hostUID string, hostPID uint64, memberUIDs []string) {
	dash.mu.Lock()
	defer dash.mu.Unlock()

	r := dash.rooms[name]
	if r == nil {
		dash.roomsMade++
		r = &dashRoom{id: uint32(dash.roomsMade), created: time.Now()}
		dash.rooms[name] = r
		dash.pushEventLocked(hostPID, kind+" cree")
	}
	r.kind = kind
	if modeKey != "" {
		r.modeKey = modeKey
	}
	if requis > 0 {
		r.requis = requis
	}
	r.max = max
	r.hostUID = hostUID
	r.hostPID = hostPID
	r.members = memberUIDs

	for _, uid := range memberUIDs {
		if p := dash.touchLocked(uid, 0); p != nil {
			p.mode = kind
			p.modeKey = modeKey
			p.room = name
		}
	}
	if p := dash.touchLocked(hostUID, hostPID); p != nil {
		p.mode = kind
		p.room = name
	}
}

// dashQuitterSalons sort un joueur de TOUS les salons ou il figure, et supprime ceux qui se
// retrouvent vides.
//
// ⚠️ POURQUOI ELLE EXISTE. Un salon n'expirait que s'il n'avait plus AUCUN joueur (voir la periode
// de grace dans le rendu), or ses membres y restaient inscrits pour toujours : rien ne les en
// sortait. Mesure du 2026-08-13 : le matchmaking forme une partie, le client n'arrive pas a la
// rejoindre (2321-3072) et relance un ticket — mais le tableau de bord continuait d'afficher
// « Lobby 5 · Partie · 1/2 » avec le joueur « En partie », alors qu'il etait revenu dans la place
// sans meme chercher. Le tableau de bord AFFIRMAIT quelque chose de faux, exactement le defaut
// corrige jadis pour les joueurs fantomes.
//
// Appelee quand un joueur repart en recherche : creer un nouveau ticket prouve qu'il n'est plus
// dans la partie precedente.
// dashSupprimerSalon retire un salon du monitoring quand sa partie est REELLEMENT liberee.
//
// Sans lui, un salon abandonne restait affiche — un « 1/8 » avec son createur dedans alors qu'il
// cherchait deja autre chose. Le monitoring devenait alors trompeur au pire moment : pendant un
// test, quand on compte les joueurs presents.
func dashSupprimerSalon(name string) {
	if name == "" {
		return
	}
	dash.mu.Lock()
	defer dash.mu.Unlock()
	delete(dash.rooms, name)
	delete(dash.rooms, lastSeg(name))
}

func dashQuitterSalons(uid string) {
	if uid == "" {
		return
	}

	dash.mu.Lock()
	defer dash.mu.Unlock()

	for name, r := range dash.rooms {
		reste := r.members[:0]
		for _, m := range r.members {
			if m != uid {
				reste = append(reste, m)
			}
		}
		r.members = reste

		if r.hostUID == uid {
			r.hostUID = ""
		}

		// Plus personne dedans : le salon n'existe plus.
		if len(r.members) == 0 && r.hostUID == "" {
			delete(dash.rooms, name)
		}
	}

	if p := dash.players[uid]; p != nil {
		p.room = ""
	}
}

// dashNotePing records the best latency the CLIENT measured against the latency servers
// (matchmaking LatencyData). It is the only real RTT NPLN gives us — we never estimate one.
func dashNotePing(uid string, ms int) {
	if uid == "" || ms <= 0 {
		return
	}
	dash.mu.Lock()
	defer dash.mu.Unlock()
	if p := dash.players[uid]; p != nil {
		p.ping = ms
	}
}

// dashNoteRoomCode attaches the on-screen join code to an existing room.
func dashNoteRoomCode(name, code string) {
	dash.mu.Lock()
	defer dash.mu.Unlock()
	if r := dash.rooms[name]; r != nil {
		r.code = code
	}
}

// touchLocked returns (creating if needed) the player entry, refreshing its liveness.
func (d *dashState) touchLocked(uid string, pid uint64) *dashSeen {
	if uid == "" {
		return nil
	}
	p := d.players[uid]
	if p == nil {
		p = &dashSeen{uid: uid, first: time.Now()}
		d.players[uid] = p
	}
	p.lastSeen = time.Now()
	if pid != 0 {
		p.pid = pid
	}
	return p
}

func (d *dashState) pushEventLocked(pid uint64, action string) {
	d.events = append([]dashEvent{{PID: pid, Action: action}}, d.events...)
	d.eventAt = append([]time.Time{time.Now()}, d.eventAt...)
	if len(d.events) > 40 {
		d.events = d.events[:40]
		d.eventAt = d.eventAt[:40]
	}
}

func (d *dashState) liveCountLocked(now time.Time) int {
	n := 0
	cut := now.Add(-dashTTL)
	for _, p := range d.players {
		if p.lastSeen.After(cut) {
			n++
		}
	}
	return n
}

// ---- HTTP ------------------------------------------------------------------

func dashSnapshot() dashStats {
	dash.mu.Lock()
	defer dash.mu.Unlock()

	now := time.Now()
	cut := now.Add(-dashTTL)

	out := dashStats{
		ServerTime:     now.Format(time.RFC3339),
		UptimeSeconds:  int(now.Sub(dash.started).Seconds()),
		TotalRMC:       dash.totalRPC,
		TotalSessions:  dash.roomsMade,
		GatheringsMade: dash.roomsMade,
		PeakConnected:  dash.peak,
		Server: dashServer{
			AccessKey:  strings.TrimPrefix(npnTenant, "tenants/"),
			NexVersion: "NPLN (gRPC/HTTP2)",
			AuthPort:   "443",
			SecurePort: 7575,
			SNIHost:    "t-dce9377b-lp1.lp1.t.npln.srv.nintendo.net",
			Stack:      "npln-s3",
		},
		Players:    []dashPlayer{},
		Gatherings: []dashGathering{},
		Events:     []dashEvent{},
		Methods:    []dashMethod{},
	}

	// Rooms first: a room is alive while at least one member still talks to us. There is no
	// "leave session" RPC in the captured NPLN flow, so liveness is the only truthful signal —
	// and it keeps closed lobbies from piling up on the dashboard.
	inLobby := map[string]bool{}
	for name, r := range dash.rooms {
		g := dashGathering{
			ID:      r.id,
			HostPID: r.hostPID,
			Type:    r.kind,
			Mode:    r.modeKey,
			Max:     r.max,
			Code:    r.code,
			State:   "active",
			Players: []dashGatheringPlayer{},
		}
		seen := map[string]bool{}
		vivants := []string{}
		for _, uid := range append([]string{r.hostUID}, r.members...) {
			if uid == "" || seen[uid] {
				continue
			}
			seen[uid] = true
			p := dash.players[uid]
			if p == nil || !p.lastSeen.After(cut) {
				continue
			}
			// Un joueur n'est que dans UN salon a la fois : sans ce test, l'hote restant en ligne
			// maintenait en vie tous les salons qu'il avait quittes, et chaque nouvelle creation en
			// empilait un de plus.
			if p.room != name {
				continue
			}
			isHost := uid == r.hostUID
			g.Players = append(g.Players, dashGatheringPlayer{
				PID: p.pid, Host: isHost,
				Camp: campDuJoueur(identifiantDeFeteMaison(time.Now()), uid),
			})
			vivants = append(vivants, uid)
		}
		if len(g.Players) == 0 {
			// Give a freshly created room a grace period: the host can call CreateGameSession
			// before its first identified RPC lands.
			if now.Sub(r.created) > dashTTL {
				delete(dash.rooms, name)
			}
			continue
		}

		// UN SALON APPARIE NE PEUT QUE SE VIDER.
		//
		// formMatchLocked cree TOUJOURS une session neuve : il n'ajoute jamais personne a une
		// session existante. Un salon forme a huit qui n'en compte plus qu'un ne verra donc jamais
		// arriver les sept manquants — la partie ne se lancera pas, et le joueur restant est
		// epingle sur un salon mort. Mesure du 2026-08-20 : un salon forme a huit est tombe a un
		// pendant que sept autres joueurs repartaient en file, et il figurait toujours comme
		// « actif » sur le monitoring comme sur le site.
		//
		// On ne ferme pas au premier passage sous l'effectif : une partie qui se termine voit ses
		// joueurs repartir un a un, et la fermer trop tot supprimerait un salon encore en jeu.
		// D'ou la fenetre dashSousEffectifTTL, et le compteur remis a zero des que l'effectif est
		// retrouve.
		//
		// Les salons d'hote (requis == 0) sont exclus : un salon prive attend legitimement ses
		// invites a un contre huit.
		if r.requis > 0 && len(g.Players) < r.requis {
			if r.faible.IsZero() {
				r.faible = now
			}
			if now.Sub(r.created) > dashTTL && now.Sub(r.faible) > dashSousEffectifTTL {
				for _, uid := range vivants {
					if p := dash.players[uid]; p != nil && p.room == name {
						// Rendre le joueur a lui-meme : il n'est plus en salon, et son mode ne
						// decrit plus rien.
						p.room = ""
						p.modeKey = ""
						p.mode = ""
					}
				}
				dash.pushEventLocked(r.hostPID, r.kind+" abandonne ("+strconv.Itoa(len(g.Players))+"/"+strconv.Itoa(r.requis)+")")
				delete(dash.rooms, name)
				continue
			}
		} else {
			r.faible = time.Time{}
		}

		for _, uid := range vivants {
			inLobby[uid] = true
		}
		g.Count = len(g.Players)
		out.Gatherings = append(out.Gatherings, g)
	}
	sort.Slice(out.Gatherings, func(i, j int) bool { return out.Gatherings[i].ID < out.Gatherings[j].ID })
	out.ActiveLobbies = len(out.Gatherings)

	for uid, p := range dash.players {
		if !p.lastSeen.After(cut) {
			if now.Sub(p.lastSeen) > 6*time.Hour {
				delete(dash.players, uid)
			}
			continue // stale: never report a player who stopped talking to us
		}
		out.Connected++
		mode := p.mode
		if !inLobby[uid] && p.room != "" {
			mode = "" // was in a room that is gone; do not claim a lobby that no longer exists
		}
		if inLobby[uid] {
			out.InLobby++
		}
		state := "En ligne"
		if inLobby[uid] {
			state = "En partie"
		} else if strings.HasPrefix(p.mode, dashModeSearching) {
			// Prefixe et non egalite : l'etiquette porte desormais le mode demande
			// (« Recherche de partie — Anarchie (serie) »).
			state = "Recherche"
		}
		var gid uint32
		if inLobby[uid] {
			if r := dash.rooms[p.room]; r != nil {
				gid = r.id
			}
		}
		geo := geoLookup(p.ip)
		// No display name: the aggregator resolves the real Nextendo pseudo + avatar from the pid
		// (via /api/names). Inventing a "Joueur-XX" here is exactly what the project forbids.
		// natType stays empty on purpose: NPLN does no NAT probe (S3 negotiates P2P over ICE), so
		// there is nothing measured to report and a guess would be worse than a dash.
		out.Players = append(out.Players, dashPlayer{
			PID: p.pid, UID: p.uid, Mode: mode, ModeKey: p.modeKey, State: state, Gathering: gid,
			IP: p.ip, Ping: p.ping,
			OnlineSecs: int(now.Sub(p.first).Seconds()),
			IdleSecs:   int(now.Sub(p.lastSeen).Seconds()),
			Calls:      p.calls, LastAction: p.lastMethod,
			Country: geo.Country, CC: geo.CC, City: geo.City, ISP: geo.ISP,
		})
	}
	sort.Slice(out.Players, func(i, j int) bool { return out.Players[i].PID < out.Players[j].PID })

	for name, n := range dash.methods {
		out.Methods = append(out.Methods, dashMethod{Name: shortMethod(name), Count: n})
	}
	sort.Slice(out.Methods, func(i, j int) bool { return out.Methods[i].Count > out.Methods[j].Count })
	if len(out.Methods) > 20 {
		out.Methods = out.Methods[:20]
	}

	for i, e := range dash.events {
		e.Ago = int(now.Sub(dash.eventAt[i]).Seconds())
		out.Events = append(out.Events, e)
	}

	return out
}

// shortMethod turns "/nn.npln.toyohr.v1.Schedule/SelectVsSchedules" into
// "Schedule/SelectVsSchedules" — the aggregator shows these in a narrow column.
func shortMethod(full string) string {
	slash := strings.LastIndexByte(full, '/')
	if slash <= 0 {
		return strings.TrimPrefix(full, "/")
	}
	svc := full[:slash]
	if dot := strings.LastIndexByte(svc, '.'); dot >= 0 {
		return svc[dot+1:] + full[slash:]
	}
	return strings.TrimPrefix(full, "/")
}

// dashIdentityFromMD pulls (uid, pid) out of a request's headers: `uid` is NPLN's own user id
// metadata, the pid comes from the VERIFIED npln.ext_id claim of our access token (same check
// friends/presence use — an unverified token must never name a player on the dashboard).
func dashIdentityFromMD(md metadata.MD) (string, uint64) {
	uid, pid, _ := dashIdentityAndIP(md, "")
	return uid, pid
}

// dashIdentityAndIP also resolves the CLIENT address. Behind Traefik the gRPC peer is the proxy's
// own container IP, so the real address only exists in x-forwarded-for; on the direct :7575
// gamesync listener there is no proxy and the peer address is already the client.
func dashIdentityAndIP(md metadata.MD, peer string) (string, uint64, string) {
	uid, pid := dashIdentityFromMDOnly(md)
	ip := ipOnly(peer)
	for _, h := range []string{"x-forwarded-for", "x-real-ip"} {
		if v := md.Get(h); len(v) > 0 && v[0] != "" {
			first := v[0]
			if i := strings.IndexByte(first, ','); i > 0 {
				first = first[:i] // the left-most entry is the originating client
			}
			if f := strings.TrimSpace(first); f != "" {
				ip = f
				break
			}
		}
	}
	return uid, pid, ip
}

func dashIdentityFromMDOnly(md metadata.MD) (string, uint64) {
	uid := ""
	if v := md.Get("uid"); len(v) > 0 {
		uid = v[0]
	}
	var pid uint64
	for _, a := range md.Get("authorization") {
		a = strings.TrimPrefix(strings.TrimPrefix(a, "Bearer "), "bearer ")
		if p, ok := pidFromJWT(a); ok {
			pid = p
			break
		}
	}
	if uid == "" && pid != 0 {
		// No uid header (gamesync transport): key on the pid so the player is still one row.
		uid = "pid-" + strconv.FormatUint(pid, 10)
	}
	return uid, pid
}

// ---- GeoIP (best-effort, cached) — same source and behaviour as the NEX dashboards ------

type geoInfo struct {
	Country string `json:"country"`
	CC      string `json:"countryCode"`
	City    string `json:"city"`
	ISP     string `json:"isp"`
}

var (
	geoMu     sync.Mutex
	geoCache  = map[string]geoInfo{}
	geoClient = &http.Client{Timeout: 4 * time.Second}
)

// geoLookup returns a cached geolocation, fetching it once asynchronously: the first call for an
// IP returns empty and starts the fetch, later calls get the result. Never blocks /api/stats.
func geoLookup(addr string) geoInfo {
	ip := ipOnly(addr)
	if ip == "" || isPrivateIP(ip) {
		return geoInfo{}
	}
	geoMu.Lock()
	g, ok := geoCache[ip]
	if !ok {
		geoCache[ip] = geoInfo{} // placeholder so we fetch once
		geoMu.Unlock()
		go func() {
			var gi geoInfo
			resp, err := geoClient.Get("http://ip-api.com/json/" + ip + "?fields=country,countryCode,city,isp")
			if err == nil {
				defer resp.Body.Close()
				_ = json.NewDecoder(resp.Body).Decode(&gi)
			}
			geoMu.Lock()
			geoCache[ip] = gi
			geoMu.Unlock()
		}()
		return geoInfo{}
	}
	geoMu.Unlock()
	return g
}

func ipOnly(addr string) string {
	if strings.HasPrefix(addr, "[") { // [v6]:port
		if i := strings.Index(addr, "]"); i > 0 {
			return addr[1:i]
		}
	}
	if i := strings.LastIndex(addr, ":"); i > 0 && strings.Count(addr, ":") == 1 {
		return addr[:i]
	}
	return addr
}

// isPrivateIP keeps us from geolocating Traefik's own container address: behind the proxy the peer
// is 10.x, and showing "localisation de 10.0.1.6" would be a fabricated location.
func isPrivateIP(ip string) bool {
	return strings.HasPrefix(ip, "10.") || strings.HasPrefix(ip, "127.") ||
		strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "172.") ||
		strings.HasPrefix(ip, "::1") || ip == ""
}

// startDashboard serves /api/stats for the monitoring aggregator. Same `?key=` token scheme as the
// NEX dashboards, so nexdash needs no special case for Splatoon 3.
func startDashboard(addr, token string) {
	mux := http.NewServeMux()
	// Rotation des stages, calculable A L'AVANCE. Sert au futur site splatoon.nextendo.network :
	// la rotation etant une fonction pure du numero de creneau, on peut rendre n'importe quelle
	// plage, passee comme future, sans rien stocker. Publique et en lecture seule — elle ne revele
	// que ce que tout joueur voit deja dans le hall.
	mux.HandleFunc("/api/rotation", rotationHandler)
	// Quel logo d'ecran-titre afficher : voir fest_logo.go. Accroche a la posture de fete,
	// donc il s'eteint tout seul quand la fete se termine.
	mux.HandleFunc("/api/fest-logo", festLogoHandler)

	// Les quarts de Salmon Run, servis par le meme chemin que le jeu (voir coop_api.go).
	mux.HandleFunc("/api/coop", coopHandler)

	// Qui joue en ce moment — vue publique et expurgee du monitoring (voir online_api.go).
	mux.HandleFunc("/api/online", onlineHandler)

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.URL.Query().Get("key") != token {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(dashSnapshot())
	})

	// Tout ce qu'on sait de la fete en cours (voir fest_api.go). Meme jeton que /api/stats :
	// elle nomme des joueurs, elle n'a rien a faire en acces libre.
	mux.HandleFunc("/api/splatfest", func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.URL.Query().Get("key") != token {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		festAPIHandler(w, r)
	})

	// Pont vers l'espace perso du site : nextendo-account vient y lire la sauvegarde S3.
	mux.HandleFunc("/api/save-record", handleSaveRecord)

	log.Printf("Nextendo NPLN dashboard listening on %s (/api/stats, /api/save-record) — Splatoon 3", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("dashboard %s: %v", addr, err)
	}
}

// dashPIDFromCtx is dashIdentityFromMD's pid for a handler that already has the context.
func dashPIDFromCtx(ctx context.Context) uint64 {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0
	}
	_, pid := dashIdentityFromMD(md)
	return pid
}

// nplnUserOnline reports whether that NPLN user is talking to us right now. The presence service
// uses it so a friend is only announced ONLINE when they really are — the monitoring liveness and
// the in-game presence must never disagree.
func nplnUserOnline(uid string) bool {
	if uid == "" {
		return false
	}
	dash.mu.RLock()
	defer dash.mu.RUnlock()
	p := dash.players[uid]
	return p != nil && time.Since(p.lastSeen) < dashTTL
}

// dashJoueurParti retire un joueur des l'instant ou sa derniere connexion HTTP/2 se ferme.
//
// La duree de grace dashTTL protege un joueur dont un battement de presence s'est perdu ; elle n'a
// pas lieu de s'appliquer quand le depart est CERTAIN. Sans ca, fermer le jeu laissait le joueur
// affiche « en ligne » pendant quatre-vingt-dix secondes — ce que l'on voit sur le tableau de bord
// comme un joueur fantome, exactement le travers deja corrige cote NEX.
func dashJoueurParti(uid string) {
	dash.mu.Lock()
	defer dash.mu.Unlock()

	p := dash.players[uid]
	if p == nil {
		return
	}
	delete(dash.players, uid)
	log.Printf("[NPLN dash] %s deconnecte (fin de sa derniere connexion) — retire du tableau de bord", uid)
	dash.pushEventLocked(p.pid, "deconnexion NPLN")
}
