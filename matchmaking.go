package main

// Matchmaking — our private implementation of nn.npln.matchmaking.v1, the gRPC
// matchmaking S3 uses. Modelled byte-for-byte on a real Switch capture
// (dist/Nextendo-MITM-Splatoon3v3): the Switch pings ListLatencyMeasurementServers,
// then CreateMatchmakingTicket → TrackMatchmakingTicket (server-stream:
// SEARCHING → PLACING → SUCCEEDED with a GameSession + per-user session token),
// then AllocateIceServerSet for the STUN/TURN it uses to reach the session host.
//
// Real S3 is NOT pure P2P: the matched ticket points at a dedicated session server
// (Agones on GCP, an ip:port) that all 8 players connect to over ICE. We return a
// GameSession pointing at OUR relay (NPLN_RELAY_HOST:PORT) and our own coturn for
// STUN/TURN. Standing up that relay/session host is the next milestone; this layer
// makes S3 accept the matchmaking and attempt the connection.

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	mmpb "npln.nintendo.net/npln-practice/proto/matchmaking/v1"
)

var rawTrackGameSessionCreationTicket = capture("captured/TrackGameSessionCreationTicket.bin")

var rawTrackMatchmakingTicket = capture("captured/TrackMatchmakingTicket.bin")

var rawAllocateIceServerSet = capture("captured/AllocateIceServerSet.bin")

// Tenant of Splatoon 3 (the client sends "tenants/current"; we resolve to this).
const npnTenant = "tenants/t-dce9377b-lp1"

// ---- config (env-overridable) ----

func mmConfig() (relayHost string, relayPort int32, stunHost string, stunPort int32, turnHost string, turnPort int32, turnSecret string, matchSize int32, latencyHost string) {
	relayHost = envOr("NPLN_RELAY_HOST", "127.0.0.1")
	relayPort = envInt("NPLN_RELAY_PORT", 7575)

	// [Nextendo] Drapeau a chaud « relaynom » : annoncer l'hote de session par son NOM sur :443
	// plutot que par IP litterale sur :7575.
	//
	// Mesure du 2026-08-13 — c'est la SEULE difference structurelle qui reste entre le canal qui
	// fonctionne et celui qui echoue :
	//     t-dce9377b-lp1.lp1.t.npln.srv.nintendo.net  :443   -> le jeu emet ses requetes   OK
	//     203.0.113.7 (litteral)                    :7575  -> canal muet, EOF a ~1 s     ECHEC
	// Le service Gamesync est enregistre sur les DEUX serveurs (main.go, buildServer), donc pointer
	// la session sur le point de terminaison principal est legitime et n'enleve aucune fonction.
	// Nintendo, lui, met bien une IP dans game_session.host — mais son infrastructure n'a pas notre
	// resolveur MITM entre les deux.
	if soirFlag("relaynom") {
		relayHost = envOr("NPLN_RELAY_NAME", "t-dce9377b-lp1.lp1.t.npln.srv.nintendo.net")
		relayPort = int32(envInt("NPLN_RELAY_NAME_PORT", 443))
	}
	stunHost = envOr("NPLN_STUN_HOST", "127.0.0.1")
	stunPort = envInt("NPLN_STUN_PORT", 3478)
	turnHost = envOr("NPLN_TURN_HOST", "127.0.0.1")
	turnPort = envInt("NPLN_TURN_PORT", 3478)
	// Aucun secret par defaut : un depot public ne livre pas la cle d'un deploiement. Vide, les
	// identifiants TURN ne sont pas signes et le relais refusera les allocations — reglez
	// NPLN_TURN_SECRET sur le meme secret que votre coturn (« static-auth-secret »).
	turnSecret = envOr("NPLN_TURN_SECRET", "")
	// 1 = instant solo match (for protocol testing); 8 = a real regular battle room.
	matchSize = envInt("NPLN_MATCH_SIZE", 1)

	// [Nextendo] Drapeau a chaud « mmsolo » : former le match des qu'UN joueur attend.
	// Sert quand on teste sans second joueur — la mesure visee (le SNI presente a l'hote de
	// session) se produit des le SUCCEEDED, un seul client suffit a la produire.
	if soirFlag("mmsolo") {
		matchSize = 1
	}

	// [Nextendo] Drapeau a chaud « mmtaille=<n> » : nombre de joueurs distincts a reunir avant de
	// former UNE partie.
	//
	// Mesure du 2026-08-14 : avec le seuil a 2, quatre joueurs en recherche donnaient DEUX parties
	// de deux au lieu d'un seul groupe — visible directement dans le monitoring. Or Splatoon 3
	// verifie l'effectif dans son propre binaire et refuse de lancer une Guerre de territoire a
	// moins de huit ; apparier par paires ne mene donc nulle part. Le seuil se regle a chaud pour
	// suivre le nombre de testeurs reellement presents.
	if v := soirFlagValeur("mmtaille"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			matchSize = int32(n)
		}
	}
	latencyHost = envOr("NPLN_LATENCY_HOST", "127.0.0.1")
	return
}

func envInt(k string, d int32) int32 {
	if v := envOr(k, ""); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return int32(n)
		}
	}
	return d
}

func uuid4() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// lastSeg returns the final path segment of a resource name (its opaque id). The
// client refers to a creation ticket under "tenants/current/gameSessionCreationTickets/<id>"
// even though we mint the name under the concrete "tenants/t-xxx-lp1/..." tenant, so we key
// and look up by <id> to survive that tenant-alias difference.
func lastSeg(name string) string {
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// tenantFromCtx resolves the CALLER's concrete tenant from the npln-tenant-id
// metadata (e.g. "t-adf89f68-lp1" for MP Jamboree, "t-dce9377b-lp1" for S3),
// returning "tenants/<id>". Resource names we mint MUST live under the caller's
// own tenant: a client rejects a GameSession whose name references a foreign
// tenant (MPJ was handed an S3-tenant session and bounced back to the menu).
// Falls back to the S3 const when the metadata is absent.
func tenantFromCtx(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("npln-tenant-id"); len(v) > 0 && v[0] != "" {
			return "tenants/" + v[0]
		}
	}
	return npnTenant
}

// uidFromCtx returns the caller's user id from the `uid` metadata (e.g.
// "u-exemple5000000000000"), used as the `sub` of the session JWT we mint.
func uidFromCtx(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("uid"); len(v) > 0 && v[0] != "" {
			return v[0]
		}
	}
	return ""
}

// ---- common.Value helpers (the gamesync attribute value oneof) ----

func vStr(s string) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_StringValue{StringValue: s}}
}
func vInt(i int64) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_IntegerValue{IntegerValue: i}}
}
func vDouble(f float64) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_DoubleValue{DoubleValue: f}}
}

// =====================================================================
// GameSessionService — AllocateIceServerSet + ListLatencyMeasurementServers
// =====================================================================

type gameSessionServer struct {
	mmpb.UnimplementedGameSessionServiceServer

	mu       sync.Mutex
	tickets  map[string]*mmpb.GameSessionCreationTicket // creation-ticket name -> ticket
	sessions map[string]*mmpb.GameSession               // session name -> session
	aliases  map[string]string                          // short CODE -> session name
}

func newGameSessionServer() *gameSessionServer {
	return &gameSessionServer{
		tickets:  map[string]*mmpb.GameSessionCreationTicket{},
		sessions: map[string]*mmpb.GameSession{},
		aliases:  map[string]string{},
	}
}

// regions S3 pings (mirrors Nintendo's set, but all pointed at our latency host).
var latencyRegions = []struct{ id, region string }{
	{"eu-01-udp", "europe-central2"},
	{"eu-02-udp", "europe-west2"},
	{"us-01-udp", "northamerica-northeast1"},
	{"us-02-udp", "us-west2"},
	{"jp-01-udp", "asia-northeast1"},
	{"sg-01-udp", "asia-southeast1"},
	{"au-01-udp", "australia-southeast1"},
}

func (g *gameSessionServer) ListLatencyMeasurementServers(ctx context.Context, req *mmpb.ListLatencyMeasurementServersRequest) (*mmpb.ListLatencyMeasurementServersResponse, error) {
	_, _, _, _, _, _, _, _, latencyHost := mmConfig()
	out := &mmpb.ListLatencyMeasurementServersResponse{}
	// Return the full latency-server list (matches the config that reached the square):
	// S3 needs to receive a server list to proceed. It pings them over UDP; even if those
	// pings time out on the emulator, having the list lets S3 continue. Returning an EMPTY
	// list makes S3 abort earlier. Set NPLN_LATENCY_EMPTY=1 to force the empty variant.
	if os.Getenv("NPLN_LATENCY_EMPTY") != "" {
		log.Printf("[NPLN MM] ListLatencyMeasurementServers -> 0 (forced empty)")
		return out, nil
	}
	for _, r := range latencyRegions {
		out.LatencyMeasurementServers = append(out.LatencyMeasurementServers, &mmpb.LatencyMeasurementServer{
			Name:     npnTenant + "/latencyMeasurementServers/" + r.id,
			Region:   r.region,
			Host:     latencyHost,
			Port:     3478,
			Protocol: mmpb.LatencyMeasurementServer_UDP,
		})
	}
	log.Printf("[NPLN MM] ListLatencyMeasurementServers -> %d servers @ %s", len(out.LatencyMeasurementServers), latencyHost)
	return out, nil
}

// AllocateIceServerSet returns our STUN + TURN. TURN uses coturn's REST ephemeral
// credential scheme: username = "<unix-expiry>:<user>", password = base64(HMAC-SHA1(secret, username)).
func (g *gameSessionServer) AllocateIceServerSet(ctx context.Context, req *mmpb.AllocateIceServerSetRequest) (*mmpb.IceServerSet, error) {
	_, _, stunHost, stunPort, turnHost, turnPort, turnSecret, _, _ := mmConfig()
	tn := tenantFromCtx(ctx)
	// L'identifiant TURN doit porter l'utilisateur CONCRET, jamais un repli partage.
	//
	// Mesure du 2026-08-15, capture reelle : la requete ne porte que « tenants/current » (le parent),
	// donc req.GetUser() est VIDE et nous retombions sur capturedUser — la meme identite pour tout le
	// monde, alors que Nintendo repond
	// « 1786797551:tenants/t-dce9377b-lp1/users/u-exemple6000000000000 ». Deux joueurs partageant un
	// meme identifiant TURN se disputent la meme allocation sur le relais.
	user := req.GetUser()
	if user == "" {
		if uid := uidFromCtx(ctx); uid != "" {
			user = tn + "/users/" + uid
		} else {
			user = tn + "/users/" + capturedUser
		}
	}
	// 120 s EXACTEMENT, comme Nintendo.
	//
	// Mesure sur captured/AllocateIceServerSet.bin : l identifiant porte 1786138031 pour un updateTime
	// de 1786137911, soit 120 s pile, avec clientCacheDuration=90 s. Le client refait donc l appel avant
	// expiration et dispose toujours d un identifiant frais — allonger la validite s ecarte du corpus
	// sans rien resoudre.
	// [Nextendo 2026-08-25] IDENTIFIANT TURN VALABLE UNE HEURE, ET NON DEUX MINUTES.
	//
	// Mesure sur les captures Nintendo (locataire-00X, port 443) : les identifiants successifs d'une
	// meme console portent des expirations espacees de 122 a 543 secondes — le jeu ne reallouе que
	// toutes les deux a neuf minutes. Avec deux minutes de validite, nos identifiants etaient donc
	// morts AVANT la reallocation suivante : le relais refusait les paquets (929 « error 401:
	// Unauthorized » releves dans /var/log/turn.log) et le P2P tombait, ce qui tue le salon avant
	// meme le debut de la partie.
	//
	// Une heure couvre largement l'ecart le plus long observe. Le mot de passe reste un HMAC lie a
	// l'utilisateur et a cette expiration : allonger la duree n'ouvre le relais a personne d'autre.
	exp := time.Now().Add(time.Hour).Unix()
	turnUser := fmt.Sprintf("%d:%s", exp, user)
	mac := hmac.New(sha1.New, []byte(turnSecret))
	mac.Write([]byte(turnUser))
	turnPass := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	set := &mmpb.IceServerSet{
		Name:       tn + "/iceServerSets/static",
		StunServer: &mmpb.StunServer{Host: stunHost, Port: stunPort, Protocol: mmpb.StunServer_UDP},
		TurnServers: []*mmpb.TurnServer{{
			Host:     turnHost,
			Port:     turnPort,
			Protocol: mmpb.TurnServer_UDP,
			Username: turnUser,
			Password: turnPass,
		}},
		// ttl PRESENT mais VIDE, comme Nintendo.
		//
		// Mesure du 2026-08-15, capture reelle d'AllocateIceServerSet : le champ 4 y figure avec une
		// longueur de ZERO octet. Nous ne l'emettions pas du tout. Or S3 distingue « absent » de
		// « present mais vide » — c'est la lecon qui nous a deja coutee sur GetDocument (un document
		// absent doit porter son NOM) et sur les documents de salle. Un champ manquant peut faire
		// rejeter tout l'objet, et le jeu n'emet alors aucune sonde STUN : mesure du meme jour, zero
		// paquet vers notre relais alors que l'adresse etait resolue et le port correct.
		Ttl:        &durationpb.Duration{},
		UpdateTime: timestamppb.Now(),
		// [Nextendo 2026-08-25] REGLABLE, parce que cette valeur est LA NOTRE et qu'elle colle au
		// symptome. Mesure du 25/08 : les connexions du jeu se fermaient sur EOF — donc a son
		// initiative — toutes les 90 secondes EXACTEMENT, six ecarts d'affilee. Or c'est la duree que
		// nous lui annoncons ici. Chaque fermeture emporte les appels en vol : trois methodes sans
		// rapport (SelectCoopSchedules, GetSaveRecord, SubscribeFriendUsers) ont rendu 2321-4992 a la
		// meme seconde, ce qui exclut un defaut propre a l'une d'elles.
		//
		// Aucune capture ne nous dit ce que Nintendo annonce ici ; le commentaire ci-dessus ne
		// documente que le Ttl vide. Le drapeau « icecache=<secondes> » permet donc d'eprouver
		// d'autres valeurs sans redeployer, en gardant 90 s par defaut tant que rien ne le contredit.
		ClientCacheDuration: durationpb.New(dureeCacheIce()),
	}
	log.Printf("[NPLN MM] AllocateIceServerSet user=%q -> STUN %s:%d TURN %s:%d", short(user), stunHost, stunPort, turnHost, turnPort)
	return set, nil
}

// IssueUserDelegationToken lets a group leader mint a token to act on behalf of a teammate
// (mandatary) with match attributes — Anarchy Series team play (capture 2026-08-07: request carries
// delegator="tenants/current/users/current" + mandatary=concrete teammate + match attrs; response
// echoes them in a UserDelegationDetail with an ES256 token + TTL). GameSessionService IS registered,
// so leaving this out returned Unimplemented (no generic replay fallback), breaking team matchmaking.
func (g *gameSessionServer) IssueUserDelegationToken(ctx context.Context, req *mmpb.IssueUserDelegationTokenRequest) (*mmpb.IssueUserDelegationTokenResponse, error) {
	tn := tenantFromCtx(ctx)
	uid := uidFromCtx(ctx)
	if uid == "" {
		uid = capturedUser
	}
	delegator := req.GetDelegatorUser()
	if delegator == "" || strings.HasSuffix(delegator, "/current") {
		delegator = tn + "/users/" + uid
	}
	mandatary := req.GetMandataryUser() // the teammate the token delegates for (concrete resource)
	// ES256 delegation token bound to the mandatary, same signer/jku/kid as the access/session tokens.
	token := mintSessionToken(userIDFromPath(mandatary), tn, tn+"/delegations/"+uuid4(), mandatary)
	detail := &mmpb.UserDelegationDetail{
		DelegatorUser:       delegator,
		MandataryUser:       mandatary,
		Attributes:          req.GetAttributes(),
		DelegationActions:   req.GetDelegationActions(),
		UserDelegationToken: token,
		Ttl:                 durationpb.New(nplnTokenTTL),
	}
	log.Printf("[NPLN MM] IssueUserDelegationToken delegator=%s mandatary=%s -> OK", short(delegator), short(mandatary))
	return &mmpb.IssueUserDelegationTokenResponse{UserDelegationDetail: detail}, nil
}

// =====================================================================
// GameSessionService — direct room creation (MP Jamboree "create party")
//
// Host flow captured from MPJ: CreateGameSessionCreationTicket (the host asks to
// open a room, sending its player + game-mode props) -> TrackGameSessionCreationTicket
// (server-stream until SUCCEEDED, carrying a GameSession that points at our relay) ->
// CreateGameSessionShortAlias (mint the join CODE the host shows on screen). Because
// the host itself creates the room, we succeed immediately (no matchmaking search).
// This is separate from S3's Matchmaker search flow below.
// =====================================================================

// join codes: unambiguous uppercase + digits (no O/0/I/1).
const shortCodeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func genShortCode(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = shortCodeAlphabet[int(b[i])%len(shortCodeAlphabet)]
	}
	return string(b)
}

// roomCodeFor derives a STABLE 6-char join code from the game-session name. Both the alias
// registration (CreateGameSessionShortAlias, :443) and the __gs/s gamesync document (:7575) call this
// with the same gsName, so the code the host reads out of __gs/s (into [bexDetail+0xf70], shown in Room
// Info) is the SAME code registered for join. Deterministic = the two separate server processes agree
// without shared state.
func roomCodeFor(gsName string) string {
	return roomCodeFromSeed(lastSeg(gsName))
}

// roomCodeFromSeed: FNV-1a 64-bit over the seed, mapped to 6 unambiguous chars. Kept deliberately
// simple so the subsdk (C++) reproduces it BYTE-FOR-BYTE from the same seed (the game-session uuid it
// reads out of bexDetail), guaranteeing the displayed code == the alias code registered here.
func roomCodeFromSeed(seed string) string {
	var h uint64 = 0xcbf29ce484222325
	for i := 0; i < len(seed); i++ {
		h ^= uint64(seed[i])
		h *= 0x100000001b3
	}
	n := uint64(len(shortCodeAlphabet))
	b := make([]byte, 6)
	for i := 0; i < 6; i++ {
		b[i] = shortCodeAlphabet[h%n]
		h /= n
	}
	return string(b)
}

// pwNeededField is the protobuf field number of GameSession.is_password_needed — the dedicated lock flag
// the client reads (struct +0x62), separate from is_public(field 5)/password(field 6). It's absent from
// our generated .proto (fields 1..12); RE + the append convention point to 13. NPLN_PWFLAG_FIELD lets us
// A/B-sweep the number without rebuilding (the lite parse table is stripped, so 13 is a best guess).
func parsePwFields(v string) []protowire.Number {
	var out []protowire.Number
	for _, part := range strings.Split(v, ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && n > 0 {
			out = append(out, protowire.Number(n))
		}
	}
	return out
}

func pwNeededFields() []protowire.Number {
	// File override (hot-swappable without restart/rebuild — write it with:
	//   docker exec npln sh -c 'echo 14,15,16 > /data/pwflag'
	// so we can A/B-sweep the stripped-parse-table field number by just re-creating a room).
	path := os.Getenv("NPLN_PWFLAG_FILE")
	if path == "" {
		path = "/data/pwflag"
	}
	if b, err := os.ReadFile(path); err == nil {
		s := strings.TrimSpace(string(b))
		if s == "none" || s == "off" || s == "0" {
			return nil // explicit disable — no injection (isolate side effects on the member count)
		}
		if fs := parsePwFields(s); len(fs) > 0 {
			return fs
		}
	}
	if fs := parsePwFields(os.Getenv("NPLN_PWFLAG_FIELD")); len(fs) > 0 {
		return fs
	}
	return nil // default: no injection until we know the real field number
}

// markPasswordNeeded appends is_password_needed=true onto the GameSession as wire-level unknown field(s).
// Our proto has no such field and hand-patching the binary rawDesc is risky; the client parses by field
// number regardless of our descriptor, so a raw varint bool drives its room-info lock indicator. The
// exact number is stripped from the lite parse table, so NPLN_PWFLAG_FIELD accepts a comma list to sweep
// several candidates in one shot (unknown numbers the client's proto lacks are simply ignored by it).
func markPasswordNeeded(gs *mmpb.GameSession) {
	if gs == nil {
		return
	}
	m := gs.ProtoReflect()
	raw := m.GetUnknown()
	for _, f := range pwNeededFields() {
		raw = protowire.AppendTag(raw, f, protowire.VarintType)
		raw = protowire.AppendVarint(raw, 1)
	}
	m.SetUnknown(raw)
}

// buildHostSession turns the host's requested GameSession into our live room,
// pointing participants at our relay while preserving the host's props/game mode.
func buildHostSession(gsName string, reqGs *mmpb.GameSession, cfg string) *mmpb.GameSession {
	room := &mmpb.GameSession{
		Name:                    gsName,
		MaxParticipantCount:     4, // MP Jamboree online party = up to 4 players
		CurrentParticipantCount: 1,
		CanParticipate:          true,
		State:                   mmpb.GameSession_ACTIVE,
		CreateTime:              timestamppb.Now(),
	}
	// host:port is REQUIRED: empty makes the host error 2321-4608 immediately. It points at our
	// relay; the host tries to REACH it (nothing there yet -> it waits then drops), which is the
	// session-server milestone. NPLN_HOST_EMPTY=1 forces the empty variant for A/B testing.
	if os.Getenv("NPLN_HOST_EMPTY") == "" {
		relayHost, relayPort, _, _, _, _, _, _, _ := mmConfig()
		room.Host = relayHost
		room.Port = relayPort
	}
	if reqGs != nil {
		room.Properties = reqGs.GetProperties()
		room.IsPublic = reqGs.GetIsPublic()
		room.Password = reqGs.GetPassword()
		if reqGs.GetMaxParticipantCount() > 0 {
			room.MaxParticipantCount = reqGs.GetMaxParticipantCount()
		}
	}
	// Bridge the room lock state to the gamesync __stg readback so the host shows the correct
	// "password on/off" (a password makes the room non-public regardless of the client's is_public).
	rememberRoomSettings(lastSeg(gsName), room.Password, room.IsPublic && room.Password == "", baseConfigName(cfg), room.GetProperties())
	// The room-info lock indicator is driven by a dedicated GameSession.is_password_needed bool (client
	// reads struct +0x62), NOT by is_public/password/the gamesync rs map. That field isn't in our .proto,
	// so inject it on the wire when a password is set.
	if room.Password != "" {
		markPasswordNeeded(room)
	}
	return room
}

func (g *gameSessionServer) CreateGameSessionCreationTicket(ctx context.Context, req *mmpb.CreateGameSessionCreationTicketRequest) (*mmpb.GameSessionCreationTicket, error) {
	// DIAG: dump the FULL request so we mirror exactly the GameSession fields MPJ
	// populates (hence expects back). Remove once the create-room flow is settled.
	log.Printf("[NPLN MM][DIAG] CreateGameSessionCreationTicket request =\n%s", prototext.Format(req))
	tn := tenantFromCtx(ctx)
	in := req.GetGameSessionCreationTicket()
	ticketName := tn + "/gameSessionCreationTickets/" + uuid4()
	gsName := tn + "/gameSessions/" + uuid4()

	var uds []*mmpb.UserDefinition
	cfg := ""
	var reqGs *mmpb.GameSession
	if in != nil {
		uds = in.GetUserDefinitions()
		cfg = in.GetMatchmakingConfig()
		reqGs = in.GetGameSession()
	}
	var hostUd *mmpb.UserDefinition
	if len(uds) > 0 {
		hostUd = uds[0]
	}

	// Resolve the caller's "current" alias to its CONCRETE user resource. The real Nintendo
	// server never echoes "tenants/current/users/current" back — the captured AllocateIceServerSet
	// reply carries the concrete "tenants/<tenant>/users/<uid>". The host identifies its own
	// session by that concrete id, so echoing "current" makes it fail to find itself in the room
	// (SessionAlone / JoinedSessionEmpty) and bounce. Build the concrete user from the metadata.
	uid := uidFromCtx(ctx)
	if uid == "" {
		uid = capturedUser
	}
	concreteUser := tn + "/users/" + uid

	// Concrete host UserDefinition (same attrs/latency/team, user resolved). Don't shallow-copy a
	// proto struct (it has internal state) — build a fresh one.
	hostUdConcrete := &mmpb.UserDefinition{User: concreteUser}
	if hostUd != nil {
		hostUdConcrete.Attributes = hostUd.GetAttributes()
		hostUdConcrete.LatencyData = hostUd.GetLatencyData()
		hostUdConcrete.Team = hostUd.GetTeam()
	}

	room := buildHostSession(gsName, reqGs, cfg)
	userSess := gsName + "/userSessions/" + uuid4()

	room.CurrentParticipantCount = 1

	if isS3Tenant(tn) {
		// La GameSession d'un salon privé S3 est ENTIÈREMENT fabriquée par nous : la requête du jeu
		// ne porte que matchmaking_config + user_definitions (aucun game_session). On la calque donc
		// sur la capture du même flux (captured/TrackGameSessionCreationTicket.bin, décodée champ par
		// champ) plutôt que sur des valeurs par défaut :
		//   - champs 1..11 seulement — Nintendo n'envoie PAS user_sessions (champ 12) ;
		//   - is_public = true (champ 5), même pour un salon privé ;
		//   - properties = _BaseConfigName (la config DEMANDÉE) + _AliasSuffix vide.
		// Le jeu abandonnait au niveau du ticket : son rapport d'erreur donne ApiType CreateSession,
		// Mode private_match_config, CommonError CreateSessionFailed et un ServerSession.Id VIDE — il
		// n'avait retenu aucune session, et refermait de lui-même le canal :7575 ouvert d'avance.
		room.UserSessions = nil
		// L'hote reste connu a part, pour la reponse GetGameSession (voir participantsParGsid).
		noterParticipant(lastSeg(gsName), &mmpb.UserSession{
			Name:       userSess,
			User:       concreteUser,
			State:      mmpb.UserSession_ACTIVE,
			Attributes: hostUdConcrete.GetAttributes(),
			CreateTime: timestamppb.Now(),
		})
		room.IsPublic = true
		room.MaxParticipantCount = s3RoomCapacity(cfg, room.GetMaxParticipantCount())
		room.Properties = s3SessionProperties(cfg, reqGs.GetProperties())
	} else {
		// MP Jamboree : l'hôte doit trouver sa propre user session DANS le salon.
		// MatchedUserSessions[i].UserSession n'est qu'une référence par NOM ; les objets UserSession
		// vivent dans GameSession.user_sessions (champ 12).
		room.UserSessions = []*mmpb.UserSession{{
			Name:       userSess,
			User:       concreteUser,
			State:      mmpb.UserSession_ACTIVE,
			Attributes: hostUdConcrete.GetAttributes(),
			CreateTime: timestamppb.Now(),
		}}
	}

	t := &mmpb.GameSessionCreationTicket{
		Name:              ticketName,
		MatchmakingConfig: s3ConfigEcho(tn, cfg),
		UserDefinitions:   []*mmpb.UserDefinition{hostUdConcrete},
		State:             mmpb.GameSessionCreationTicket_SUCCEEDED,
		GameSession:       room,
		MatchedUserSessions: []*mmpb.MatchedUserSession{{
			UserDefinition: hostUdConcrete,
			UserSession:    userSess,
			// Le salon PRIVÉ doit porter le MÊME jeton que le match public. La capture Nintendo de
			// ce flux (captured/TrackGameSessionCreationTicket.bin, config "coop_private_config",
			// MatchedUserSession champ 3) contient un JWT { iss:"gss", gamesync:{gsid,usid,uid,tid,
			// team,attr,ltcy,typ} } : ce sont ces identifiants que npl1 lit pour construire
			// Gamesync/IssueToken puis KeepUserSession. mintSessionToken ne les portait pas
			// (iss "default iss" + {game_session,user_session}) — le client ouvrait bien TCP+TLS+h2
			// vers :7575 puis n'émettait JAMAIS de frame HEADERS, la connexion tombait en EOF au
			// bout d'~1 s et le jeu rendait 2321-4992 sur KeepUserSession.
			MatchmakingIdToken: hostSessionToken(tn, uid, gsName, userSess, hostUdConcrete),
		}},
	}

	g.mu.Lock()
	g.tickets[lastSeg(ticketName)] = t // full SUCCEEDED ticket, delivered by Track
	g.sessions[lastSeg(gsName)] = room
	g.mu.Unlock()

	dashNoteRoom(gsName, "Salon", uint16(room.GetMaxParticipantCount()), uid, dashPIDFromCtx(ctx), nil)
	// L'hôte joint ses latences MESURÉES dans sa UserDefinition : c'est le vrai ping du joueur.
	dashNotePing(uid, bestLatencyMs(hostUdConcrete.GetLatencyData()))

	// The host has a creation LIFECYCLE state machine: it observed us jump straight to SUCCEEDED
	// (never PENDING) and, unable to follow the transition, sat idle and eventually CANCELLED the
	// ticket. Return the ticket as PENDING ("creation in progress"); Track then streams
	// PENDING -> SUCCEEDED so the host sees the real progression.
	pending := &mmpb.GameSessionCreationTicket{
		Name:              ticketName,
		MatchmakingConfig: s3ConfigEcho(tn, cfg),
		UserDefinitions:   t.GetUserDefinitions(),
		State:             mmpb.GameSessionCreationTicket_PENDING,
		GameSession:       s3PendingGameSession(tn, reqGs),
	}

	tok := t.GetMatchedUserSessions()[0].GetMatchmakingIdToken()
	log.Printf("[NPLN MM][DIAG] session token parts=%d head=%.16s", strings.Count(tok, ".")+1, tok)
	log.Printf("[NPLN MM] CreateGameSessionCreationTicket host=%q -> ticket=%s (PENDING) session=%s @ %s:%d",
		short(hostUd.GetUser()), ticketName, gsName, room.GetHost(), room.GetPort())
	return pending, nil
}

// isS3Tenant : ce serveur est multi-locataire (Splatoon 3 et Mario Party Jamboree partagent les
// mêmes handlers). Tout ce qui est calqué sur une capture S3 doit être gaté ici, sinon on change
// aussi le comportement d'un jeu qui marche.
func isS3Tenant(tenant string) bool { return strings.Contains(tenant, "dce9377b") }

// hostSessionToken choisit la forme du jeton de session d'un salon HÔTE selon le locataire.
//
// La capture Nintendo de ce flux (captured/TrackGameSessionCreationTicket.bin, config
// "coop_private_config" = salon privé S3, MatchedUserSession champ 3) contient un JWT
// { iss:"gss", gamesync:{gsid,usid,uid,tid,team,attr,ltcy,typ} } : ce sont ces identifiants que
// npl1 lit pour construire Gamesync/IssueToken puis KeepUserSession. mintSessionToken ne les porte
// pas (iss "default iss" + {game_session,user_session}) — S3 ouvrait TCP+TLS+h2 vers :7575 puis
// n'émettait JAMAIS de frame HEADERS, la connexion tombait en EOF et le jeu rendait 2321-4992.
//
// ⚠️ Ce handler est PARTAGÉ entre locataires : Mario Party Jamboree termine aujourd'hui son
// IssueToken avec l'ancienne forme. On ne change donc la forme QUE pour le locataire S3 ; tout
// autre jeu garde exactement ce qui marche pour lui.
func hostSessionToken(tenant, uid, gsName, userSess string, ud *mmpb.UserDefinition) string {
	if !isS3Tenant(tenant) {
		return mintSessionToken(uid, tenant, gsName, userSess)
	}
	return mintGssMatchToken(uid, tenant, gsName, userSess, ud.GetTeam(),
		gamesyncAttrJSON(ud.GetAttributes()), gamesyncLtcyJSON(ud.GetLatencyData()))
}

// s3RoomCapacity : la taille d'un salon S3 depend du MODE demande, et c'est nous qui la fixons
// puisque la requete du jeu ne porte aucune game_session.
//   - coop_private_config    -> 4  (Salmon Run ; c'est exactement ce que porte la capture Nintendo,
//     captured/TrackGameSessionCreationTicket.bin champ 6.2 = 4)
//   - private_match_config   -> 10 (match prive : 8 joueurs + 2 spectateurs)
//
// Annoncer 4 pour un match prive, c'est promettre au jeu un salon plus petit que le mode qu'il a
// demande.
// baseConfigName rend le dernier segment de la configuration demandee
// (« tenants/current/matchmakingConfigs/regular_match_config » -> « regular_match_config »).
// configSansNom : la file des demandes qui n'annoncent AUCUNE configuration.
//
// ⚠️ CETTE LIGNE ETAIT UN TROU DANS L'ISOLATION DES MODES. Une demande sans configuration etait
// versee d'office dans « regular_match_config » — donc dans la file de Guerre de territoire. Un
// salon prive, un quart de Salmon Run ou un mode inconnu qui omettait ce champ se retrouvait
// apparie avec des joueurs de Turf War, sans que rien ne le signale.
//
// Mesure du 2026-08-22 : aucun ticket vide n'est arrive depuis le demarrage du serveur, le trou
// n'a donc jamais ete emprunte. Il reste qu'un mode qui l'emprunterait un jour contaminerait la
// file la plus frequentee du serveur. On lui donne sa propre file : ces demandes ne s'apparient
// qu'entre elles, et le nom se voit dans le monitoring au lieu de se deguiser en Turf War.
const configSansNom = "sans_configuration"

func baseConfigName(cfg string) string {
	if cfg == "" {
		return configSansNom
	}
	return lastSeg(cfg)
}

// nomLisibleDuMode rend le mode d'une configuration sous la forme affichee dans le monitoring.
//
// Les noms sont ceux OBSERVES dans les captures et les journaux, jamais devines :
// « regular_match_config » et « coop_regular_config » (journal du 2026-08-19),
// « bankara_match_challenge_ar_team_config » (ticket capture le 2026-08-14, 14:17:56),
// « private_match_config » et « coop_private_config » (chemin des salons prives).
// Toute configuration inconnue est rendue TELLE QUELLE plutot que rangee de force dans une
// categorie : c'est ainsi qu'un mode encore jamais vu se signale de lui-meme dans le monitoring,
// au lieu de se deguiser en Guerre de territoire.
// cleDuMode rend une cle STABLE pour le mode d'un salon, celle-la meme que /api/rotation emploie
// pour ses creneaux. Le libelle lisible ci-dessous sert au monitoring, en francais ; une page
// publique, elle, doit pouvoir traduire, donc elle recoit la cle et non la phrase.
func cleDuMode(cfg string) string {
	c := lastSeg(cfg)
	switch {
	case strings.Contains(c, "coop_private"):
		return "coop_private"
	case strings.Contains(c, "coop"):
		return "coop"
	case strings.Contains(c, "private_match"):
		return "private"
	case strings.Contains(c, "bankara") && strings.Contains(c, "challenge"):
		return "bankara_challenge"
	case strings.Contains(c, "bankara") && strings.Contains(c, "open"):
		return "bankara_open"
	case strings.Contains(c, "bankara"):
		return "bankara_open"
	// LES TROIS FILES DE FESTIVAL, relevees sur la capture du Splatfest officiel JUEA-00107
	// (2026-08-22, console reelle contre Nintendo) :
	//
	//	fest_match_regular_normal_config        le festimatch ouvert
	//	fest_match_regular_direct_pair_config   « continuer avec cette equipe »
	//	fest_match_challenge_oneshot_config     le festimatch defi
	//
	// Les confondre sous un seul « fest » avait un effet visible : huit joueurs qui rempilent
	// ensemble redemandaient une file ordinaire au lieu d'un appariement direct, et le suivi ne les
	// voyait plus en recherche.
	case strings.Contains(c, "fest") && strings.Contains(c, "tricolor"):
		return "fest_tricolore"
	case strings.Contains(c, "fest") && strings.Contains(c, "direct_pair"):
		return "fest_equipe"
	case strings.Contains(c, "fest") && strings.Contains(c, "challenge"):
		return "fest_defi"
	case strings.Contains(c, "fest"):
		return "fest"
	case strings.HasPrefix(c, "x_match"):
		return "x"
	}
	return "regular"
}

func nomLisibleDuMode(cfg string) string {
	c := lastSeg(cfg)
	switch {
	case strings.Contains(c, "coop_private"):
		return "Salmon Run (prive)"
	case strings.Contains(c, "coop"):
		return "Salmon Run"
	case strings.Contains(c, "private_match"):
		return "Match prive"
	case strings.Contains(c, "bankara") && strings.Contains(c, "challenge"):
		return "Anarchie (serie)"
	case strings.Contains(c, "bankara") && strings.Contains(c, "open"):
		return "Anarchie (ouverte)"
	case strings.Contains(c, "bankara"):
		return "Anarchie"
	// Les deux configurations tricolores, relevees sur capture-tricolore-20260822 :
	// fest_match_tricolor_team_config et fest_match_tricolor_oneshot_config. Elles contiennent
	// « fest » mais ni « challenge » ni « direct_pair » : sans ce cas, elles s'affichaient
	// « Festival (ouvert) », ce qui rendait un tricolore indiscernable d'un festimatch ordinaire.
	case strings.Contains(c, "fest") && strings.Contains(c, "tricolor") && strings.Contains(c, "team"):
		return "Tricolore (equipe)"
	case strings.Contains(c, "fest") && strings.Contains(c, "tricolor"):
		return "Tricolore"
	case strings.Contains(c, "fest") && strings.Contains(c, "direct_pair"):
		return "Festival (meme equipe)"
	case strings.Contains(c, "fest") && strings.Contains(c, "challenge"):
		return "Festival (defi)"
	case strings.Contains(c, "fest"):
		return "Festival (ouvert)"
	case strings.HasPrefix(c, "x_match"):
		return "Match X"
	case strings.Contains(c, "regular"):
		return "Guerre de territoire"
	case c == "":
		return "Guerre de territoire"
	}
	return c
}

// dureeCacheIce : combien de temps le jeu garde le jeu de serveurs ICE avant d'en redemander un.
// Reglable par « icecache=<secondes> ».
func dureeCacheIce() time.Duration {
	if v := soirFlagValeur("icecache"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}

	return 90 * time.Second
}

func s3RoomCapacity(cfg string, fallback int32) int32 {
	switch {
	case strings.Contains(cfg, "coop"):
		return 4
	case strings.Contains(cfg, "private_match"):
		return 10
	case strings.Contains(cfg, "regular"), strings.Contains(cfg, "bankara"),
		strings.Contains(cfg, "league"), strings.Contains(cfg, "x_match"),
		strings.Contains(cfg, "fest"):
		// ⚠️ LES QUATRE AUTRES MODES VS SONT AUSSI A HUIT, et il a fallu les nommer.
		//
		// Seuls « regular » et « bankara » etaient reconnus. La ligue, le mode X et le festimatch
		// DEFI ne contiennent aucun des deux : ils retombaient sur la valeur de repli, c'est-a-dire
		// NPLN_MATCH_SIZE, soit deux joueurs. Une serie de ligue ou un festimatch defi se serait
		// forme a deux au lieu de huit.
		//
		// Le festimatch OUVERT passait par hasard : « fest_match_regular_normal_config » contient
		// « regular ». Le defi, « fest_match_challenge_oneshot_config », ne contient rien.
		// Capture Nintendo, match public : max_participant_count = 8 ET
		// current_participant_count = 8. La session est annoncee PLEINE des le cadre SUCCEEDED,
		// alors que la console n'est qu'un joueur parmi huit — c'est une capacite de configuration,
		// pas un effectif reel.
		return 8
	}
	return fallback
}

// s3SessionProperties reproduit le champ 11 de la capture : Nintendo CONSERVE les propriétés
// envoyées par le client et AJOUTE les deux siennes par-dessus.
//
//	requête du jeu  (captured/CreateGameSessionCreationTicket.bin 6.11) : {userName, game_mode}
//	réponse Nintendo (captured/TrackGameSessionCreationTicket.bin 6.11) : {_BaseConfigName,
//	                                                       _AliasSuffix, userName, game_mode}
//
// Nous remplacions toute la map par nos deux clés : `game_mode` — le mode du match privé — et
// `userName` disparaissaient de la session rendue au jeu. C'est cohérent avec son rapport
// (ApiType CreateSession, CommonError CreateSessionFailed, ServerSession.Id VIDE) : il ne
// retrouvait pas dans la session le mode qu'il venait de demander.
func s3SessionProperties(cfg string, req *commonpb.MapValue) *commonpb.MapValue {
	out := &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
	for k, v := range req.GetFields() {
		if k == "_BaseConfigName" || k == "_AliasSuffix" {
			continue // ces deux-là appartiennent au serveur
		}
		out.Fields[k] = v
	}
	out.Fields["_BaseConfigName"] = vStr(lastSeg(cfg))
	out.Fields["_AliasSuffix"] = vStr("")
	return out
}

// s3PendingGameSession : Nintendo renvoie DÉJÀ une GameSession à l'état PENDING (capture
// CreateGameSessionCreationTicket.bin champ 6, 43 octets : uniquement les properties recopiées de
// la requête). Nous n'envoyions aucun champ 6 en PENDING. nil hors S3 : MP Jamboree inchangé.
func s3PendingGameSession(tenant string, reqGs *mmpb.GameSession) *mmpb.GameSession {
	if !isS3Tenant(tenant) || reqGs.GetProperties() == nil {
		return nil
	}
	return &mmpb.GameSession{Properties: reqGs.GetProperties()}
}

// s3ConfigEcho renvoie la matchmakingConfig avec le locataire CONCRET : le jeu l'envoie sous
// l'alias "tenants/current/...", Nintendo la lui rend résolue ("tenants/t-dce9377b-lp1/...").
func s3ConfigEcho(tenant, cfg string) string {
	if !isS3Tenant(tenant) || cfg == "" {
		return cfg
	}
	return tenant + "/matchmakingConfigs/" + lastSeg(cfg)
}

func (g *gameSessionServer) TrackGameSessionCreationTicket(req *mmpb.TrackGameSessionCreationTicketRequest, stream grpc.ServerStreamingServer[mmpb.GameSessionCreationTicket]) error {
	name := req.GetName()
	g.mu.Lock()
	t := g.tickets[lastSeg(name)]
	g.mu.Unlock()
	if t == nil {
		log.Printf("[NPLN MM] TrackGameSessionCreationTicket %s -> unknown (FAILED)", name)
		return stream.Send(&mmpb.GameSessionCreationTicket{Name: name, State: mmpb.GameSessionCreationTicket_FAILED})
	}
	// [Nextendo] Combien de messages ce flux doit-il porter ? La capture tranche : les fichiers
	// captured/Track*.bin conservent le flux ENTIER (TrackMatchmakingTicket.bin en contient 3,
	// tailles 4052/4054/11673, les suivants precedes de leur cadre gRPC), et
	// TrackGameSessionCreationTicket.bin, lui, consomme ses 1954 octets en UN SEUL message, deja a
	// l'etat SUCCEEDED (champ 4 = 2). Nintendo n'envoie donc PAS de PENDING intermediaire pour la
	// creation d'un salon. Nous en envoyions un, suivi de 800 ms d'attente : le client voyait une
	// creation « en cours » la ou le vrai serveur lui annonce une creation FAITE.
	s3One := isS3Tenant(tenantFromCtx(stream.Context()))
	log.Printf("[NPLN MM] TrackGameSessionCreationTicket %s -> %s session=%s (le flux se ferme apres le dernier message)",
		name, map[bool]string{true: "SUCCEEDED seul (forme capturee)", false: "PENDING puis SUCCEEDED"}[s3One],
		t.GetGameSession().GetName())
	// Stream the real creation lifecycle: PENDING ("creating") first, then SUCCEEDED with the
	// room. The host's state machine expects to observe this transition; a first message already
	// at SUCCEEDED left it stuck (and it eventually cancelled the ticket).
	pending := &mmpb.GameSessionCreationTicket{
		Name:              t.GetName(),
		MatchmakingConfig: t.GetMatchmakingConfig(),
		UserDefinitions:   t.GetUserDefinitions(),
		State:             mmpb.GameSessionCreationTicket_PENDING,
	}
	if !s3One {
		if err := stream.Send(pending); err != nil {
			return err
		}
		time.Sleep(800 * time.Millisecond)
	}
	// Send the terminal SUCCEEDED and CLOSE the stream (return). The client's NPLN DetailModule
	// treats the creation ticket as COMPLETE only when the Track stream closes with OK — only then
	// does it advance the session state to "connected" (IsSessionConnected true), which lets the
	// create-room fiber leave its wait loop and reach the (client-patched) station-count gate.
	// Holding the stream open kept the session stuck at "creating" forever.

	// [Nextendo] Serve Nintendo's own SUCCEEDED payload. Ours is a few hundred bytes; the captured
	// one is 1954 and carries what we never reproduced: the MatchedUserSession block (field 5) with
	// the host's userSession and its signed `gss` token, plus a fully populated GameSession (field
	// 6). Since the check_peer patch the client now reaches the gRPC layer on the session server but
	// opens no stream at all — consistent with a session description it cannot act on. Same move
	// that fixed InitializeAttributes: replay the real bytes, realigned onto this session.
	// [Nextendo] DESACTIVE PAR DEFAUT apres mesure. Rejouer la reponse de Nintendo est SEDUISANT mais
	// contre-productif ici : elle embarque un jeton `gss` signe par LEUR cle et pointant vers LEURS
	// identifiants de session (gsid 1559755f-..., usid 34c551db-...), c'est-a-dire une session qui
	// n'existe sur aucun de nos serveurs. Mesure : avant le rejeu, le serveur de session logait
	// "S3 reached the gRPC layer" ; avec le rejeu, il ne recoit plus AUCUNE connexion. Notre reponse
	// generee est plus pauvre mais elle est COHERENTE (jeton que nous signons, session que nous
	// connaissons). NPLN_GSCT_REPLAY=1 pour re-essayer le rejeu.
	// [Nextendo 2026-08-12] Le rejeu redevient le comportement par defaut pour S3, parce que la
	// mesure qui l'avait ecarte portait sur un chemin CASSE : l'adresse de session n'etait pas
	// vraiment redirigee (port laisse a 7090 alors que notre transport ecoute sur 7575, et hote
	// complete par des espaces pour conserver la longueur du message — le jeu composait
	// « 203.0.113.7 »). « Le jeu ne se connecte pas au serveur de session » ne disait donc rien du
	// rejeu lui-meme : il n'y avait aucune adresse valide a composer. La redirection protobuf
	// corrige les deux, et le test session_address_rewrite_test.go le verrouille sur la capture.
	// NPLN_GSCT_REPLAY=0 revient a notre reponse generee.
	if soirFlag("gsct") && isS3Tenant(tenantFromCtx(stream.Context())) && len(rawTrackGameSessionCreationTicket) > 0 {
		resp := alignCapturedIdentity(rawTrackGameSessionCreationTicket)

		// Point it at the ticket the client is tracking (uuid4, same length either way).
		if capName, ok := pbWalk(resp)[1]; ok && len(capName) == len(t.GetName()) {
			resp = bytes.ReplaceAll(resp, capName, []byte(t.GetName()))
		}

		// La capture pointe la salle vers l'hote Agones de Nintendo (35.223.226.237:7090). On la
		// redirige vers NOTRE transport de session — hote ET port, au niveau protobuf.
		//
		// Avant : un simple remplacement d'octets sur l'IP, qui laissait le port a 7090 (le notre
		// ecoute sur 7575) et completait l'hote avec des espaces pour garder la longueur du message,
		// si bien que le jeu composait « 203.0.113.7 ». Ce rejeu ne pouvait pas aboutir : le test
		// qu'il devait trancher n'avait donc jamais reellement eu lieu.
		hote, portSession, _, _, _, _, _, _, _ := mmConfig()
		if reecrit, err := reecrireAdresseSession(resp, hote, uint32(portSession)); err != nil {
			log.Printf("[NPLN MM] rejeu : reecriture de l'adresse impossible (%v) — envoi non redirige", err)
		} else {
			resp = reecrit
		}

		// Rediriger l'adresse ne suffit pas : la capture decrit LA session de Nintendo (gsid
		// 1559755f-…, usid 34c551db-…) et porte un jeton signe par LEUR cle. Le client composerait
		// donc notre serveur en presentant des identifiants qu'il ne connait pas. On greffe : on garde
		// la structure riche de la capture — c'est tout son interet, elle contient les champs que
		// notre reponse generee n'a jamais reproduits — et on y substitue NOS identifiants, ceux que
		// notre propre transport de session attend.
		gsName := t.GetGameSession().GetName()
		var userSess, jeton string
		if mus := t.GetMatchedUserSessions(); len(mus) > 0 {
			userSess = mus[0].GetUserSession()
			jeton = mus[0].GetMatchmakingIdToken()
		}
		greffes := []struct {
			quoi   string
			chemin []protowire.Number
			val    string
		}{
			{"gameSession", []protowire.Number{6, 1}, gsName},
			{"userSession", []protowire.Number{5, 2}, userSess},
			{"jeton", []protowire.Number{5, 3}, jeton},
			{"matchmakingConfig", []protowire.Number{2}, t.GetMatchmakingConfig()},
		}
		for _, g := range greffes {
			if g.val == "" {
				log.Printf("[NPLN MM] rejeu : %s absent de notre ticket, la capture garde le sien", g.quoi)
				continue
			}
			reecrit, err := reecrireChaine(resp, g.chemin, g.val)
			if err != nil {
				log.Printf("[NPLN MM] rejeu : %s non greffe (%v)", g.quoi, err)
				continue
			}
			resp = reecrit
		}
		log.Printf("[NPLN MM] rejeu greffe : session=%s adresse=%s:%d userSession=%s jeton=%do",
			gsName, hote, portSession, lastSeg(userSess), len(jeton))

		log.Printf("[NPLN MM] TrackGameSessionCreationTicket %s -> rejeu capture Nintendo (%d o)", name, len(resp))

		return stream.SendMsg(&rawMsg{b: resp})
	}

	if err := stream.Send(t); err != nil {
		return err
	}

	// [Nextendo] Garder le flux OUVERT apres le SUCCEEDED, pour S3.
	//
	// Cette variante n'avait JAMAIS ete testee : le commentaire au-dessus affirmait que le flux
	// restait ouvert (« KEEP THE STREAM OPEN as the room's liveness channel ») alors que le code
	// rendait la main juste apres l'envoi, ce qui le FERME. Les deux comportements reellement
	// essayes sont donc « PENDING puis SUCCEEDED, ferme » et « SUCCEEDED seul, ferme ».
	//
	// Pourquoi ca peut compter : terminer un flux server-streaming, c'est dire au client que le
	// suivi du ticket est FINI. S'il traite cette fin comme la cloture du cycle de creation, il
	// n'a plus de raison d'emettre Gamesync/IssueToken sur le canal de session — ce qui colle a la
	// mesure (canal ouvert, preface envoyee, aucun appel, fermeture par le jeu au bout d'~1 s).
	// MESURE 2026-08-11 : garder le flux ouvert est FAUX. Avec la variante « maintenu ouvert », le
	// jeu ne compose meme plus le serveur de session — il attend 20 s puis appelle
	// CancelGameSessionCreationTicket. C'est donc la FERMETURE du flux qui lui dit « creation
	// terminee, va te connecter » : fermee, il compose :7575 dans la seconde. On ferme.
	return nil
}

// CancelGameSessionCreationTicket : l'hote renonce a son salon prive.
//
// ⚠️ Ce chemin ne supprimait QUE le ticket. La partie restait dans g.sessions, l'hote restait
// inscrit parmi ses participants, et le code de salon restait enregistre. Chaque salon ouvert puis
// quitte laissait donc une partie fantome avec son createur dedans — visible dans le monitoring
// comme un salon a « 1/8 » alors que le joueur cherchait deja autre chose — et a la tentative
// suivante il se retrouvait a la fois dans l'ancien salon et le nouveau.
//
// Le chemin du MATCHMAKING avait deja recu ce traitement en aout, apres deux mesures : retirer le
// joueur de la file ne suffit pas, il faut aussi conclure son flux. Celui-ci ne l'avait jamais reçu.
func (g *gameSessionServer) CancelGameSessionCreationTicket(ctx context.Context, req *mmpb.CancelGameSessionCreationTicketRequest) (*emptypb.Empty, error) {
	id := lastSeg(req.GetName())

	g.mu.Lock()
	t := g.tickets[id]
	delete(g.tickets, id)

	// La partie que ce ticket avait creee, et tout ce qui la designe.
	gsName := t.GetGameSession().GetName()
	gsid := lastSeg(gsName)
	if gsid != "" {
		delete(g.sessions, gsid)
		for code, nom := range g.aliases {
			if lastSeg(nom) == gsid {
				delete(g.aliases, code)
			}
		}
	}
	g.mu.Unlock()

	if gsid != "" {
		// Sortir l'hote de la fiche que GetGameSession rend aux suivants, sans quoi un salon
		// abandonne continue d'annoncer un participant qui n'y est plus.
		participantsParGsid.Lock()
		delete(participantsParGsid.m, gsid)
		participantsParGsid.Unlock()
		dashSupprimerSalon(gsName)
	}

	log.Printf("[NPLN MM] CancelGameSessionCreationTicket %s -> salon %s libere (session, alias et participants retires)",
		req.GetName(), gsid)
	return &emptypb.Empty{}, nil
}

func (g *gameSessionServer) CreateGameSessionShortAlias(ctx context.Context, req *mmpb.CreateGameSessionShortAliasRequest) (*mmpb.GameSessionShortAlias, error) {
	sessName := ""
	if in := req.GetGameSessionShortAlias(); in != nil {
		sessName = in.GetGameSession()
	}
	// The client does NOT propose a code (request carries only game_session); the server assigns it.
	// Deterministic from the session name so the __gs/s gamesync doc (served by the :7575 process)
	// derives the SAME code the game reads into [0xf70] for the Room Info display.
	code := roomCodeFor(sessName)
	g.mu.Lock()
	g.aliases[code] = sessName
	// Persist the join code on the session so GetGameSession hands it back for the on-screen
	// "Room ID" (the game reads properties["_AliasSuffix"]).
	if sess := g.sessions[lastSeg(sessName)]; sess != nil {
		if sess.Properties == nil {
			sess.Properties = &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
		}
		if sess.Properties.Fields == nil {
			sess.Properties.Fields = map[string]*commonpb.Value{}
		}
		sess.Properties.Fields["_AliasSuffix"] = vStr(code)
	}
	g.mu.Unlock()
	alias := &mmpb.GameSessionShortAlias{
		Name:        tenantFromCtx(ctx) + "/gameSessionShortAliases/" + code,
		GameSession: sessName,
		ExpireTime:  timestamppb.New(time.Now().Add(1 * time.Hour)),
	}
	dashNoteRoomCode(sessName, code)
	log.Printf("[NPLN MM] CreateGameSessionShortAlias code=%s -> session=%s", code, sessName)
	return alias, nil
}

// GetGameSession returns a live room by name. The host calls this right after creating the room and
// its short alias to fill in the room screen (participant count, Room ID); returning Unimplemented
// left that screen blank. We hand back the stored session, which now carries the join code in
// properties["_AliasSuffix"].
func (g *gameSessionServer) GetGameSession(ctx context.Context, req *mmpb.GetGameSessionRequest) (*mmpb.GameSession, error) {
	id := lastSeg(req.GetName())
	g.mu.Lock()
	sess := g.sessions[id]
	g.mu.Unlock()
	if sess == nil {
		log.Printf("[NPLN MM] GetGameSession %s -> NotFound", req.GetName())
		return nil, status.Errorf(codes.NotFound, "game session %q not found", req.GetName())
	}
	// Champ 12 : les UserSession de la partie. Nintendo les porte ICI (et pas dans le ticket) ;
	// c'est ce que le jeu lit pour peupler l'ecran de jonction avant d'entrer.
	rendu := proto.Clone(sess).(*mmpb.GameSession)
	if parts := participantsDe(id); len(parts) > 0 {
		rendu.UserSessions = parts
	}
	log.Printf("[NPLN MM] GetGameSession %s -> ACTIVE participants=%d/%d code=%s user_sessions=%d",
		req.GetName(), rendu.GetCurrentParticipantCount(), rendu.GetMaxParticipantCount(),
		rendu.GetProperties().GetFields()["_AliasSuffix"].GetStringValue(), len(rendu.GetUserSessions()))
	return rendu, nil
}

// participantsParGsid tient les UserSession d'une partie POUR LA SEULE reponse GetGameSession.
//
// Mesure du 2026-08-15, deux captures du meme salon prive Nintendo :
//   - CreateGameSessionCreationTicket rend une GameSession SANS user_sessions (champ 12 absent) ;
//   - GetGameSession rend la MEME partie AVEC son champ 12 rempli — nom, utilisateur et latences de
//     chaque participant.
//
// Deux messages, deux contenus. On garde donc le ticket vide comme mesure, et on ne remplit le
// champ 12 qu'ici : c'est lui que le jeu lit pour afficher « 1/10 » et le pseudo de l'hote sur
// l'ecran de jonction, avant meme d'entrer.
var participantsParGsid = struct {
	sync.Mutex
	m map[string][]*mmpb.UserSession
}{m: map[string][]*mmpb.UserSession{}}

// noterParticipant enregistre un joueur dans la partie, EN REMPLACANT sa fiche precedente.
//
// ⚠️ On dedoublonne par UTILISATEUR, pas par session. Chaque jonction cree un identifiant de session
// neuf : en dedoublonnant sur lui, un joueur qui reessaye apres une erreur s'ajoutait une seconde
// fois. Mesure du 2026-08-15 : apres quelques tentatives le salon annoncait 4 participants pour
// DEUX joueurs reels, et le jeu attendait des absents.
func noterParticipant(gsid string, us *mmpb.UserSession) {
	if gsid == "" || us == nil {
		return
	}
	participantsParGsid.Lock()
	defer participantsParGsid.Unlock()
	liste := participantsParGsid.m[gsid]
	for i, d := range liste {
		if d.GetUser() == us.GetUser() && us.GetUser() != "" {
			liste[i] = us // meme joueur, nouvelle session : on remplace
			participantsParGsid.m[gsid] = liste
			return
		}
	}
	participantsParGsid.m[gsid] = append(liste, us)
}

// oublierParticipant retire un joueur d'une partie (depart, ou salon referme).
func oublierParticipant(gsid, user string) {
	if gsid == "" || user == "" {
		return
	}
	participantsParGsid.Lock()
	defer participantsParGsid.Unlock()
	liste := participantsParGsid.m[gsid]
	garde := liste[:0]
	for _, d := range liste {
		if d.GetUser() != user {
			garde = append(garde, d)
		}
	}
	if len(garde) == 0 {
		delete(participantsParGsid.m, gsid)
		return
	}
	participantsParGsid.m[gsid] = garde
}

func participantsDe(gsid string) []*mmpb.UserSession {
	participantsParGsid.Lock()
	defer participantsParGsid.Unlock()
	return append([]*mmpb.UserSession(nil), participantsParGsid.m[gsid]...)
}

// GetGameSessionShortAlias resout le CODE affiche par l'hote vers sa partie.
//
// C'est le premier des deux appels de l'invite. Nous frappions le code (CreateGameSessionShortAlias)
// sans jamais savoir le relire : le service declare pourtant les deux. Un ami qui saisissait le code
// tombait donc sur le gestionnaire de rejeu, qui lui rendait une partie capturee — jamais celle de
// l'hote. Sans cet appel, un salon prive ne peut accueillir personne, quelle que soit la qualite du
// reste.
func (g *gameSessionServer) GetGameSessionShortAlias(ctx context.Context, req *mmpb.GetGameSessionShortAliasRequest) (*mmpb.GameSessionShortAlias, error) {
	code := lastSeg(req.GetName())
	g.mu.Lock()
	sessName := g.aliases[code]
	g.mu.Unlock()
	if sessName == "" {
		log.Printf("[NPLN MM] GetGameSessionShortAlias %q -> NotFound (aucun salon sous ce code)", code)
		return nil, status.Errorf(codes.NotFound, "game session short alias %q not found", code)
	}
	log.Printf("[NPLN MM] GetGameSessionShortAlias %q -> %s", code, sessName)
	return &mmpb.GameSessionShortAlias{
		Name:        tenantFromCtx(ctx) + "/gameSessionShortAliases/" + code,
		GameSession: sessName,
		ExpireTime:  timestamppb.New(time.Now().Add(1 * time.Hour)),
	}, nil
}

// QueryGameSessions est le navigateur de salons des applications Nintendo Classics (tenant
// t-7b4e32ca-lp1 : N64, GBA, Game Boy, NES, Genesis). Sans elle le service rendait Unimplemented,
// que le jeu affiche en 2321-4224.
func (g *gameSessionServer) QueryGameSessions(ctx context.Context, req *mmpb.QueryGameSessionsRequest) (*mmpb.QueryGameSessionsResponse, error) {
	log.Printf("[NPLN MM] QueryGameSessions tenant=%q view=%v config=%q minVacancy=%d users=%d pageSize=%d -> 0 salon(s)",
		req.GetTenant(), req.GetView(), req.GetGameSessionSearchConfig(),
		req.GetMinVacancyCount(), len(req.GetUsers()), req.GetPageSize())
	return &mmpb.QueryGameSessionsResponse{}, nil
}

// JoinGameSession fait ENTRER l'invite dans la partie de l'hote.
//
// Second appel de l'invite, et le seul qui le rende visible des autres. La reponse a la meme forme
// que le ticket de match reussi — matched_user_sessions + game_session — parce que le client en fait
// exactement le meme usage : il y lit l'hote de session et son jeton, puis ouvre Gamesync/IssueToken
// vers :7575. On lui frappe donc le MEME jeton que sur le chemin du matchmaking (mintGssMatchToken),
// faute de quoi il ouvrirait TCP+TLS+h2 sans jamais emettre de HEADERS, comme cela s'est produit sur
// le chemin du salon prive avant que ce jeton n'y soit corrige.
//
// Le mot de passe est verifie ici : c'est le seul endroit ou le serveur peut le faire.
func (g *gameSessionServer) JoinGameSession(ctx context.Context, req *mmpb.JoinGameSessionRequest) (*mmpb.JoinGameSessionResponse, error) {
	gsName := req.GetName()
	id := lastSeg(gsName)

	g.mu.Lock()
	sess := g.sessions[id]
	g.mu.Unlock()
	if sess == nil {
		log.Printf("[NPLN MM] JoinGameSession %s -> NotFound", gsName)
		return nil, status.Errorf(codes.NotFound, "game session %q not found", gsName)
	}

	if mdp, _, connu := roomSettingsFor(id); connu && mdp != "" && req.GetPassword() != mdp {
		log.Printf("[NPLN MM] JoinGameSession %s -> mot de passe refuse", gsName)
		return nil, status.Error(codes.PermissionDenied, "wrong password")
	}

	// RESOUDRE l'utilisateur. Mesure du 2026-08-15, jonction reelle sur un salon prive Nintendo :
	// la console envoie « tenants/current/users/current », et la reponse rend le chemin CONCRET
	// (« tenants/t-dce9377b-lp1/users/u-exemple6000000000000 »). Renvoyer l'alias tel quel laisse le
	// client sans moyen de se reconnaitre parmi les participants.
	demande := req.GetUserDefinitions()
	uid := uidFromCtx(ctx)
	if uid == "" && len(demande) > 0 {
		uid = userIDFromPath(demande[0].GetUser())
	}
	ud := &mmpb.UserDefinition{User: npnTenant + "/users/" + uid}
	if len(demande) > 0 {
		ud.Attributes = demande[0].GetAttributes()
		ud.LatencyData = demande[0].GetLatencyData()
		ud.Team = demande[0].GetTeam()
	}

	userSess := gsName + "/userSessions/" + uuid4()
	team := ud.GetTeam()
	rememberParticipantAttrs(lastSeg(userSess), ud.GetAttributes(), ud.GetLatencyData().GetLatencies(), team)

	// L'arrivant rejoint le champ 12 que GetGameSession rendra aux suivants.
	noterParticipant(id, &mmpb.UserSession{
		Name:       userSess,
		User:       ud.GetUser(),
		State:      mmpb.UserSession_ACTIVE,
		Attributes: ud.GetAttributes(),
		CreateTime: timestamppb.Now(),
	})

	// L'effectif annonce DECOULE de la liste reelle des participants — il ne s'incremente pas.
	//
	// Incremente a chaque jonction, il ne redescendait jamais : un joueur qui reessayait apres une
	// erreur faisait monter le compteur, et le salon annoncait quatre presents pour deux joueurs.
	g.mu.Lock()
	if n := int32(len(participantsDe(id))); n > 0 {
		sess.CurrentParticipantCount = n
	}
	g.mu.Unlock()

	// Tenir le monitoring au courant de l'ARRIVEE.
	//
	// Le tableau de bord n'etait notifie qu'a la CREATION du salon : un invite qui rejoignait
	// n'apparaissait nulle part. Mesure du 2026-08-16 : deux joueurs dans le salon a l'ecran, un
	// seul affiche cote monitoring, et l'arrivant reste marque « en ligne » au lieu de « en partie ».
	// On republie donc la composition complete a chaque jonction.
	var membres []string
	for _, p := range participantsDe(id) {
		if u := userIDFromPath(p.GetUser()); u != "" {
			membres = append(membres, u)
		}
	}
	hote := ""
	if parts := participantsDe(id); len(parts) > 0 {
		hote = userIDFromPath(parts[0].GetUser())
	}
	dashNoteRoom(gsName, "Salon", uint16(sess.GetMaxParticipantCount()), hote, 0, membres)

	log.Printf("[NPLN MM] JoinGameSession %s <- uid=%s userSession=%s (%d/%d, %d membre(s) publie(s) au monitoring)",
		gsName, uid, lastSeg(userSess), sess.GetCurrentParticipantCount(), sess.GetMaxParticipantCount(), len(membres))

	return &mmpb.JoinGameSessionResponse{
		GameSession: sess,
		MatchedUserSessions: []*mmpb.MatchedUserSession{{
			UserDefinition: ud,
			UserSession:    userSess,
			MatchmakingIdToken: mintGssMatchToken(
				uid, npnTenant, gsName, userSess, team,
				gamesyncAttrJSON(ud.GetAttributes()), gamesyncLtcyJSON(ud.GetLatencyData())),
		}},
	}, nil
}

// =====================================================================
// Matchmaker — CreateMatchmakingTicket + TrackMatchmakingTicket
// =====================================================================

type matchmakerServer struct {
	mmpb.UnimplementedMatchmakerServer

	mu      sync.Mutex
	tickets map[string]*mmpb.MatchmakingTicket // ticket name -> ticket

	// [Nextendo] Real pooling. Replaying a captured 8-player session got S3 past "this session is
	// unusable" (2321-3072 -> 2321-0384) but can never produce a playable match: the opponents in
	// a capture are ghosts that answer no packet, and the local player has no entry of their own in
	// it. To actually play with someone, the matched session has to be built from the players who
	// are genuinely searching right now -- each with their own user definition, their own
	// userSession and their own signed match token, all pointing at ONE shared GameSession.
	waiting []*mmWaiter
}

// mmWaiter is one client parked in TrackMatchmakingTicket, waiting for enough players to show up.
type mmWaiter struct {
	ticket *mmpb.MatchmakingTicket
	uid    string                       // owning player; two tickets from the SAME player never match
	out    chan *mmpb.MatchmakingTicket // fed once the match is formed
	// depuis : quand ce joueur a commence a patienter. C'est l'attente du PLUS ANCIEN de la file
	// qui assouplit le seuil de formation — voir matchmaking_seuil_attente.go.
	depuis time.Time
}

// distinctWaitersLocked returns at most one waiter per player. S3 opens a matchmaking ticket during
// BOOT and another when you actually search from the lobby, so a naive count reaches 2 with a single
// player online and matches them against themselves -- which is what stopped the game connecting at
// launch. Caller holds m.mu.
func (m *matchmakerServer) distinctWaitersLocked() []*mmWaiter {
	seen := map[string]bool{}
	out := make([]*mmWaiter, 0, len(m.waiting))

	for _, w := range m.waiting {
		if w.uid != "" && seen[w.uid] {
			continue
		}
		seen[w.uid] = true
		out = append(out, w)
	}

	return out
}

// waitersForConfigLocked rend les joueurs distincts qui attendent une partie de CE mode.
//
// Mesure du 2026-08-19 : la file etait UNIQUE et n'avait aucun filtre. Un joueur parti en Salmon Run
// etait donc compte parmi les huit d'une Guerre de territoire, et formMatchLocked etiquetait toute la
// session avec la configuration du PREMIER de la liste — les sept autres recevaient une partie dans un
// mode qu'ils n'avaient pas demande. Pire, un chercheur Salmon Run ne trouve jamais ses quatre
// coequipiers quand il est seul : son ticket reste dans la file et empoisonne CHAQUE salon forme
// pendant ce temps. Les capacites sont en plus incompatibles — 8 en Turf et en Anarchie, 4 en Salmon
// Run — donc un seul groupe ne peut pas servir les deux.
func (m *matchmakerServer) waitersForConfigLocked(cfg string) []*mmWaiter {
	seen := map[string]bool{}
	out := make([]*mmWaiter, 0, len(m.waiting))

	for _, w := range m.waiting {
		if w.ticket == nil || baseConfigName(w.ticket.GetMatchmakingConfig()) != cfg {
			continue
		}
		if w.uid != "" && seen[w.uid] {
			continue
		}
		seen[w.uid] = true
		out = append(out, w)
	}

	return out
}

// configsEnAttenteLocked rend les modes distincts presents dans la file, dans l'ordre d'arrivee.
func (m *matchmakerServer) configsEnAttenteLocked() []string {
	seen := map[string]bool{}
	out := make([]string, 0, 4)

	for _, w := range m.waiting {
		if w.ticket == nil {
			continue
		}
		cfg := baseConfigName(w.ticket.GetMatchmakingConfig())
		if seen[cfg] {
			continue
		}
		seen[cfg] = true
		out = append(out, cfg)
	}

	return out
}

// seuilPourConfig rend l'effectif a reunir pour CE mode : la capacite de la configuration, et non un
// nombre unique pour tout le serveur. Un drapeau a chaud pose explicitement reste prioritaire pour les
// tests a effectif reduit.
func seuilPourConfig(cfg string, defaut int32) int32 {
	// Seuil PAR MODE, pose a chaud : « mmtaille_coop_regular_config=1 » permet d'eprouver la chaine
	// Salmon Run a un seul joueur sans toucher au Turf, qui doit rester a huit pour les autres.
	// Un seuil global baisse pour tester un mode cassait tous les autres en meme temps.
	if v := soirFlagValeur("mmtaille_" + lastSeg(cfg)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return int32(n)
		}
	}
	if soirFlag("mmsolo") {
		return 1
	}
	if v := soirFlagValeur("mmtaille"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return int32(n)
		}
	}
	return s3RoomCapacity(cfg, defaut)
}

func newMatchmaker() *matchmakerServer {
	return &matchmakerServer{tickets: map[string]*mmpb.MatchmakingTicket{}}
}

// formMatchLocked builds one GameSession shared by every waiting ticket and hands each waiter its
// own finalized ticket. Caller holds m.mu.
func (m *matchmakerServer) formMatchLocked(cfg string, relayHost string, relayPort int32) {
	players := m.waitersForConfigLocked(cfg)

	// Ne jamais verser plus de joueurs que la configuration n'en accepte : une session annoncee pour
	// huit qui en recoit douze decrit une partie que le jeu ne peut pas lancer. Le surplus reste en
	// file et formera le salon suivant.
	seuilConfig := seuilPourConfig(cfg, int32(len(players)))

	// ⚠️ SUR UNE FILE DE FETE, REPARTIR AVANT DE COUPER.
	//
	// La coupe ci-dessous ne garde que les premiers arrives. En la faisant D'ABORD, repartirParCamp
	// ne voyait que ces huit-la : mesure du 25/08, « Alpha=7, Bravo=1 » alors que DIX-HUIT joueurs
	// patientaient, dont largement quatre de chaque camp. Aucun festimatch ne se formait et le
	// journal repetait « on attend » indefiniment, file pleine.
	//
	// On choisit donc dans la file ENTIERE, puis on coupe.

	// [Nextendo 2026-08-25] SUR UNE FILE DE FETE, LES CAMPS S'AFFRONTENT.
	//
	// Sans cela les deux equipes melangeaient les camps, et un festival dont les joueurs d'un meme
	// camp sont des deux cotes ne veut plus rien dire : on ne peut pas lui compter ses victoires.
	// La console declare son camp dans les attributs de son ticket (« fest_team »), et c'est le
	// serveur qui distribue les creneaux team1 / team2 — mesure sur la capture du Splatfest officiel
	// JUEA-00107 du 2026-08-22. Voir fest_equipes.go.
	//
	// Si deux camps ne peuvent pas fournir un effectif equilibre, repartirParCamp rend nil et la
	// file reprend son comportement ordinaire : mieux vaut un match melange qu'aucun match.
	creneauDeFete := map[*mmWaiter]string{}
	if estFileDeFete(cfg) {
		seuilDetendu, _ := m.seuilCourantLocked(cfg, seuilConfig)
		choisis, creneau := repartirParCamp(players, int(seuilConfig), int(seuilDetendu), cfg, identifiantDeFeteMaison(time.Now()))
		if choisis == nil {
			// ⚠️ NE PAS FORMER. Rendre la main ici formait le salon avec la liste D'ORIGINE, donc
			// avec les camps melanges : mesure du 25/08, un festimatch a quatre Alpha, trois Bravo
			// et un Charlie — trois camps dans un salon, c'est la disposition du tricolore, pas
			// celle d'un festimatch. Le commentaire de repartirParCamp disait « on attend » alors
			// que rien n'attendait.
			//
			// Un festimatch oppose DEUX camps, quatre contre quatre. Si la file ne peut pas le
			// fournir, les joueurs restent en attente et le salon suivant reessaiera : mieux vaut
			// patienter qu'un festival dont les camps jouent des deux cotes, qui ne mesure plus rien.
			return
		}
		players, creneauDeFete = choisis, creneau
	}

	if int32(len(players)) > seuilConfig {
		players = players[:seuilConfig]
	}
	// L'effectif avec lequel la partie se forme. Il ne bougera plus : cette fonction cree TOUJOURS
	// une session neuve et n'ajoute jamais personne a une session existante, donc un salon ne peut
	// que se vider. Le monitoring s'en sert pour savoir quand il est mort.
	requis := len(players)
	if len(players) == 0 {
		return
	}

	gsName := npnTenant + "/gameSessions/" + uuid4()
	participants := int32(len(players))

	// ⚠️ La CAPACITE de la configuration demandee, pas l'effectif present. Mesure sur la capture
	// Nintendo (match public) : max_participant_count = 8 ET current_participant_count = 8, alors
	// que la console est seule a ce moment-la. Nous annoncions le nombre de joueurs reellement
	// apparies — « session partagee, 1 participant » — ce qui decrit une partie que le jeu ne
	// reconnait pas comme une Guerre de territoire.
	cfgDemandee := ""
	if len(players) > 0 && players[0].ticket != nil {
		cfgDemandee = players[0].ticket.GetMatchmakingConfig()
	}
	capacite := s3RoomCapacity(cfgDemandee, participants)

	// Faire connaitre la configuration au serveur gamesync : c'est lui qui construit les donnees
	// mutables que l'hote relit (mcn, maxu, bfmax). Le chemin des tickets de creation le faisait
	// deja ; celui du matchmaking, non — d'ou une Turf annoncee avec la capacite figee.
	var propsDemandees *commonpb.MapValue
	if len(players) > 0 && players[0].ticket != nil {
		propsDemandees = s3SessionProperties(cfgDemandee, players[0].ticket.GetGameSession().GetProperties())
	}
	rememberRoomSettings(lastSeg(gsName), "", true, baseConfigName(cfgDemandee), propsDemandees)

	session := &mmpb.GameSession{
		Name:                    gsName,
		MaxParticipantCount:     capacite,
		CurrentParticipantCount: capacite,
		CanParticipate:          true,
		IsPublic:                true,
		State:                   mmpb.GameSession_ACTIVE,
		Host:                    relayHost,
		Port:                    relayPort,
		CreateTime:              timestamppb.Now(),
		Properties: &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			// Dernier segment de la configuration que le client a DEMANDEE (capture : le nom de
			// base de la config, pas une valeur fixe). En dur, une partie Anarchie ou Salmon Run
			// etait annoncee comme une Guerre de territoire.
			"_BaseConfigName": vStr(baseConfigName(cfgDemandee)),
			"_AliasSuffix":    vStr(""),
		}},
	}

	// [Nextendo] Chaque joueur reçoit UNIQUEMENT SA PROPRE entrée, et son propre jeton.
	//
	// Loi mesurée sur les 17 flux TrackMatchmakingTicket des 7 captures Nintendo :
	// count(matched_user_sessions) == count(user_definitions), SANS EXCEPTION — 1↔1 pour un joueur
	// seul, 4↔4 pour un groupe de 4. Le ticket ne décrit donc JAMAIS les adversaires : dans le match
	// à 4 du 23:41:21 (11673 o, 4 entrées) une SEULE porte le jeton, celle du destinataire, et les
	// trois autres n'ont que user_definition + user_session. Les joueurs se découvrent sur l'hôte de
	// session (game_session.host:port), pas dans le ticket.
	//
	// Nous envoyions l'inverse : la liste COMPLÈTE à tout le monde, avec un jeton par joueur — une
	// forme qui n'existe nulle part dans le corpus.
	for _, w := range players {
		var ud *mmpb.UserDefinition
		if len(w.ticket.UserDefinitions) > 0 {
			ud = w.ticket.UserDefinitions[0]
		}
		if ud == nil {
			ud = &mmpb.UserDefinition{User: npnTenant + "/users/" + w.uid}
		}

		// team reste "" quand le client n'en demande pas : la capture d'un match régulier porte
		// team:"" (et "team1" en bankara open). "Alpha"/"Bravo" étaient inventés, et cette valeur
		// est recopiée telle quelle dans la claim gamesync.team du jeton.
		team := ud.GetTeam()
		if c := creneauDeFete[w]; c != "" {
			team = c
		}

		// L'identité fait foi côté transport : le uid du porteur du flux, jamais un repli.
		uid := w.uid
		if uid == "" {
			uid = userIDFromPath(ud.GetUser())
		}

		userSess := gsName + "/userSessions/" + uuid4()

		// Acheminer les attributs de CE joueur jusqu'au serveur gamesync : ce sont eux qui
		// remplissent att/ltc/tn de sa UserSession, donc ce que les autres voient de lui.
		rememberParticipantAttrs(lastSeg(userSess), ud.GetAttributes(), ud.GetLatencyData().GetLatencies(), team)

		// ⚠️ INSCRIRE LE JOUEUR AU REGISTRE DE LA PARTIE — c'est lui qui fait le ROSTER.
		//
		// Mesure du 2026-08-16, sous charge reelle : 22 matchs formes, 2 seulement parvenus jusqu'a
		// l'arbitre. Les vingt autres se vidaient joueur par joueur en quelques secondes, par des
		// fermetures PROPRES cote client — donc un abandon decide par le jeu, pas une coupure.
		//
		// La cause est ici. `noterParticipant` n'etait appele que depuis CreateGameSessionCreationTicket
		// et JoinGameSession, les deux chemins du SALON PRIVE. Le matchmaking public, lui, fabrique ses
		// sessions utilisateur sans jamais rien inscrire : le registre de la partie restait VIDE.
		//
		// Consequence visible dans le journal du salon : « collection docs/__us -> 1 membre(s) »
		// alors qu'ils sont huit. Chaque console ne voyait qu'elle-meme, n'avait donc aucun
		// adversaire, et abandonnait au bout d'une vingtaine de secondes.
		noterParticipant(lastSeg(gsName), &mmpb.UserSession{
			Name:       userSess,
			User:       ud.GetUser(),
			State:      mmpb.UserSession_ACTIVE,
			Attributes: ud.GetAttributes(),
			CreateTime: timestamppb.Now(),
		})

		t := w.ticket
		t.State = mmpb.MatchmakingTicket_SUCCEEDED
		t.MatchedUserSessions = []*mmpb.MatchedUserSession{{
			UserDefinition: ud,
			UserSession:    userSess,
			MatchmakingIdToken: mintGssMatchToken(
				uid, npnTenant, gsName, userSess, team,
				gamesyncAttrJSON(ud.GetAttributes()), gamesyncLtcyJSON(ud.GetLatencyData())),
		}}
		t.GameSession = session

		log.Printf("[NPLN MM]   -> %s : uid=%s user=%s userSession=%s",
			lastSeg(t.GetName()), uid, ud.GetUser(), lastSeg(userSess))

		select {
		case w.out <- t:
		default: // client already gone
		}
	}

	served := map[*mmWaiter]bool{}
	for _, w := range players {
		served[w] = true
	}
	rest := m.waiting[:0]
	for _, w := range m.waiting {
		if !served[w] {
			rest = append(rest, w)
		}
	}
	m.waiting = rest

	members := make([]string, 0, len(players))
	for _, w := range players {
		members = append(members, w.uid)
	}
	hostUID := ""
	if len(members) > 0 {
		hostUID = members[0]
	}
	// Etiqueter le salon avec SON mode : sans cela le monitoring affichait « Partie » pour tout le
	// monde, et un salon melangeant deux modes etait indiscernable d'un salon sain.
	dashNoteRoomMode(gsName, nomLisibleDuMode(cfg), cleDuMode(cfg), requis, uint16(participants), hostUID, 0, members)
	for _, w := range players {
		if len(w.ticket.UserDefinitions) > 0 {
			dashNotePing(w.uid, bestLatencyMs(w.ticket.UserDefinitions[0].GetLatencyData()))
		}
	}

	log.Printf("[NPLN MM] MATCH FORME : %d joueur(s) distinct(s) en %s -> %s (host %s:%d)", participants, nomLisibleDuMode(cfg), gsName, relayHost, relayPort)
}

func (m *matchmakerServer) CreateMatchmakingTicket(ctx context.Context, req *mmpb.CreateMatchmakingTicketRequest) (*mmpb.MatchmakingTicket, error) {
	name := npnTenant + "/matchmakingTickets/" + uuid4()
	t := &mmpb.MatchmakingTicket{
		Name:              name,
		MatchmakingConfig: npnTenant + "/matchmakingConfigs/regular_match_config",
		State:             mmpb.MatchmakingTicket_SEARCHING,
	}
	// echo back the requesting user's definition (gamesync attrs + measured latencies)
	if td := req.GetMatchmakingTicket(); td != nil {
		if uds := td.GetUserDefinitions(); len(uds) > 0 {
			t.UserDefinitions = uds
		}
		if c := td.GetMatchmakingConfig(); c != "" {
			// Le jeu envoie l'alias « tenants/current/matchmakingConfigs/... ». Nintendo le rend
			// RESOLU vers le locataire concret — mesure sur captured/TrackMatchmakingTicket.bin,
			// qui ne contient aucune occurrence de « tenants/current », alors que notre reponse en
			// portait une. Meme correction que pour le champ `user` (bloc concreteUser) : le
			// chemin GameSessionCreationTicket resolvait deja, celui du matchmaking non.
			t.MatchmakingConfig = s3ConfigEcho(npnTenant, c)
		}
	}
	log.Printf("[NPLN MM] CreateMatchmakingTicket config=%q -> capacite annoncee %d",
		lastSeg(t.MatchmakingConfig), s3RoomCapacity(t.MatchmakingConfig, 0))

	// [Nextendo] Résoudre l'alias vers le user CONCRET, comme le fait déjà le chemin
	// GameSessionCreationTicket (bloc concreteUser). Mesuré sur la capture du 2026-08-07 :
	// la REQUÊTE porte user="tenants/current/users/current", la RÉPONSE de Nintendo porte
	// "tenants/t-dce9377b-lp1/users/u-exemple7000000000000", et les 3 trames Track le répètent.
	// Sans cette résolution, userIDFromPath() rend "current" et le jeton gss de CHAQUE joueur
	// porte la même identité bidon — aucun client ne peut se reconnaître dans le match.
	if uid := uidFromCtx(ctx); uid != "" {
		tn := tenantFromCtx(ctx)
		if tn == "" {
			tn = npnTenant
		}
		for i, ud := range t.UserDefinitions {
			t.UserDefinitions[i] = &mmpb.UserDefinition{
				User:        tn + "/users/" + uid,
				Attributes:  ud.GetAttributes(),
				LatencyData: ud.GetLatencyData(),
				Team:        ud.GetTeam(),
			}
		}
	}

	// [Nextendo] Indexer par l'ID OPAQUE. Le client crée sous "tenants/<tid>/..." (ce que nous lui
	// rendons) mais suit sous l'ALIAS "tenants/current/..." : m.tickets[req.GetName()] ne pouvait
	// donc JAMAIS aboutir, et Track repartait sur un ticket de secours SANS user_definitions.
	// gameSessionServer indexe déjà par lastSeg() (lignes 500-501, 616) — même règle ici.
	m.mu.Lock()
	m.tickets[lastSeg(name)] = t
	m.mu.Unlock()
	// Repartir en recherche PROUVE qu'on n'est plus dans la partie precedente : on l'en sort, sinon
	// le tableau de bord garde un salon fantome. Voir dashQuitterSalons.
	dashQuitterSalons(uidFromCtx(ctx))

	dashNoteSearchingMode(uidFromCtx(ctx), dashPIDFromCtx(ctx),
		dashModeSearching+" — "+nomLisibleDuMode(t.GetMatchmakingConfig()),
		cleDuMode(t.GetMatchmakingConfig()))
	if uds := t.GetUserDefinitions(); len(uds) > 0 {
		dashNotePing(uidFromCtx(ctx), bestLatencyMs(uds[0].GetLatencyData()))
	}
	log.Printf("[NPLN MM] CreateMatchmakingTicket -> %s (SEARCHING)", name)
	return t, nil
}

func (m *matchmakerServer) TrackMatchmakingTicket(req *mmpb.TrackMatchmakingTicketRequest, stream grpc.ServerStreamingServer[mmpb.MatchmakingTicket]) error {
	relayHost, relayPort, _, _, _, _, _, matchSize, _ := mmConfig()
	name := req.GetName()
	m.mu.Lock()
	stored := m.tickets[lastSeg(name)]
	m.mu.Unlock()
	var t *mmpb.MatchmakingTicket
	if stored != nil {
		// Copie : le pointeur stocké est partagé avec Create, et formMatchLocked le mute
		// (State / MatchedUserSessions / GameSession). Chaque flux Track travaille sur le sien.
		t = proto.Clone(stored).(*mmpb.MatchmakingTicket)
	}
	if t == nil {
		// unknown ticket — still answer with a minimal searching ticket so the client doesn't error
		t = &mmpb.MatchmakingTicket{Name: name, MatchmakingConfig: npnTenant + "/matchmakingConfigs/regular_match_config"}
	}
	log.Printf("[NPLN MM] TrackMatchmakingTicket %s (matchSize=%d, mode=%s)", name, matchSize, envOr("NPLN_MATCH_MODE", "search"))

	// By DEFAULT stay in SEARCHING (heartbeat). S3 opens a matchmaking ticket during boot;
	// if we instantly SUCCEED it, S3 tries to connect to a game-session server (host:7575)
	// that doesn't exist yet and hangs on the loading screen. Heartbeating SEARCHING keeps
	// S3 in the normal "no match found yet" state so it proceeds. Set NPLN_MATCH_MODE=instant
	// to return a real match (once the relay/session server exists).
	if envOr("NPLN_MATCH_MODE", "search") != "instant" {
		t.State = mmpb.MatchmakingTicket_SEARCHING
		for {
			if err := stream.Send(t); err != nil {
				return err
			}
			select {
			case <-stream.Context().Done():
				return nil
			case <-time.After(3 * time.Second):
			}
		}
	}

	// Phase 1: SEARCHING / PLACING heartbeats (the real server streams a few of these).
	for _, st := range []mmpb.MatchmakingTicket_State{mmpb.MatchmakingTicket_SEARCHING, mmpb.MatchmakingTicket_PLACING} {
		t.State = st
		if st == mmpb.MatchmakingTicket_PLACING {
			marquerChamp9(t)
		}
		if err := stream.Send(t); err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
	}

	// [Nextendo] Phase 2, replayed. The hand-built ticket below is structurally a stub: one
	// participant, no opponents, no per-player attributes. S3 accepts it as SUCCEEDED and then
	// stops dead with 2321-3072 -- it cannot start a 4v4 from a session that holds a single user.
	// The captured Nintendo answer (19789 bytes) is a COMPLETE match: every player with their
	// user id, weapons, ranks, the bankara config, the whole session block. Replay it, realigned
	// onto this session's identity and this ticket's name, so the game finally gets something it
	// can act on. The opponents in it are not live players, so this is not a playable match yet --
	// it is what gets us past the current wall and shows the next one (P2P/ICE).
	// [Nextendo] Default path: pool the clients that are actually searching and match them together.
	// This is what makes a match with a friend possible -- both builds hit this server, both park
	// here, and as soon as NPLN_MATCH_SIZE of them are waiting they get ONE shared session in which
	// each of them exists as a real participant.
	if os.Getenv("NPLN_MM_REPLAY") != "1" {
		debutRecherche := time.Now()
		w := &mmWaiter{ticket: t, uid: uidFromCtx(stream.Context()), out: make(chan *mmpb.MatchmakingTicket, 1), depuis: time.Now()}

		cfgAttendue := baseConfigName(t.GetMatchmakingConfig())

		m.mu.Lock()
		m.waiting = append(m.waiting, w)
		queued := len(m.waitersForConfigLocked(cfgAttendue))
		seuil := seuilPourConfig(cfgAttendue, matchSize)
		if int32(queued) >= seuil {
			m.formMatchLocked(cfgAttendue, relayHost, relayPort)
		}
		m.mu.Unlock()

		log.Printf("[NPLN MM] %s en file (%d/%d joueur(s) distinct(s) en %s, uid=%s)", name, queued, seuil, cfgAttendue, w.uid)

		// RE-EVALUER LE SEUIL PENDANT L'ATTENTE.
		//
		// Le seuil n'etait consulte qu'a l'insertion d'un ticket : une fois les joueurs gares dans la
		// boucle ci-dessous, plus personne ne recomptait. Mesure du 2026-08-15 : deux joueurs en file
		// (2/8), seuil abaisse a 2 par « mmtaille », et rien ne s'est produit pendant quatre minutes —
		// il aurait fallu qu'un troisieme ticket arrive pour declencher le comptage. Un drapeau a
		// chaud qui n'agit qu'au prochain evenement n'est pas un drapeau a chaud.
		revision := time.NewTicker(2 * time.Second)
		defer revision.Stop()

		for {
			select {
			case <-revision.C:
				_, _, _, _, _, _, _, tailleCourante, _ := mmConfig()
				m.mu.Lock()
				// Chaque mode a sa propre file et son propre seuil : reveiller le Salmon Run ne doit
				// pas declencher une Guerre de territoire incomplete, ni l'inverse.
				for _, cfg := range m.configsEnAttenteLocked() {
					n := len(m.waitersForConfigLocked(cfg))
					nominal := seuilPourConfig(cfg, tailleCourante)
					// Le seuil se DETEND avec l'attente : exiger huit joueurs simultanes dans une
					// population qui en reunit cinq, c'est garantir que personne ne joue.
					seuil, attente := m.seuilCourantLocked(cfg, nominal)
					journaliserAssouplissement(cfg, nominal, seuil, attente)
					if int32(n) >= seuil {
						log.Printf("[NPLN MM] seuil atteint pendant l'attente (%d/%d en %s) -> formation de la partie", n, seuil, cfg)
						m.formMatchLocked(cfg, relayHost, relayPort)
					}
				}
				m.mu.Unlock()

			case final := <-w.out:
				// Ticket annule par le joueur : le rendre tel quel et clore le flux proprement.
				if final.GetState() == mmpb.MatchmakingTicket_CANCELLED {
					log.Printf("[NPLN MM] %s -> CANCELLED (flux conclu apres annulation)", name)
					return stream.Send(final)
				}

				// [Nextendo] Ne pas annoncer le match plus tot que la vraie infrastructure.
				//
				// Mesure sur le corpus (le corpus de captures NPLN, vraie console) :
				//     23:35:14.091 -> 23:35:27.933  TrackMatchmakingTicket  = 13,8 s
				//     23:40:22.456 -> 23:41:10.851                          = 48,4 s
				// Nintendo n'envoie RIEN entre PLACING et SUCCEEDED : le flux reste simplement
				// ouvert. Nous, nous concluons en 1 a 4 s. C'est le DERNIER ecart mesurable de cet
				// echange : la requete du client est octet pour octet celle de la vraie console, et
				// notre reponse a desormais la meme structure et les memes valeurs.
				// La couche P2P/ICE du jeu se prepare pendant ce temps ; un SUCCEEDED trop precoce
				// peut l'atteindre avant qu'elle soit prete. Le meme genre de cadence a deja compte
				// ici (la reponse KeepAlive devait suivre une horloge, pas repondre du tac au tac).
				if d := dureeMiniRecherche(); d > 0 {
					if reste := d - time.Since(debutRecherche); reste > 0 {
						log.Printf("[NPLN MM] %s : match pret, on tient le flux encore %s (cadence Nintendo)",
							name, reste.Truncate(time.Millisecond))
						select {
						case <-time.After(reste):
						case <-stream.Context().Done():
							return nil
						}
					}
				}
				marquerChamp9(final)
				if soirFlag("dumpsucceeded") {
					log.Print("[NPLN MM][DIAG] NOTRE SUCCEEDED =\n" + prototext.Format(final))
				}
				// Octets REELS du message, pour un diff au fil avec captured/TrackMatchmakingTicket.bin.
				// Le prototext masque l'ordre des champs, les zeros explicites et les champs inconnus :
				// deux messages qui s'impriment pareil peuvent se serialiser differemment.
				if soirFlag("hexsucceeded") {
					if brut, err := proto.Marshal(final); err == nil {
						log.Printf("[NPLN MM][DIAG] SUCCEEDED brut %d o = %x", len(brut), brut)
					}
				}
				log.Printf("[NPLN MM] %s -> SUCCEEDED (session %s : %d/%d annonces, %d session(s) utilisateur pour moi)",
					name, lastSeg(final.GetGameSession().GetName()),
					final.GetGameSession().GetCurrentParticipantCount(),
					final.GetGameSession().GetMaxParticipantCount(),
					len(final.MatchedUserSessions))
				if err := stream.Send(final); err != nil {
					return err
				}

				// [Nextendo] NE PAS refermer le flux sur le SUCCEEDED.
				//
				// Mesure du 2026-08-13 : apres le SUCCEEDED, le jeu ouvre bien sa connexion vers
				// l'hote de session (:7575), monte le TLS puis le HTTP/2 — et sa pile grpc
				// n'a RIEN en attente d'emission (grpcPoll : « fd=1 req=Input », lecture seule).
				// Il n'a donc jamais confie IssueToken/KeepUserSession a son transport, et 1,2 s
				// plus tard il ferme la socket. Le transport est sain : c'est la couche au-dessus
				// qui renonce.
				//
				// Or nous rendions la main juste apres l'envoi, ce qui clot le flux avec des
				// trailers OK. Le code documente cette fermeture pour TrackGameSessionCreationTicket
				// — « le flux se ferme apres le dernier message » — parce qu'elle y a ete MESUREE ;
				// pour TrackMatchmakingTicket, personne ne l'a jamais verifiee, nous l'avions
				// deduite par symetrie. Dans ce projet, la maniere dont un flux se termine a deja
				// compte plus que son contenu (l'absence rendue en DEUX cadres, jamais en
				// Trailers-Only). On tient donc le flux ouvert jusqu'a ce que le client le lache.
				//
				// Drapeau a chaud « ticketferme » pour revenir a l'ancien comportement sans
				// redeployer.
				if soirFlag("ticketferme") {
					return nil
				}
				<-stream.Context().Done()
				log.Printf("[NPLN MM] %s : flux relache par le client apres le SUCCEEDED", name)
				return nil

			case <-stream.Context().Done():
				// Client gave up: drop it from the pool so it cannot poison the next match.
				m.mu.Lock()
				for i, x := range m.waiting {
					if x == w {
						m.waiting = append(m.waiting[:i], m.waiting[i+1:]...)
						break
					}
				}
				m.mu.Unlock()
				return nil

			case <-time.After(3 * time.Second):
				t.State = mmpb.MatchmakingTicket_SEARCHING
				if err := stream.Send(t); err != nil {
					return err
				}
			}
		}
	}

	if len(rawTrackMatchmakingTicket) > 0 {
		resp := alignCapturedIdentity(rawTrackMatchmakingTicket)

		// Point the ticket at the name the client is actually tracking; the captured one carries
		// the ticket uuid of the session it was recorded in. Same length either way (uuid4).
		if capName, ok := pbWalk(resp)[1]; ok && len(capName) == len(name) {
			resp = bytes.ReplaceAll(resp, capName, []byte(name))
		}

		log.Printf("[NPLN MM] TrackMatchmakingTicket %s -> rejeu capture Nintendo (%d o, match complet)",
			name, len(resp))

		return stream.SendMsg(&rawMsg{b: resp})
	}

	// Phase 2: SUCCEEDED — build the matched GameSession pointing at our relay.
	gsName := npnTenant + "/gameSessions/" + uuid4()
	var ud *mmpb.UserDefinition
	if len(t.UserDefinitions) > 0 {
		ud = t.UserDefinitions[0]
	}
	userSess := gsName + "/userSessions/" + uuid4()
	rememberParticipantAttrs(lastSeg(userSess), ud.GetAttributes(), ud.GetLatencyData().GetLatencies(), ud.GetTeam())
	cfgDemandee := t.GetMatchmakingConfig()
	capacite := s3RoomCapacity(cfgDemandee, matchSize)

	t.State = mmpb.MatchmakingTicket_SUCCEEDED
	t.MatchedUserSessions = []*mmpb.MatchedUserSession{{
		UserDefinition: ud,
		UserSession:    userSess,
		// Real S3 (capture 2026-08-07) carries an ES256 "gss" JWT here, with the match attrs +
		// latencies under a `gamesync` block. The old opaque token failed the client's JWT parse.
		MatchmakingIdToken: mintGssMatchToken(
			userIDFromPath(ud.GetUser()), npnTenant, gsName, userSess, ud.GetTeam(),
			gamesyncAttrJSON(ud.GetAttributes()), gamesyncLtcyJSON(ud.GetLatencyData())),
	}}
	t.GameSession = &mmpb.GameSession{
		Name:                    gsName,
		MaxParticipantCount:     capacite,
		CurrentParticipantCount: capacite,
		CanParticipate:          true,
		IsPublic:                true,
		State:                   mmpb.GameSession_ACTIVE,
		Host:                    relayHost,
		Port:                    relayPort,
		CreateTime:              timestamppb.Now(),
		Properties: &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			// Dernier segment de la configuration que le client a DEMANDEE (capture : le nom de
			// base de la config, pas une valeur fixe). En dur, une partie Anarchie ou Salmon Run
			// etait annoncee comme une Guerre de territoire.
			"_BaseConfigName": vStr(baseConfigName(cfgDemandee)),
			"_AliasSuffix":    vStr(""),
		}},
	}
	log.Printf("[NPLN MM] TrackMatchmakingTicket %s -> SUCCEEDED gameSession=%s host=%s:%d", name, gsName, relayHost, relayPort)
	return stream.Send(t)
}

func (m *matchmakerServer) CancelMatchmakingTicket(ctx context.Context, req *mmpb.CancelMatchmakingTicketRequest) (*emptypb.Empty, error) {
	// [Nextendo] Même clé opaque que Create/Track. Et surtout : retirer le joueur de la FILE.
	// Mesuré le 2026-08-13 : le ticket 41626599, annulé à 18:56:44, a quand même été apparié à
	// 18:57:21 (37 s plus tard) parce que son mmWaiter était resté dans m.waiting. Un ticket
	// fantôme dans la file fausserait la lecture du prochain test.
	id := lastSeg(req.GetName())
	m.mu.Lock()
	delete(m.tickets, id)
	rest := m.waiting[:0]
	var annules []*mmWaiter
	for _, w := range m.waiting {
		if lastSeg(w.ticket.GetName()) == id {
			annules = append(annules, w)
			continue
		}
		rest = append(rest, w)
	}
	m.waiting = rest
	m.mu.Unlock()

	// [Nextendo] CONCLURE le flux TrackMatchmakingTicket reste en attente.
	//
	// Retirer le ticket de la file ne suffisait pas : le flux Track du joueur restait bloque a
	// attendre un appariement qui ne viendrait plus, et le jeu — qui attend la conclusion de SON
	// flux apres avoir annule — rendait « une erreur de communication ». Mesure du 2026-08-14 :
	// ticket bankara_match_challenge_ar_team_config cree a 14:17:56, annule a 14:18:10, aucun
	// appariement, aucune activite gamesync ensuite, et l'erreur a l'ecran.
	// On lui rend donc son ticket a l'etat CANCELLED, ce qui termine le flux normalement.
	for _, w := range annules {
		fin := proto.Clone(w.ticket).(*mmpb.MatchmakingTicket)
		fin.State = mmpb.MatchmakingTicket_CANCELLED
		select {
		case w.out <- fin:
		default: // le flux s'est deja conclu de lui-meme
		}
	}

	// [Nextendo] Sortir aussi le joueur du salon affiche par le monitoring. Sans cela il y restait
	// « en partie » apres avoir annule sa recherche — symptome signale le 2026-08-14, en meme temps
	// que le blocage sur « Connexion a Internet… ».
	if uid := uidFromCtx(ctx); uid != "" {
		dashQuitterSalons(uid)
	}
	log.Printf("[NPLN MM] CancelMatchmakingTicket %s", req.GetName())
	return &emptypb.Empty{}, nil
}

// gamesyncAttrJSON renders a UserDefinition's match attributes as the typed-value JSON STRING the
// real gss token embeds under gamesync.attr: {"<attr>":{"type":"string|integer|double|array|boolean","value":…}}.
func gamesyncAttrJSON(attrs *commonpb.MapValue) string {
	m := map[string]any{}
	for k, v := range attrs.GetFields() {
		m[k] = valueTyped(v)
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// valueTyped maps a common.Value to {"type":…,"value":…} (the real gss attr encoding, mirroring the
// mmr/spr/match_hash/block_list values seen in the Turf War capture).
func valueTyped(v *commonpb.Value) map[string]any {
	switch x := v.GetValueType().(type) {
	case *commonpb.Value_StringValue:
		return map[string]any{"type": "string", "value": x.StringValue}
	case *commonpb.Value_IntegerValue:
		return map[string]any{"type": "integer", "value": x.IntegerValue}
	case *commonpb.Value_DoubleValue:
		return map[string]any{"type": "double", "value": x.DoubleValue}
	case *commonpb.Value_FloatValue:
		return map[string]any{"type": "double", "value": x.FloatValue}
	case *commonpb.Value_BooleanValue:
		return map[string]any{"type": "boolean", "value": x.BooleanValue}
	case *commonpb.Value_ArrayValue:
		return map[string]any{"type": "array", "value": []any{}}
	default:
		return map[string]any{"type": "string", "value": ""}
	}
}

// gamesyncLtcyJSON renders per-region measured latencies as {"latencies":{"<region>":{"nanos":N}}}
// (the gss token's ltcy claim), from the UserDefinition's LatencyData.
// bestLatencyMs is the lowest latency the CLIENT itself measured against the latency servers —
// the only real RTT NPLN reports to us. 0 when it measured none (we never estimate one).
func bestLatencyMs(ld *mmpb.LatencyData) int {
	best := 0
	for _, d := range ld.GetLatencies() {
		ms := int(d.GetSeconds()*1000) + int(d.GetNanos()/1_000_000)
		if ms > 0 && (best == 0 || ms < best) {
			best = ms
		}
	}
	return best
}

func gamesyncLtcyJSON(ld *mmpb.LatencyData) string {
	lat := map[string]any{}
	for region, d := range ld.GetLatencies() {
		lat[region] = map[string]any{"nanos": d.GetSeconds()*1_000_000_000 + int64(d.GetNanos())}
	}
	b, err := json.Marshal(map[string]any{"latencies": lat})
	if err != nil {
		return "{\"latencies\":{}}"
	}
	return string(b)
}

// relayHostPadded returns our relay host padded/truncated to the exact length of the captured
// Nintendo host string, so substituting it inside the replayed protobuf never changes any
// length prefix. A trailing-space-padded IPv4 is still parsed correctly by the client's
// inet_addr-style reader, and truncation cannot happen for an address shorter than the original.
func relayHostPadded() string {
	const capturedHost = "35.223.226.237"
	h, _, _, _, _, _, _, _, _ := mmConfig()

	if len(h) > len(capturedHost) {
		h = "127.0.0.1"
	}
	for len(h) < len(capturedHost) {
		h += " "
	}
	return h
}
