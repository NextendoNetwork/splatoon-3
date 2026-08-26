package main

// Generic capture-replay for the WHOLE S3 launch flow. The Switch's boot sequence
// (captured in dist/Nextendo-MITM-Splatoon3v3, extracted to ./captured_boot/*.grpc)
// hits ~25 gRPC methods before it lets you into the hall: Auth, LobbyMessaging,
// Friends, Presence, Maintenance, UserScreening, CloudSave, ugcstore, Canola,
// Locker, schedules... Implementing each by hand is huge; instead we REPLAY the
// exact bytes Nintendo returned for any method we haven't typed-implemented.
//
// How: an UnknownServiceHandler catches every method not served by a registered
// service (auth/matchmaking/toyohr-schedule+fest stay dynamic/typed and take
// precedence). A hybrid codec lets us push the raw captured protobuf bytes through
// the gRPC framing without a typed message. Each .grpc file holds the response's
// DATA stream (one or more length-prefixed gRPC messages) exactly as captured.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

var rawVsUserAttribute = capture("captured/VsUserAttribute.bin")

var rawInitializeAttributes = capture("captured/InitializeAttributes.bin")

// Reponse de Nintendo a InitializeAttributes pour un compte QUI N'A JAMAIS JOUE : les memes
// attributs coop, tous a zero. Capturee le 2026-08-12 sur un compte Nintendo vierge contre les
// vrais serveurs (une capture de compte neuf*, 389 octets de message).
//
// Elle repare deux choses. D'abord une variante que nous ne servions pas du tout : la requete
// porte le champ 15 (attributs coop sans creneau) la ou nous ne distinguions que le champ 16
// (creneau Salmon Run) et « tout le reste = VS » — un joueur qui demandait ses attributs coop
// recevait donc un document de bataille. Ensuite l'heritage de progression : notre seule capture
// venait d'un compte ETABLI, si bien qu'un nouveau joueur se voyait attribuer le grade, la note et
// les points d'un autre. Un joueur sans document recoit desormais la base a zero.
var rawInitCoopVierge = capture("captured/InitializeAttributesCoopVierge.bin")

// Meme chose pour la variante AVEC creneau (champ 16), capturee sur le meme compte vierge
// (une capture de compte neuf*, 403 octets). Le creneau qu'elle porte est celui du jour
// de la capture ; il est remplace par celui que le jeu demande, meme longueur.
var rawInitCoopShiftVierge = capture("captured/InitializeAttributesCoopShiftVierge.bin")

// Creneau porte par la capture vierge ci-dessus.
const capturedShiftVierge = "20260811160000"

// L'identifiant du compte vierge dans cette capture, a remplacer par celui du joueur.
const captureVierge = "u-exemple9000000000000"

// capturedShiftID is the Salmon Run shift the InitializeAttributes capture was taken on. Same
// 14-character format as any other shift id, so swapping it for the live one is length-safe.
const capturedShiftID = "20260810000000"

// nplnStreamHeartbeat is how often we repeat a long-lived stream's heartbeat frame. The
// captured contract is a 30s ping with a 50s deadline (SubscribePresences literally carries
// {30, 50}); 20s keeps a comfortable margin against jitter.
const nplnStreamHeartbeat = 20 * time.Second

// lobbyEcho fans S3's LobbyMessaging/SendMessage payloads back into its RecvMessage stream.
// S3's ink loading screen only advances when a "type":"NplnLogin" message comes BACK on
// RecvMessage, and the one hard check is that the message's AppVer == the running build's
// version. S3 publishes its own NplnLogin (with the correct live AppVer) via SendMessage, so
// echoing it back to RecvMessage satisfies the gate without us guessing AppVer. (RE: scratchpad/
// S3_LOBBY_READY_FINDINGS.md — discriminator @0x32675e0, AppVer cmp @0x3269604, advance evt #5.)
// ⚠️ UN CANAL PAR JOUEUR, PAS UN SEUL POUR TOUT LE SERVEUR.
//
// C'etait « var lobbyEcho = make(chan []byte, 64) » — un canal unique. Chaque SendMessage y etait
// depose et n'importe quel flux RecvMessage le ramassait : une invitation de match prive partait
// donc chez UN AUTRE joueur, tire au sort par l'ordonnanceur. Signale le 2026-08-22 : un joueur
// voyait l'annonce d'un inconnu qui n'etait meme pas son ami.
//
// Et l'intention d'origine, ecrite juste au-dessus, etait bien « lui renvoyer SES publications » —
// le defaut n'apparaissait qu'a partir du deuxieme joueur connecte.
//
// On cloisonne donc par joueur. Un joueur ne recoit plus que ses propres echos : c'est ce que le
// code voulait faire, et ca supprime la fuite. Distribuer une invitation A SES AMIS reste a ecrire,
// et ce sera une fonctionnalite a part, pas un effet de bord.
var echosDeHall = struct {
	sync.Mutex
	m map[string]chan []byte
}{m: map[string]chan []byte{}}

// canalDeHall rend le canal d'echo de ce joueur, en le creant au besoin.
func canalDeHall(uid string) chan []byte {
	echosDeHall.Lock()
	defer echosDeHall.Unlock()
	c := echosDeHall.m[uid]
	if c == nil {
		c = make(chan []byte, 64)
		echosDeHall.m[uid] = c
	}
	return c
}

// oublierCanalDeHall libere le canal quand le joueur s'en va.
func oublierCanalDeHall(uid string) {
	echosDeHall.Lock()
	delete(echosDeHall.m, uid)
	echosDeHall.Unlock()
}

// deposerSiPresent remet un message a un joueur SEULEMENT s'il ecoute deja.
//
// On ne cree pas de canal ici, a la difference de canalDeHall : distribuer a une liste d'amis
// creerait sinon un canal par ami hors ligne, jamais lu et jamais libere. Le message d'un joueur
// absent est simplement perdu, ce qui est le comportement attendu d'une notification de hall.
func deposerSiPresent(uid string, msg []byte) bool {
	echosDeHall.Lock()
	c := echosDeHall.m[uid]
	echosDeHall.Unlock()
	if c == nil {
		return false
	}
	select {
	case c <- msg:
		return true
	default:
		return false
	}
}

// distribuerAuxAmis remet une publication de hall a son auteur ET a ses amis en ligne.
//
// C'est ce que le jeu attend d'une invitation de match — privee comme publique : elle s'affiche
// dans les notifications de vos AMIS. Nous n'avions rien de tel. Le canal etait global, donc
// l'annonce partait chez un joueur au hasard ; puis cloisonne par joueur, donc elle ne partait plus
// nulle part. Ici on la distribue enfin a qui de droit.
//
// La liste d'amis vient du service de comptes, la meme que l'authentification interroge. L'appel
// part dans sa propre routine : c'est un aller-retour HTTP, et la boucle qui lit les publications
// ne doit pas l'attendre.
func distribuerAuxAmis(uid string, msg []byte) {
	if uid == "" {
		return
	}
	// L'auteur recoit toujours son echo — c'est lui qui debloque son propre ecran de connexion.
	deposerSiPresent(uid, msg)

	go func() {
		pid := pidPourUid(uid)
		if pid == 0 {
			log.Printf("[NPLN LOBBY] %s : pas de PID connu, publication non distribuee", short(uid))
			return
		}
		acc, err := accountFriends(pid)
		if err != nil {
			log.Printf("[NPLN LOBBY] %s : liste d'amis indisponible (%v)", short(uid), err)
			return
		}
		remis, total := 0, 0
		for _, ami := range acc.Friends {
			if ami.UserID == "" || ami.UserID == uid {
				continue
			}
			total++
			if deposerSiPresent(ami.UserID, msg) {
				remis++
			}
		}
		log.Printf("[NPLN LOBBY] %s : publication remise a %d ami(s) en ligne sur %d",
			short(uid), remis, total)
	}()
}

// hybrid codec: passes *rawMsg through untouched, everything else via protobuf.
// (We marshal proto ourselves rather than delegating to gRPC's registered codec,
// which in recent gRPC is a CodecV2 not reachable via encoding.GetCodec.)
type rawMsg struct{ b []byte }

type hybridCodec struct{}

func (hybridCodec) Marshal(v any) ([]byte, error) {
	if r, ok := v.(*rawMsg); ok {
		return r.b, nil
	}
	if m, ok := v.(proto.Message); ok {
		return proto.Marshal(m)
	}
	return nil, fmt.Errorf("hybridCodec: cannot marshal %T", v)
}

func (hybridCodec) Unmarshal(data []byte, v any) error {
	if r, ok := v.(*rawMsg); ok {
		r.b = append([]byte(nil), data...)
		return nil
	}
	if m, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, m)
	}
	return fmt.Errorf("hybridCodec: cannot unmarshal into %T", v)
}

func (hybridCodec) Name() string { return "proto" } // client speaks content-subtype "proto"

func newHybridCodec() hybridCodec { return hybridCodec{} }

// lobbyHeartbeatFrame returns the trailing heartbeat frame of the captured lobby stream
// (the small frame Nintendo repeats after the cursor + message frames), if there is one.
func lobbyHeartbeatFrame() ([]byte, bool) {
	data, err := capturedBoot.ReadFile("captured_boot/toyohr.v1.LobbyMessaging.RecvMessage.grpc")
	if err != nil {
		return nil, false
	}
	var last []byte
	for len(data) >= 5 {
		mlen := int(binary.BigEndian.Uint32(data[1:5]))
		if 5+mlen > len(data) {
			break
		}
		last = append([]byte(nil), data[5:5+mlen]...)
		data = data[5+mlen:]
	}
	// Only a small trailing frame is a heartbeat; a big one is real content we must not repeat.
	if len(last) == 0 || len(last) > 16 {
		return nil, false
	}
	return last, true
}

// lobbyCursorInitFrame returns the FIRST frame of the captured lobby stream — the toyohr
// cursor-init frame ({2:{1:<n>, 2:"{\"v\":1,\"t\":\"<ms>-0\"}"}}). It carries NO NplnLogin and no
// stale identity (just the stream's starting cursor), so replaying it verbatim is safe. S3's toyohr
// stream layer needs this baseline before it will hand later messages up to the game payload parser;
// without it our reframed NplnLogin echo never reaches the NplnLogin handler and the loading screen
// never advances (RE: scratchpad/S3_LOBBY_READY_FINDINGS.md — the game gate is fine, the stream
// delivery is what stalls). It is a small metadata frame, distinct from the (poison) stale NplnLogin
// content frame we still skip.
func lobbyCursorInitFrame() ([]byte, bool) {
	data, err := capturedBoot.ReadFile("captured_boot/toyohr.v1.LobbyMessaging.RecvMessage.grpc")
	if err != nil || len(data) < 5 {
		return nil, false
	}
	mlen := int(binary.BigEndian.Uint32(data[1:5]))
	if 5+mlen > len(data) {
		return nil, false
	}
	first := append([]byte(nil), data[5:5+mlen]...)
	// A cursor-init frame is small (the captured one is 37 bytes) and must NOT be the big NplnLogin
	// content frame; guard on size so a different capture layout can't make us replay poison.
	if len(first) == 0 || len(first) > 80 {
		return nil, false
	}
	return first, true
}

// methodToFile maps "/nn.npln.toyohr.v1.LobbyMessaging/RecvMessage" ->
// "captured_boot/toyohr.v1.LobbyMessaging.RecvMessage.grpc".
func methodToFile(fullMethod string) string {
	k := strings.TrimPrefix(fullMethod, "/")
	k = strings.TrimPrefix(k, "nn.npln.")
	k = strings.ReplaceAll(k, "/", ".")
	return "captured_boot/" + k + ".grpc"
}

// isServerStreamingMethod reports whether a replayed method is a long-lived server stream
// (the lobby / notification / subscription feeds) that the Switch keeps open for the whole
// session. These must NOT be closed after the captured messages or S3 thinks it lost the lobby.
func isServerStreamingMethod(method string) bool {
	for _, s := range []string{"RecvMessage", "Subscribe", "Watch", "GetEvent", "Stream"} {
		if strings.Contains(method, s) {
			return true
		}
	}
	return false
}

// isClientHeartbeat reports whether the method is a long-lived client->server heartbeat
// (presence KeepAlive). The client sends a ping and keeps its send side OPEN, expecting an
// ack per ping. The generic "drain all requests, then reply once" path DEADLOCKS here: the
// second RecvMsg blocks forever waiting for the next periodic ping, so we never reply, S3
// never gets its presence ack, and it hangs on the ink loading screen. Handle as bidi ack.
func isClientHeartbeat(method string) bool {
	return strings.Contains(method, "KeepAlive")
}

// pbWalk returns the top-level length-delimited protobuf fields of b (field number -> bytes).
func pbWalk(b []byte) map[int][]byte {
	out := map[int][]byte{}
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			break
		}
		b = b[n:]
		field, wt := int(tag>>3), tag&7
		if wt != 2 {
			break // we only care about length-delimited fields
		}
		l, n2 := binary.Uvarint(b)
		if n2 <= 0 || int(l) > len(b) {
			break
		}
		b = b[n2:]
		out[field] = b[:l]
		b = b[l:]
	}
	return out
}

func pbField(field int, val []byte) []byte {
	var hdr [20]byte
	n := binary.PutUvarint(hdr[:], uint64(field<<3|2))
	n += binary.PutUvarint(hdr[n:], uint64(len(val)))
	return append(append([]byte(nil), hdr[:n]...), val...)
}

type pbEntry struct {
	field int
	val   []byte
}

// pbWalkList returns the top-level length-delimited fields IN ORDER (keeps duplicates).
func pbWalkList(b []byte) []pbEntry {
	var out []pbEntry
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			break
		}
		b = b[n:]
		field, wt := int(tag>>3), tag&7
		if wt != 2 {
			break
		}
		l, n2 := binary.Uvarint(b)
		if n2 <= 0 || int(l) > len(b) {
			break
		}
		b = b[n2:]
		out = append(out, pbEntry{field, b[:l]})
		b = b[l:]
	}
	return out
}

// verVal builds the version value-submsg {3: <varint v>} (matching how app_version is wired).
func verVal(v uint64) []byte {
	var t [10]byte
	n := binary.PutUvarint(t[:], v)
	return append([]byte{0x18}, t[:n]...)
}

// patchPayloadVer rebuilds S3's NplnLogin payload (fields 1=id,2=body,3="NplnLogin") with the
// app_version map entry overwritten to v AND an extra "AppVer"=v entry — S3 sends a placeholder
// 0 that the real server fills with the running build's version (the one hard-checked field).
func patchPayloadVer(payload []byte, v uint64) []byte {
	var id, body, typ []byte
	for _, f := range pbWalkList(payload) {
		switch f.field {
		case 1:
			id = f.val
		case 2:
			body = f.val
		case 3:
			typ = f.val
		}
	}
	var nb []byte
	for _, e := range pbWalkList(body) { // each entry e.field==1, e.val = {1:key, 2:val}
		var key []byte
		for _, kv := range pbWalkList(e.val) {
			if kv.field == 1 {
				key = kv.val
			}
		}
		entry := e.val
		if string(key) == "app_version" {
			entry = append(pbField(1, key), pbField(2, verVal(v))...)
		}
		nb = append(nb, pbField(1, entry)...)
	}
	// extra "AppVer" entry (the game reads "AppVer", distinct from "app_version")
	nb = append(nb, pbField(1, append(pbField(1, []byte("AppVer")), pbField(2, verVal(v))...))...)
	out := pbField(1, id)
	out = append(out, pbField(2, nb)...)
	out = append(out, pbField(3, typ)...)
	return out
}

// sendToRecv reframes S3's LobbyMessaging/SendMessage publish into the RecvMessage delivery
// shape so S3 accepts it as a real lobby message. SendMessage = {1:from, 2:to(userpath),
// 3:payload(id/body/"NplnLogin")}. RecvMessage body (from the captured template) =
// {1:{ 1:payload, 2:senderPath, 4:envelope, 5:ts }}. The NplnLogin payload (carrying the live
// AppVer/device_id) is field 3 of SendMessage; we lift it into RecvMessage field 1, keep the
// user path, and attach a minimal envelope + the captured static timestamp.
func buildRecv(send, payload []byte, cursor string) []byte {
	f := pbWalk(send)
	if payload == nil {
		payload = f[3]
	}
	if payload == nil {
		return nil
	}
	// Resolve the user path. S3 sends the placeholder "tenants/current/users/<uid>" (44 bytes);
	// the deserializer (0x356786c) copies a FIXED 0x33=51-byte path, so it must be the resolved
	// form "tenants/t-dce9377b-lp1/users/<uid>" (29+22=51) — exactly the length the capture used.
	src := f[2]
	if src == nil {
		src = f[1]
	}
	uid := string(src)
	if i := strings.LastIndexByte(uid, '/'); i >= 0 {
		uid = uid[i+1:]
	}
	userpath := []byte("tenants/t-dce9377b-lp1/users/" + uid)
	inner := pbField(1, payload)
	inner = append(inner, pbField(2, userpath)...)
	// The {"v","t"} envelope is the toyohr STREAM CURSOR; messages older than the last-seen
	// cursor are dropped. Each variant gets a strictly-increasing cursor (passed in).
	inner = append(inner, pbField(4, []byte(`{"v":1,"t":"`+cursor+`-0"}`))...)
	// field 5 = timestamp submsg, copied verbatim from the captured RecvMessage template.
	inner = append(inner, 0x2a, 0x0b, 0x08, 0xd9, 0x93, 0x84, 0xd2, 0x06, 0x10, 0x81, 0x8b, 0xda, 0x25)
	return pbField(1, inner)
}

// liveReplayUID resolves THIS session's live NPLN user id from the stream. Replayed captured bodies
// name the capture user (u-qoahvkaf…); rewriting them to the live user makes every replayed response
// self-consistent with the logged-in identity. Without this, the NPLN SDK searches the presence /
// session collection for its OWN user id, never finds it (the replays name a different user), logs
// "Timed out to get my UserSession." and never advances to the plaza (RE: S3_PLAZA_GATE_FINDINGS.md).
func liveReplayUID(stream grpc.ServerStream) string {
	if pid, ok := callerPID(stream.Context()); ok {
		if me, err := accountFriends(pid); err == nil && me.UserID != "" {
			return me.UserID
		}
	}
	return capturedUser
}

// rewriteCapturedIdentity replaces the capture user's id with the live session's id inside captured
// protobuf bytes. Both NPLN user ids are 22 chars ("u-"+20), so the replacement is length-preserving
// and never disturbs the gRPC/protobuf framing.
func rewriteCapturedIdentity(data []byte, liveUID string) []byte {
	if liveUID == "" || liveUID == capturedUser || len(liveUID) != len(capturedUser) {
		return data
	}
	return bytes.ReplaceAll(data, []byte(capturedUser), []byte(liveUID))
}

// --- realignement des identifiants d'une reponse capturee sur ceux reellement demandes ---

// motifUid reconnait un identifiant NPLN : « u- » suivi de 20 caracteres minuscules ou chiffres.
// Longueur totale 22, invariable — c'est ce qui rend la substitution sans risque pour le protobuf.
var motifUid = regexp.MustCompile(`u-[a-z0-9]{20}`)

// uidsPresents rend les identifiants distincts d'un bloc, dans leur ordre d'apparition.
func uidsPresents(b []byte) []string {
	vus := map[string]bool{}
	var out []string
	for _, m := range motifUid.FindAll(b, -1) {
		s := string(m)
		if !vus[s] {
			vus[s] = true
			out = append(out, s)
		}
	}
	return out
}

// uidsDemandes lit les identifiants que le client a mis dans sa requete (parametre « UIDs »).
// On les extrait par motif plutot qu'en decodant la carte de parametres : la forme du parametre
// varie d'un appel a l'autre, l'identifiant non.
func uidsDemandes(requete []byte) []string {
	if len(requete) == 0 {
		return nil
	}
	return uidsPresents(requete)
}

// realignerUids remplace les identifiants de la capture par ceux demandes, un pour un et dans
// l'ordre. Un seul passage, pour qu'une substitution ne soit jamais resubstituee ensuite.
func realignerUids(data []byte, demandes []string) []byte {
	presents := uidsPresents(data)
	if len(presents) == 0 || len(demandes) == 0 {
		return data
	}
	corresp := make(map[string]string, len(presents))
	for i, p := range presents {
		corresp[p] = demandes[i%len(demandes)]
	}
	return motifUid.ReplaceAllFunc(data, func(m []byte) []byte {
		if v, ok := corresp[string(m)]; ok && len(v) == len(m) {
			return []byte(v)
		}
		return m
	})
}

// replayHandler answers any unregistered method by replaying the captured response.
func replayHandler(srv any, stream grpc.ServerStream) error {
	method := "?"
	if ts := grpc.ServerTransportStreamFromContext(stream.Context()); ts != nil {
		method = ts.Method()
	}

	// Presence heartbeat (bidi): S3 keeps the send side open and pings periodically, expecting
	// an ack each time. Ack every ping so its presence stays active and the hall-init completes.
	// (The generic drain-then-reply path below would block forever on the open stream.)
	if isClientHeartbeat(method) {
		var ack []byte
		if data, err := capturedBoot.ReadFile(methodToFile(method)); err == nil && len(data) >= 5 {
			if mlen := int(binary.BigEndian.Uint32(data[1:5])); 5+mlen <= len(data) {
				ack = data[5 : 5+mlen]
			}
		}
		log.Printf("[NPLN replay] %s -> bidi heartbeat (ack %d bytes/ping)", method, len(ack))
		for {
			var req rawMsg
			if err := stream.RecvMsg(&req); err != nil {
				return nil // client closed the stream
			}
			if err := stream.SendMsg(&rawMsg{b: append([]byte(nil), ack...)}); err != nil {
				return err
			}
		}
	}

	// LobbyMessaging pub/sub. SendMessage: log + fan S3's published bytes into lobbyEcho.
	// RecvMessage: replay the captured context messages, then forward echoed publishes so S3
	// receives its own NplnLogin (correct AppVer) and leaves the loading screen.
	if strings.Contains(method, "LobbyMessaging/SendMessage") {
		for {
			var req rawMsg
			if stream.RecvMsg(&req) != nil {
				break
			}
			log.Printf("[NPLN LOBBY] SendMessage publish %d bytes: %x", len(req.b), req.b)
			// [Nextendo] The toyohr stream cursor "t" MUST be a MILLISECOND epoch (13 digits), exactly
			// like the real Nintendo server ({"v":1,"t":"1782647257080-0"}). We previously used
			// UnixNano() -> a 19-digit value; S3's toyohr cursor parser reads "t" as milliseconds, so a
			// nanosecond value is an out-of-range/garbage timestamp -> S3 REJECTS the reframed NplnLogin
			// echo, never completes login, and times out on the ink loading screen after ~30s. UnixMilli
			// matches the captured format and stays strictly newer than the captured cursor.
			recv := buildRecv(req.b, nil, fmt.Sprintf("%d", time.Now().UnixMilli()))
			if recv == nil {
				continue
			}
			log.Printf("[NPLN LOBBY] reframed -> RecvMessage %d bytes: %x", len(recv), recv)
			distribuerAuxAmis(uidFromCtx(stream.Context()), recv)
		}
		var ack []byte
		if data, err := capturedBoot.ReadFile(methodToFile(method)); err == nil && len(data) >= 5 {
			if mlen := int(binary.BigEndian.Uint32(data[1:5])); 5+mlen <= len(data) {
				ack = data[5 : 5+mlen]
			}
		}
		return stream.SendMsg(&rawMsg{b: append([]byte(nil), ack...)})
	}
	if strings.Contains(method, "LobbyMessaging/RecvMessage") {
		// NOTE: we deliberately do NOT replay the stale captured RecvMessage messages — they
		// carry an old stream cursor (~1782647257080) and a stale NplnLogin that poison the
		// stream. We deliver only the live reframed NplnLogin echo (high cursor) below.
		log.Printf("[NPLN LOBBY] RecvMessage: forwarding live SendMessage echoes only (no stale canned) + heartbeat %s", nplnStreamHeartbeat)
		// [Nextendo] NOTE: sending the captured cursor-init frame here (lobbyCursorInitFrame) REGRESSED
		// the flow — when S3 opened RecvMessage before its SendMessage, receiving the init frame made it
		// skip announcing its own NplnLogin, so we never got a SendMessage to echo and it stalled even
		// earlier. Reverted: forward only live echoes + heartbeat (the furthest-reaching behavior, which
		// gets S3 through the NplnLogin echo and on to Schedule fetch).
		// The captured lobby stream also ends with a small heartbeat frame that Nintendo keeps
		// repeating. Between echoes we used to send nothing at all, so S3 timed out and
		// re-subscribed roughly every minute — the same flap as the other streams.
		beat, hasBeat := lobbyHeartbeatFrame()
		ticker := time.NewTicker(nplnStreamHeartbeat)
		defer ticker.Stop()
		moi := uidFromCtx(stream.Context())
		mien := canalDeHall(moi)
		defer oublierCanalDeHall(moi)
		for {
			select {
			case b := <-mien:
				log.Printf("[NPLN LOBBY] RecvMessage -> echo %d bytes to S3", len(b))
				if err := stream.SendMsg(&rawMsg{b: b}); err != nil {
					return err
				}
			case <-ticker.C:
				if hasBeat {
					if err := stream.SendMsg(&rawMsg{b: beat}); err != nil {
						return nil
					}
				}
			case <-stream.Context().Done():
				return nil
			}
		}
	}

	// [Nextendo] GameRecord/InitializeAttributes creates a user's game-record attributes and, per
	// the captured proto (les protos reconstitues), answers
	// {1: MapValue attributes, 2: string document}. The generic replay had no capture for it and
	// returned an EMPTY message, so S3 learned neither the attributes nor WHICH document now holds
	// them; it re-read the document, still found it missing, called this again, and spun forever
	// (measured ~600 calls and ~1200 GetDocument in three minutes = the endless ink screen).
	// Answer with the document path the caller asked us to initialise so the cycle terminates.
	// [Nextendo] GameRecord/InitializeAttributes -- now served from a REAL capture of Nintendo's
	// answer (les outils de capture, 2026-08-10, exchange #8; the Switch sent byte-for-byte the same
	// request our emulator does). Everything I reconstructed by hand pointed at the wrong resource:
	// the reply's `document` is CoopUserAttribute/Data, NOT the point card the game had just failed
	// to read. That mismatch is what kept S3 re-initialising ~600 times a minute. The captured reply
	// also carries the full attribute map (grade, total_grade_point, team_contest_point, smell,
	// limited_shift_id...), which no amount of guessing was going to produce.
	// Two substitutions keep it consistent with the live session: the captured account id, and the
	// shift id -- the game asks for today's shift and the capture holds the one it was taken on.
	// Both are fixed-length, so the wire stays valid.
	// [Nextendo] Meme spirale que ci-dessous, mesuree le 2026-08-23 : voir
	// game_record_mesure_fete.go. Le rejeu generique repondait vide, le jeu relisait un document
	// qui n'existait pas, et recommencait vingt-six fois par seconde.
	// Le rejeu de la capture n'est actif que sur demande : depuis que WeaponPowerMeasurement/16
	// recoit sa reponse a deux cadres, le jeu n'appelle plus cette methode en boucle, et la reponse
	// que nous rejouons vient d'un AUTRE compte — puissance, horodatage de creation et defi de
	// festival compris. Tant qu'on ne l'a pas innocentee, elle reste derriere « festpower ».
	if strings.Contains(method, "GameRecord/CreateFestPowerMeasurement") && soirFlag("festpower") {
		var greq rawMsg
		var first []byte
		for stream.RecvMsg(&greq) == nil {
			if first == nil && len(greq.b) > 0 {
				first = append([]byte(nil), greq.b...)
			}
			greq = rawMsg{}
		}
		resp, err := repondreMesureDeFete(first, liveReplayUID(stream))
		if err != nil {
			log.Printf("[NPLN GameRecord] CreateFestPowerMeasurement illisible (%d o) : %v", len(first), err)
			return err
		}
		return stream.SendMsg(&rawMsg{b: resp})
	}

	if strings.Contains(method, "GameRecord/InitializeAttributes") {
		var greq rawMsg
		var first []byte
		for stream.RecvMsg(&greq) == nil {
			if first == nil && len(greq.b) > 0 {
				first = append([]byte(nil), greq.b...)
			}
			greq = rawMsg{}
		}

		// [Nextendo] The method serves TWO different initialisations and the answer must match which
		// one was asked for. CoopShift is field 16 (tag bytes 82 01 = varint 130, 130>>3 = 16) and
		// means Salmon Run; anything else is the VS side (initial_vs / new_season / the per-mode
		// sub-messages). Replying with the Coop capture to a VS request hands the game a Salmon Run
		// document when it asked for its battle attributes, and it re-initialises forever -- that is
		// exactly what happened on entering the hall: 1191 calls in two minutes, zero GetDocument,
		// matchmaking never reached.
		var resp []byte

		// Champ 15 : attributs coop SANS creneau. Variante distincte du champ 16, mesuree sur la
		// capture du compte vierge (requete de 33 octets utiles, reponse de 389). Elle tombait
		// jusqu'ici dans la branche VS.
		if _, ok := pbWalk(first)[15]; ok && soirFlag("initattr") {
			resp = alignerCompteVierge(rawInitCoopVierge, liveReplayUID(stream))
			log.Printf("[NPLN GameRecord] InitializeAttributes COOP (champ 15) -> base joueur neuf (%d o)", len(resp))
		} else if sub, ok := pbWalk(first)[16]; ok {
			shiftID, jobType := parseInitializeAttributes(sub)

			// Quelle base ? Notre capture d'origine vient d'un compte ETABLI : la rejouer telle quelle
			// donne a un nouveau joueur le grade, la note et les points de quelqu'un d'autre. Tant que
			// le joueur n'a pas son propre document, on part de la capture du compte VIERGE, dont tous
			// les compteurs sont a zero.
			uid := liveReplayUID(stream)
			resp = alignCapturedIdentity(rawInitializeAttributes)
			origine, capShift := "rejeu capture", capturedShiftID
			if soirFlag("initattr") {
				// Base a zero tant que le joueur n'a pas son propre document.
				vierge := alignerCompteVierge(rawInitCoopShiftVierge, uid)
				if doc, ok := pbWalk(vierge)[2]; ok {
					if _, connu := storeGetDocument(string(doc)); !connu {
						resp, origine, capShift = vierge, "base joueur neuf", capturedShiftVierge
					}
				}
			}
			if len(shiftID) == len(capShift) {
				resp = bytes.ReplaceAll(resp, []byte(capShift), []byte(shiftID))
			}
			log.Printf("[NPLN GameRecord] InitializeAttributes COOP (champ 16) shift=%s type=%s -> %s (%d o)",
				shiftID, jobType, origine, len(resp))
		} else {
			// VS: build the reply from the captured VsUserAttribute document -- its field 2 is the
			// very attribute map the game expects back (season_id, udemae, every x_power_*, the
			// battle history...), and field 1 is the document name to announce.
			vsDoc := alignCapturedIdentity(rawVsUserAttribute)
			parts := pbWalk(vsDoc)

			resp = pbField(2, parts[1])
			if attrs, ok := parts[2]; ok {
				resp = append(pbField(1, attrs), resp...)
			}

			log.Printf("[NPLN GameRecord] InitializeAttributes VS -> rejeu capture (%d o, doc=%s)",
				len(resp), string(parts[1]))
		}

		// Materialise the document the reply names, so the read that follows finds it.
		if doc, ok := pbWalk(resp)[2]; ok {
			storeInitializedCoopAttribute(string(doc), resp)
		}

		return stream.SendMsg(&rawMsg{b: resp})
	}

	// [Nextendo] CloudSave — REAL persistence. This was falling through to the generic replay, so
	// every WriteSaveRecord was acknowledged with a canned reply and thrown away, and every
	// GetSaveRecord replayed the 15 KB save blob frozen in the capture. Visible consequence: change
	// your in-game name, restart the emulator, and the CAPTURE's old name is back -- nothing the
	// player does is ever kept. Store what the client writes, per user, and hand it back on read.
	if strings.Contains(method, "CloudSave/WriteSaveRecord") {
		var wreq rawMsg
		var body []byte
		for stream.RecvMsg(&wreq) == nil {
			if len(wreq.b) > 0 {
				body = append([]byte(nil), wreq.b...)
			}
			wreq = rawMsg{}
		}

		uid := saveOwner(stream.Context())

		req := &toyohrpb.WriteSaveRecordRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			log.Printf("[NPLN CloudSave] WriteSaveRecord uid=%s: delta illisible (%d o): %v", uid, len(body), err)
			return stream.SendMsg(&toyohrpb.WriteSaveRecordResponse{Timestamp: timestamppb.Now()})
		}

		if os.Getenv("NPLN_SAVE_DELTAS") == "1" {
			saveStore.put(uid, body) // keep raw deltas on disk for diagnostics
		}

		// Le jeton de version que le client renvoie. Chez Nintendo, WriteSaveRecord porte en champ 2
		// l'update_time rendu par CreateSaveRecord (capture du compte vierge, 27.*.req : « 12 0c »
		// suivi d'un Timestamp, puis 28.*.req qui renvoie celui de 27). Un client qui n'a jamais cree
		// son record n'a aucun jeton a presenter — on veut donc SAVOIR ce qu'il envoie.
		jeton := "absent"
		if t := req.GetUpdateTime(); t != nil {
			jeton = t.AsTime().Format(time.RFC3339Nano)
		}

		// ⚠️ On n'ecrit pas dans un record qui n'existe pas.
		//
		// Jusqu'ici, apply() appelait load(), qui FABRIQUAIT le record manquant : le client recevait
		// un OK et un horodatage tout neuf, donc il n'apprenait jamais qu'il n'avait rien dans le
		// cloud, et il n'appelait jamais CreateSaveRecord. C'est la meme faute que le drapeau
		// « recordneuf », prise par une autre porte.
		//
		// MESURE DU 2026-08-13 qui l'a mise au jour : sauvegarde locale supprimee, tutoriel refait,
		// cloud vide, reponse vide a la forme exacte de Nintendo — et pourtant zero CreateSaveRecord
		// (plus gros envoi socket du jeu : 5 922 o, contre 15 049 pour un CreateSaveRecord). Le jeu
		// enchainait directement sur WriteSaveRecord (InitCoopTeamContest, NewSeason,
		// SeqFlagVsLobbyMulti), que nous acceptions.
		//
		// La sequence de Nintendo sur un compte vierge est : GetSaveRecord vide -> CreateSaveRecord
		// (le client televerse SA save, 15 049 o) -> WriteSaveRecord (les deltas). Un Write ne peut
		// donc pas preceder un Create.
		if soirFlag("writestrict") && !recordStore.has(uid) {
			log.Printf("[NPLN CloudSave] WriteSaveRecord uid=%s evt=%s cles=%v jeton=%s -> REFUSE : aucun record (le client doit d'abord appeler CreateSaveRecord)",
				uid, strings.TrimPrefix(req.GetSaveEventType(), "tenants/current/saveEventTypes/"),
				deltaKeys(req.GetSaveRecord().GetSaveData()), jeton)

			return status.Error(codes.NotFound, "save record not found")
		}

		delta := req.GetSaveRecord().GetSaveData()
		ts := recordStore.apply(uid, delta)
		log.Printf("[NPLN CloudSave] WriteSaveRecord uid=%s jeton recu=%s", uid, jeton)
		log.Printf("[NPLN CloudSave] WriteSaveRecord uid=%s evt=%s cles=%v -> fusionne, update_time=%s",
			uid, strings.TrimPrefix(req.GetSaveEventType(), "tenants/current/saveEventTypes/"),
			deltaKeys(delta), ts.AsTime().Format(time.RFC3339Nano))

		// The client echoes this timestamp back as the next request's update_time, so it MUST advance.
		return stream.SendMsg(&toyohrpb.WriteSaveRecordResponse{Timestamp: ts})
	}

	if strings.Contains(method, "CloudSave/GetSaveRecord") {
		var greq rawMsg
		var greqBody []byte
		for stream.RecvMsg(&greq) == nil {
			if len(greq.b) > 0 {
				greqBody = append([]byte(nil), greq.b...)
			}
			greq = rawMsg{}
		}
		// Comparaison directe avec la console VIERGE, qui envoyait exactement 37 octets utiles :
		//   0a 23 "tenants/current/saveRecords/current"
		// Si notre client envoie autre chose (un nom de record concret, un update_time...), c'est
		// que son etat local n'est PAS celui d'un compte neuf — et c'est ce qui expliquerait qu'il
		// n'appelle jamais CreateSaveRecord la ou la console le faisait 7 s apres.
		log.Printf("[NPLN CloudSave] GetSaveRecord requete (%d o) : %x", len(greqBody), greqBody)

		uid := saveOwner(stream.Context())

		// Joueur sans sauvegarde cloud : lui en creer une VIERGE, plutot que de repondre vide.
		//
		// MESURE DU 2026-08-12 qui tranche entre les deux : avec un record present, le jeu enchaine
		// enfin FestService/SelectFestSchedule (jamais vu jusque-la), la rafale de GetDocument et
		// WriteSaveRecord ; avec une reponse vide, il s'arrete exactement la et se declare hors
		// ligne. Le jeu attend donc une sauvegarde RESOLUE, et il ne la cree pas de lui-meme dans
		// nos conditions. On la lui donne — mais celle d'un joueur neuf, pas celle d'un inconnu.
		if soirFlag("recordneuf") && uid != "" && !recordStore.has(uid) && !seedFromCapture() {
			// Le pseudo n'est pas connu ici (le service de comptes ne le renvoie pas) : le jeu le
			// posera lui-meme au premier ChangeUserName, deja implemente plus bas.
			rec := creerRecordJoueurNeuf(uid, "")
			return stream.SendMsg(rec)
		}

		if !recordStore.has(uid) && !seedFromCapture() {
			// Joueur sans sauvegarde dans le cloud : SUCCES AVEC UN CORPS VIDE, et rien d'autre.
			//
			// MESURE DIRECTE (capture Proxide du 2026-08-11 22:47, compte Nintendo VIERGE contre les
			// VRAIS serveurs de Nintendo, la capture qui manquait depuis le debut) :
			// CONFIRME une seconde fois le 2026-08-12 00:17, compte Nintendo vierge, capture Proxide
			// archivee dans une capture de compte neuf :
			//   00:17:09.338  GetSaveRecord     Succeeded  requete 42 o -> reponse 0 o
			//   00:17:15.729  CreateSaveRecord  Succeeded  15049 o -> 15337 o (le client televerse SA save)
			//   00:17:19.726  WriteSaveRecord   Succeeded
			// Le client comprend le corps vide comme « rien dans le cloud » et cree son record.
			//
			// Les deux autres comportements ont ete essayes et MESURES :
			//   - amorcer depuis la capture -> le joueur heritait du pseudo ET de la progression
			//     d'un autre compte ("gen"), puis cette progression etait persistee comme la sienne ;
			//   - repondre NotFound -> le jeu ne cree rien, envoie quelques deltas et part en boucle
			//     d'erreur de communication.
			// On n'envoie AUCUN message : c'est ce que « reponse 0 o » veut dire. Dans la capture,
			// une reponse normale commence par le CADRE gRPC (00 00 00 00 A7 … pour
			// SelectSeasonSchedules) — Proxide inclut le cadre. Le GetSaveRecord du compte vierge a
			// `content: ""` ET `content-length: 0` : aucun cadre du tout. Un message vide (5 octets de
			// cadre) n'est donc PAS la meme chose.
			//
			// ⚠️ CE QUI MANQUAIT (mesure du 2026-08-12). Zero message avait deja ete essaye ici, et le
			// client rebouclait sur GetSaveRecord sans jamais creer son record. La raison n'etait pas
			// le vide, mais sa FORME : grpc-go, quand le handler sort sans avoir rien envoye, replie
			// toute la reponse dans un unique cadre HEADERS (« Trailers-Only »), alors que Nintendo
			// emet un cadre d'en-tetes PUIS les trailers — verifiable sur les huit reponses vides de
			// une capture du hall et sur celle-ci. finirVideCommeNintendo produit les deux cadres
			// (voir npln_empty_reply.go et son test de fil).
			// ⚠️ « Zero message » ne dit PAS au client que le record n'existe pas.
			//
			// Mesure du 2026-08-13 (12:50 UTC, sauvegarde locale supprimee, tutoriel refait, cloud
			// vide, reponse vide DEJA a deux cadres) : le jeu n'a jamais tente le moindre envoi de la
			// classe d'un Create — son plus gros envoi socket de toute la session fait 5 922 o contre
			// 15 049 pour le CreateSaveRecord de la console. Il a enchaine sur un WriteSaveRecord
			// evt=Validate portant CloudRandomSet, HaveGearClothesMap, HaveGearHeadMap et
			// HaveGearShoesMap : les cles qu'on initialise dans un record EXISTANT et vide. Cet
			// evenement « Validate » n'existe nulle part dans une capture de compte neuf, ou
			// c'est le Create qui remplit ces champs ; la suite (InitFest, InitCoopTeamContest,
			// NewSeason, SeqFlagVsLobbyMulti) est en revanche identique. Notre client court donc deja
			// sur le chemin D'APRES le Create.
			//
			// La raison est connue et mesuree sur CE client, sur un autre appel : sans message, le SDK
			// construit un objet de reponse PAR DEFAUT (commit 6ed6b4d, GetDocument -> « Document par
			// defaut, au nom vide »). Ici cela donne un SaveRecord par defaut = un record qui existe
			// et qui est vide.
			//
			// Le corps de notre reponse est deja identique a celui de Nintendo (zero octet) et le
			// premier cadre porte les memes en-tetes. Le seul champ de la reponse que le client puisse
			// encore lire differemment est le grpc-status des trailers — et il n'est PAS mesure :
			// Proxide ne transcrit aucun trailer (0 sur les 59 echanges de vierge.json et les 43 de
			// hall3.json) et « grpc-status » n'apparait dans aucun en-tete, ce qui exclut par ailleurs
			// la forme Trailers-Only. Voir finirAbsentCommeNintendo.
			log.Printf("[NPLN CloudSave] GetSaveRecord uid=%s -> aucun record dans le cloud", uid)
			if soirFlag("saveabsent") {
				log.Printf("[NPLN CloudSave] GetSaveRecord uid=%s -> NotFound en DEUX cadres (en-tetes puis status)", uid)
				return finirAbsentCommeNintendo(stream, "save record not found")
			}
			if soirFlag("save") {
				return finirVideCommeNintendo(stream)
			}
			return nil
		}
		rec := recordStore.snapshot(uid) // copie : marshaler le pointeur vivant course avec WriteSaveRecord
		log.Printf("[NPLN CloudSave] GetSaveRecord uid=%s -> %d cles, update_time=%s",
			uid, len(rec.GetSaveData().GetFields()), rec.GetUpdateTime().AsTime().Format(time.RFC3339Nano))
		return stream.SendMsg(rec)
	}

	// CreateSaveRecord: the client hands us a COMPLETE SaveRecord built from its local save.
	if strings.Contains(method, "CloudSave/CreateSaveRecord") {
		var creq rawMsg
		var body []byte
		for stream.RecvMsg(&creq) == nil {
			if len(creq.b) > 0 {
				body = append([]byte(nil), creq.b...)
			}
			creq = rawMsg{}
		}

		uid := saveOwner(stream.Context())
		req := &toyohrpb.CreateSaveRecordRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			log.Printf("[NPLN CloudSave] CreateSaveRecord uid=%s: illisible: %v", uid, err)
			return status.Error(codes.InvalidArgument, "bad save record")
		}
		rec := recordStore.create(uid, req.GetSaveRecord())
		log.Printf("[NPLN CloudSave] CreateSaveRecord uid=%s -> %d cles enregistrees", uid, len(rec.GetSaveData().GetFields()))
		return stream.SendMsg(rec)
	}

	// ValidateSaveRecord: the client checks its local save is in step with the cloud one. Never
	// seen in the captures; accept whatever it claims rather than block the boot on a guess.
	if strings.Contains(method, "CloudSave/ValidateSaveRecord") {
		var vreq rawMsg
		for stream.RecvMsg(&vreq) == nil {
			vreq = rawMsg{}
		}
		return stream.SendMsg(&toyohrpb.ValidateSaveRecordResponse{})
	}

	// ChangeUserName rewrites save_data["UserName"] — the field the whole game reads its pseudo from
	// (proven: presence attributes PlayerName, locker documents Player.UserName and the matchmaking
	// session attributes are all denormalised copies the client derives from the SaveRecord).
	if strings.Contains(method, "CloudSave/ChangeUserName") {
		var creq rawMsg
		var body []byte
		for stream.RecvMsg(&creq) == nil {
			if len(creq.b) > 0 {
				body = append([]byte(nil), creq.b...)
			}
			creq = rawMsg{}
		}

		uid := saveOwner(stream.Context())
		req := &toyohrpb.ChangeUserNameRequest{}
		if err := proto.Unmarshal(body, req); err != nil || req.GetUserName() == "" {
			log.Printf("[NPLN CloudSave] ChangeUserName uid=%s: requete illisible: %v", uid, err)
			return stream.SendMsg(&toyohrpb.ChangeUserNameResponse{
				Timestamp1: timestamppb.Now(), Timestamp2: timestamppb.Now(),
			})
		}

		rec := recordStore.setString(uid, "UserName", req.GetUserName())
		log.Printf("[NPLN CloudSave] ChangeUserName uid=%s -> %q", uid, req.GetUserName())

		return stream.SendMsg(&toyohrpb.ChangeUserNameResponse{
			Identifier: saveIdentifier(rec),
			Timestamp1: rec.GetUpdateTime(),
			Timestamp2: rec.GetUpdateTime(),
		})
	}

	// UserScreening/GetViolation: S3 sends "tenants/current/users/current/violations" and
	// parses the response's resource name. Our old empty reply left it BLANK, so S3 parsed
	// "" and hit a hard abort (2162-0001) in nn.npln.Worker (the resource-path parser at
	// 0x7d58a0 requires a trailing "violations" segment). Return the caller's RESOLVED
	// violations resource path so the parse succeeds and the screening check passes.
	if strings.Contains(method, "UserScreening/GetViolation") {
		var vreq rawMsg
		for stream.RecvMsg(&vreq) == nil {
			vreq = rawMsg{}
		}
		uid := capturedUser
		if pid, ok := callerPID(stream.Context()); ok {
			if me, err := accountFriends(pid); err == nil && me.UserID != "" {
				uid = me.UserID
			}
		}
		// [Nextendo] When we serve the CAPTURE's identity everywhere (NPLN_JWT_SUB_CAPTURED),
		// the violations resource MUST use that same user id too — otherwise the game logs in as
		// the capture user but gets a violations path for a DIFFERENT user (moha), the screening
		// object is self-inconsistent, and the post-login state machine parks on the ink loading
		// screen (never advancing to SubscribeFriendUsers / the plaza). Keep the whole session on
		// ONE identity.
		if os.Getenv("NPLN_JWT_SUB_CAPTURED") != "" {
			uid = capturedUser
		}
		// [Nextendo] The path must carry a violation ID after "violations" -- a collection path is
		// rejected. Disassembly of the parser (Thunder.nss:0x7d58a0, called from 0x7d5eac; on
		// failure 0x7d5ec0 branches straight to abort) shows it walks: literal "tenants" (7) / arg /
		// literal "users" (5) / arg / then scans the REMAINDER for a '/' and compares the text
		// before it to "violations" (0xd4b65ab). Reaching the end of the string without finding
		// that '/' leaves the result register zeroed -> parse fails -> 2162-0001 in nn.npln.Worker.
		// So ".../violations" cannot parse; only ".../violations/<id>" can. Using "current" keeps
		// the same magic segment S3 itself sends in the request ("tenants/current/users/current").
		// ⚠️ MESURE QUI CORRIGE CE QUI PRECEDE (2026-08-12) : la capture reelle console <-> Nintendo
		// (une capture du hall, 43 echanges tous servis par « istio-envoy ») montre que S3
		// demande une violation NOMMEE — « tenants/current/users/current/violations/
		// inappropriate_nickname » — et que Nintendo repond SUCCES SANS AUCUN MESSAGE
		// (content-length: 0). Un vide veut dire « pas de violation ». En renvoyant un chemin, nous
		// affirmions au jeu que le joueur EST sanctionne : de quoi le renvoyer du hall vers la place.
		//
		// Le vide seul ne suffisait pas jusqu'ici parce que grpc-go le condensait en Trailers-Only ;
		// finirVideCommeNintendo emet les deux cadres du vrai serveur (voir npln_empty_reply.go et le
		// test de fil qui le verrouille).
		// ⚠️ CE QUE COUTAIT LA FORME NOMMEE, mesure du 2026-08-13 sur un compte niveau 1 :
		// « Tu ne peux pas utiliser ce surnom. Utilise le terminal du hall pour le modifier avant de
		// poursuivre. » Le jeu demande « ai-je une sanction sur mon pseudo ? » et, en lui rendant un
		// chemin de violation, nous lui repondions OUI. Un compte neuf est alors bloque : le jeu le
		// force a jouer une Guerre de territoire, qu'il refuse de lancer tant que le pseudo est
		// sanctionne — donc ni match, ni liste d'amis, ni salon prive. Le meme compte neuf sur une
		// vraie Switch contre Nintendo n'a PAS ce message.
		//
		// Les trois formes, comme pour GetDocument et GetSaveRecord :
		//	nommee            -> le joueur est declare sanctionne (ci-dessus) ;
		//	OK + zero message -> le SDK construit un objet par defaut au nom VIDE, et l'analyseur de
		//	                     chemin (Thunder.nss:0x7d58a0, abort en 0x7d5ec0) s'abat dessus ;
		//	NOT_FOUND, DEUX cadres -> « aucune violation », ce qui est la verite.
		//
		// C'est la meme lecon que sur les deux autres services : la capture montre 0 octet, mais
		// Proxide ne transcrit AUCUN trailer, donc son statut n'a jamais ete mesure.
		if soirFlag("violationnommee") {
			path := npnTenant + "/users/" + uid + "/violations/current"
			resp := append([]byte{0x0a, byte(len(path))}, []byte(path)...) // {1: name}
			log.Printf("[NPLN] UserScreening/GetViolation -> %s (forme heritee, declare le joueur SANCTIONNE)", path)
			return stream.SendMsg(&rawMsg{b: resp})
		}

		log.Printf("[NPLN] UserScreening/GetViolation (%s) -> NotFound en DEUX cadres (aucune violation)", uid)

		return finirAbsentCommeNintendo(stream, "violation not found")
	}

	// drain the request message(s) so the client's send side completes
	//
	// On RETIENT la premiere : elle porte, pour les appels groupes, la liste des joueurs dont le jeu
	// veut les documents. Nous la jetions, donc nous ne pouvions pas savoir a qui repondre.
	var req rawMsg
	var premiereRequete []byte
	for stream.RecvMsg(&req) == nil {
		if premiereRequete == nil && len(req.b) > 0 {
			premiereRequete = append([]byte(nil), req.b...)
		}
		req = rawMsg{}
	}

	data, err := capturedBoot.ReadFile(methodToFile(method))
	if err != nil {
		// no captured response (e.g. UserScreening/GetViolation = empty) -> send one
		// empty message + OK so the unary caller gets a valid (empty) reply.
		log.Printf("[NPLN replay] %s -> empty OK (no capture)", method)
		return stream.SendMsg(&rawMsg{b: []byte{}})
	}

	// [Nextendo] Rewrite the capture user id -> this session's live user id so replayed bodies
	// (SubscribePresences, SubscribeFriendUsers, CloudSave, Ugcstore, AllocateIceServerSet…) all
	// name THE SAME user the client logged in as. This is what lets the SDK find its OWN UserSession
	// in the presence collection and leave the ink loading screen for the plaza.
	if live := liveReplayUID(stream); live != capturedUser {
		before := bytes.Count(data, []byte(capturedUser))
		data = rewriteCapturedIdentity(data, live)
		if before > 0 {
			log.Printf("[NPLN replay] %s -> rewrote %d capture-uid -> %s", method, before, live)
		}
	}

	// Certaines captures viennent de la session du COMPTE VIERGE (le casier, notamment) : leur
	// identifiant n'est pas celui de la capture historique, et il doit etre realigne lui aussi,
	// sans quoi le joueur lit un document qui nomme quelqu'un d'autre.
	if live := liveReplayUID(stream); live != "" && live != captureVierge && len(live) == len(captureVierge) {
		if n := bytes.Count(data, []byte(captureVierge)); n > 0 {
			data = bytes.ReplaceAll(data, []byte(captureVierge), []byte(live))
			log.Printf("[NPLN replay] %s -> realigne %d identifiant(s) du compte vierge -> %s", method, n, live)
		}
	}

	// PUBLICATION D'UN DESSIN. Trois appels, mesures sur une vraie console le 2026-08-15 :
	// une permission de depot, le document, puis l'enregistrement. Sans eux, le jeu n'a aucune URL
	// ou televerser et rend une erreur de communication a la boite aux lettres.
	if strings.Contains(method, "Ugcstore/IssueUploadUri") {
		pid, _ := callerPID(stream.Context())
		corps := reponseIssueUploadUri(pid)
		log.Printf("[NPLN UGC] IssueUploadUri pid=%d -> permission de depot delivree (%d o)", pid, len(corps))
		return stream.SendMsg(&rawMsg{b: corps})
	}
	if strings.Contains(method, "Ugcstore/CommitDocuments") {
		nom := nomDocumentDansCommit(premiereRequete)
		retenirDessin(nom, liveReplayUID(stream), premiereRequete)
		corps := reponseCommitDocuments(1)
		log.Printf("[NPLN UGC] CommitDocuments %q -> %d o de resultat", nom, len(corps))
		return stream.SendMsg(&rawMsg{b: corps})
	}
	if strings.Contains(method, "Canola/RegisterDocument") {
		// Reponse VIDE chez Nintendo (0 octet) : c'est l'acquittement de publication.
		log.Printf("[NPLN UGC] RegisterDocument -> publie (reponse vide, comme la capture)")
		return stream.SendMsg(&rawMsg{b: []byte{}})
	}

	// LES PLAQUES DE JOUEUR DE LA LISTE D'AMIS.
	//
	// Mesure du 2026-08-15, capture d'une vraie console : quand le jeu affiche des joueurs, il appelle
	// Locker/SelectDocuments (et Canola/SelectDocuments) en passant la LISTE DES IDENTIFIANTS qui
	// l'interessent, et Nintendo lui rend leurs documents — qui portent NamePlate, Byname et Badge1-3,
	// c'est-a-dire l'icone affichee a cote du pseudo.
	//
	// Nous rendions la reponse capturee telle quelle : elle nomme les joueurs de la capture, pas les
	// amis demandes. Le jeu ne trouvait donc de plaque pour aucun d'eux et affichait « ? » a la place,
	// en ligne comme hors ligne.
	//
	// On realigne : les identifiants presents dans la capture sont remplaces par ceux effectivement
	// demandes. Tous font 22 caracteres, la substitution est donc a longueur constante et ne touche
	// pas au cadrage protobuf — meme technique que la reecriture de l'identite ci-dessus.
	// LA PLACE PUBLIQUE, AVEC NOS PROPRES JOUEURS.
	//
	// On rejouait le casier CAPTURE, le meme pour tout le monde, 327 fois par jour. Nos
	// sauvegardes portent les vrais : 905 sur 905 ont un LockerInfo, et 37 joueurs ont
	// accepte de le publier via la question que le jeu leur pose lui-meme
	// que si le joueur a accepte de le montrer. Voir place_publique.go.
	for _, coll := range []struct {
		methode string
		quoi    string
		lire    func() []*ugcpb.Document
	}{
		{"Locker/SelectDocuments", "casier(s)", casiersPublics},
		{"Canola/SelectDocuments", "jeu(x) de cartes", jeuxDeCartesPublics},
	} {
		if !strings.Contains(method, coll.methode) {
			continue
		}
		if docs := coll.lire(); len(docs) > 0 {
			log.Printf("[NPLN place] %s -> %d %s de joueurs Nextendo", method, len(docs), coll.quoi)
			return stream.SendMsg(&toyohrpb.SelectDocumentsResponse{Documents: docs})
		}
	}

	if strings.Contains(method, "/SelectDocuments") {
		if demandes := uidsDemandes(premiereRequete); len(demandes) > 0 {
			avant := len(uidsPresents(data))
			data = realignerUids(data, demandes)
			log.Printf("[NPLN replay] %s -> %d identifiant(s) de la capture realigne(s) sur %d ami(s) demande(s)",
				method, avant, len(demandes))
		}
	}

	// split the captured DATA into gRPC messages: [1 flag][4 len BE][body]...
	n := 0
	var lastFrame []byte
	for len(data) >= 5 {
		mlen := int(binary.BigEndian.Uint32(data[1:5]))
		if 5+mlen > len(data) {
			break
		}
		body := data[5 : 5+mlen]
		if e := stream.SendMsg(&rawMsg{b: append([]byte(nil), body...)}); e != nil {
			return e
		}
		lastFrame = append([]byte(nil), body...)
		n++
		data = data[5+mlen:]
	}
	// Server-streaming RPCs (the lobby/notification feeds) must stay OPEN: the Switch
	// keeps them alive for the whole session and treats a premature close as "lost the
	// lobby", which leaves S3 stuck on the loading screen. After replaying the captured
	// messages, hold the stream open until the client disconnects instead of returning.
	if isServerStreamingMethod(method) {
		// Only a SMALL trailing frame is a heartbeat. Every captured Subscribe* stream ends with
		// one (2-10 o), but guard the size anyway: repeating a big trailing frame would spam real
		// content at S3 forever instead of a keepalive.
		if len(lastFrame) > 16 {
			lastFrame = nil
		}
		if len(lastFrame) == 0 {
			log.Printf("[NPLN replay] %s -> %d captured msg(s), holding stream OPEN (aucun heartbeat a repeter)", method, n)
			<-stream.Context().Done()
			return nil
		}
		// Every captured stream ENDS with a small heartbeat frame that Nintendo repeats for the
		// whole session, and those frames carry the contract explicitly: SubscribeFriendUsers
		// sends keep_alive_interval=50s, SubscribePresences sends {30s ping, 50s deadline}.
		// Replaying once and then sitting silent made S3 hit the 50s deadline and re-subscribe
		// every ~52s, so the streams never stayed established. Repeat the last frame well inside
		// the interval to keep them alive.
		log.Printf("[NPLN replay] %s -> %d captured msg(s), stream OPEN + heartbeat %d o toutes les %s",
			method, n, len(lastFrame), nplnStreamHeartbeat)
		ticker := time.NewTicker(nplnStreamHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-stream.Context().Done():
				return nil
			case <-ticker.C:
				if err := stream.SendMsg(&rawMsg{b: lastFrame}); err != nil {
					return nil // client went away
				}
			}
		}
	}
	log.Printf("[NPLN replay] %s -> %d captured msg(s)", method, n)
	return nil
}

// parseInitializeAttributes pulls the two leading string fields out of an
// InitializeAttributesRequest without generated code: field 1 = user, field 2 = initialize_type
// (proto/toyohr/v1/game_record.proto). Anything else in the message is skipped by wire type.
func parseInitializeAttributes(b []byte) (user, initType string) {
	for i := 0; i < len(b); {
		key, n := binary.Uvarint(b[i:])
		if n <= 0 {
			return
		}
		i += n

		field, wire := key>>3, key&7
		switch wire {
		case 2: // length-delimited
			l, n := binary.Uvarint(b[i:])
			if n <= 0 || i+n+int(l) > len(b) {
				return
			}
			i += n
			val := string(b[i : i+int(l)])
			i += int(l)
			switch field {
			case 1:
				user = val
			case 2:
				initType = val
			}
		case 0: // varint
			_, n := binary.Uvarint(b[i:])
			if n <= 0 {
				return
			}
			i += n
		case 1:
			i += 8
		case 5:
			i += 4
		default:
			return
		}
	}
	return
}

// initAttrDiag throttles the one-off dump of the InitializeAttributes request.
var initAttrDiag int

// [Nextendo] Per-user cloud saves, persisted to disk so they survive a server restart (the
// in-memory-only variant would have re-lost the player's name on every redeploy, which is the very
// bug this fixes). One file per user under /data (the container's mount) or NPLN_SAVE_DIR.
type cloudSaveStore struct {
	mu  sync.RWMutex
	mem map[string][]byte
}

var saveStore = &cloudSaveStore{mem: map[string][]byte{}}

func saveDir() string {
	if v := os.Getenv("NPLN_SAVE_DIR"); v != "" {
		return v
	}
	return "/data/saves"
}

func savePath(uid string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, uid)

	return filepath.Join(saveDir(), safe+".save")
}

// saveOwner resolves whose save this is. Falls back to the capture identity so a session whose uid
// metadata is missing still gets a stable slot instead of scribbling over a shared one.
func saveOwner(ctx context.Context) string {
	// L'identite du proprietaire vient du JETON VERIFIE, pas de ce que le client declare.
	//
	// On lisait l'uid des metadonnees de la requete — une valeur que le client choisit. N'importe
	// qui pouvait donc reclamer l'uid d'un autre et lire ou ecrire SA sauvegarde (niveau, argent,
	// progression). callerPID, lui, verifie la signature ES256 du jeton d'acces avant d'en tirer le
	// PID : c'est la meme preuve que celle deja exigee pour les amis et la presence.
	if pid, ok := callerPID(ctx); ok {
		if acc, err := accountFriends(pid); err == nil && acc.UserID != "" {
			return acc.UserID
		}
	}

	// Pas de jeton exploitable : on retombe sur l'uid declare. L'ancien repli renvoyait
	// capturedUser pour TOUT LE MONDE, donc deux joueurs non identifies ecrivaient dans le MEME
	// fichier de sauvegarde.
	if uid := uidFromCtx(ctx); uid != "" {
		return uid
	}
	return capturedUser
}

func (s *cloudSaveStore) put(uid string, blob []byte) {
	s.mu.Lock()
	s.mem[uid] = blob
	s.mu.Unlock()

	if err := os.MkdirAll(saveDir(), 0o755); err != nil {
		log.Printf("[NPLN CloudSave] mkdir %s: %v", saveDir(), err)
		return
	}
	if err := os.WriteFile(savePath(uid), blob, 0o644); err != nil {
		log.Printf("[NPLN CloudSave] ecriture %s: %v", savePath(uid), err)
	}

	// Keep every delta as its own file too: the client sends partial records, and the only way to
	// learn that format is to collect a series of them alongside what the game displays.
	seq := filepath.Join(saveDir(), "deltas")
	if os.MkdirAll(seq, 0o755) == nil {
		_ = os.WriteFile(filepath.Join(seq, fmt.Sprintf("%s-%d.bin", uid, len(blob))), blob, 0o644)
	}
}

func (s *cloudSaveStore) get(uid string) ([]byte, bool) {
	s.mu.RLock()
	blob, ok := s.mem[uid]
	s.mu.RUnlock()
	if ok {
		return blob, true
	}

	blob, err := os.ReadFile(savePath(uid))
	if err != nil || len(blob) == 0 {
		return nil, false
	}

	s.mu.Lock()
	s.mem[uid] = blob
	s.mu.Unlock()

	return blob, true
}

// alignerCompteVierge remplace l'identifiant du compte vierge de la capture par celui du joueur.
// Les deux ont la meme forme (« u- » suivi de 20 caracteres), mais on reserialise quand meme par
// substitution directe : le champ apparait dans les attributs ET dans le nom du document, et les
// deux doivent designer le meme joueur, sans quoi le jeu initialise un document qu'il ne relira
// jamais.
func alignerCompteVierge(capture []byte, uid string) []byte {
	out := append([]byte(nil), capture...)
	if uid == "" || uid == captureVierge {
		return out
	}
	if len(uid) != len(captureVierge) {
		// Longueur differente : on reserialise proprement plutot que de casser les prefixes de
		// longueur du protobuf.
		log.Printf("[NPLN GameRecord] uid %q de longueur inattendue, capture laissee telle quelle", uid)
		return out
	}
	return bytes.ReplaceAll(out, []byte(captureVierge), []byte(uid))
}
