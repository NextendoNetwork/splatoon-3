package main

// Connection-level tracing. The TCP+TLS to us succeeds but S3 never dispatches a
// gRPC method (neither the interceptor nor the replay handler fires). This stats
// handler logs the gRPC CONNECTION lifecycle (h2 conn begin/end) and any RPC stat,
// so we can tell whether S3 reaches the gRPC/h2 layer at all — or only TCP/TLS.

import (
	"context"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// dashCtxKey carries the caller identity resolved once at TagRPC time, so the per-message
// callbacks can refresh liveness without re-parsing the token on every frame.
type dashCtxKey struct{}

type dashCaller struct {
	uid string
	pid uint64
	ip  string
}

// suiviConnexions relie chaque connexion HTTP/2 au joueur qui parle dessus, pour pouvoir le
// retirer du tableau de bord DES QUE le jeu se ferme, sans attendre la duree de grace de 90 s.
//
// Le probleme observe : a la fermeture du jeu, le joueur restait affiche « en ligne » pendant une
// minute et demie. La duree de grace existe pour une bonne raison (un battement de presence manque
// ne doit pas faire disparaitre quelqu'un qui joue), mais elle n'a aucune raison de s'appliquer
// quand on SAIT que le client est parti : la fin de sa connexion h2 est un signal certain.
//
// On compte les connexions par joueur : S3 en ouvre plusieurs, et il ne faut le declarer parti
// qu'a la fermeture de la derniere.
type cleConnexion struct{}

type connexion struct {
	id uint64
}

var suiviConnexions = struct {
	sync.Mutex
	uidDe    map[uint64]string // connexion -> joueur
	ouvertes map[string]int    // joueur -> connexions ouvertes en ce moment
	cumul    map[string]int    // joueur -> connexions ouvertes DEPUIS LE DEMARRAGE
	seq      uint64
}{uidDe: map[uint64]string{}, ouvertes: map[string]int{}, cumul: map[string]int{}}

// noterConnexionDuJoueur associe la connexion courante a un joueur (idempotent).
func noterConnexionDuJoueur(ctx context.Context, uid string) {
	c, ok := ctx.Value(cleConnexion{}).(*connexion)
	if !ok || uid == "" {
		return
	}
	suiviConnexions.Lock()
	defer suiviConnexions.Unlock()
	if suiviConnexions.uidDe[c.id] == uid {
		return
	}
	suiviConnexions.uidDe[c.id] = uid
	suiviConnexions.ouvertes[uid]++
	suiviConnexions.cumul[uid]++

	// COMPTER LES RECONNEXIONS PAR JOUEUR.
	//
	// Mesure du 2026-08-22 : 44 connexions ouvertes et 45 fermees en quinze minutes pour 21
	// joueurs — un renouvellement permanent. Et le journal d'un joueur en echec montrait SEIZE
	// connexions la ou une session qui tient n'en ouvre que cinq.
	//
	// Impossible jusqu'ici de savoir QUI rouvre : toutes les connexions arrivent par 10.0.1.7,
	// l'adresse du proxy Docker, et le port source n'etait rattache a aucun joueur. On le note ici,
	// au seul endroit ou la connexion et le joueur se rencontrent.
	debugLogf("[stats] connexion #%d pour %s — %d ouverte(s) maintenant, %d depuis le demarrage",
		c.id, short(uid), suiviConnexions.ouvertes[uid], suiviConnexions.cumul[uid])
}

// fermerConnexion rend le joueur dont c'etait la DERNIERE connexion, sinon "".
func fermerConnexion(ctx context.Context) string {
	c, ok := ctx.Value(cleConnexion{}).(*connexion)
	if !ok {
		return ""
	}
	suiviConnexions.Lock()
	defer suiviConnexions.Unlock()
	uid := suiviConnexions.uidDe[c.id]
	delete(suiviConnexions.uidDe, c.id)
	if uid == "" {
		return ""
	}
	suiviConnexions.ouvertes[uid]--
	if suiviConnexions.ouvertes[uid] > 0 {
		return ""
	}
	delete(suiviConnexions.ouvertes, uid)
	return uid
}

type connTracer struct{}

// cleMethode retient le nom de l'appel jusqu'a sa fin : stats.End ne le porte pas, or c'est
// justement la qu'on apprend combien de temps il a pris.
type cleMethode struct{}

// seuilRPCLent : au-dela, l'appel est journalise a part. Le jeu, lui, renonce vers vingt-cinq
// secondes et affiche « erreur de communication » — on veut donc voir venir bien avant.
const seuilRPCLent = 2 * time.Second

type rpcTraceKey struct{}

type rpcTrace struct {
	id            uint64
	requests      atomic.Int64
	responses     atomic.Int64
	requestBytes  atomic.Int64
	responseBytes atomic.Int64
}

var rpcTraceSequence atomic.Uint64

func traceConnectionID(ctx context.Context) uint64 {
	if c, ok := ctx.Value(cleConnexion{}).(*connexion); ok {
		return c.id
	}
	return 0
}

func (connTracer) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	trace := &rpcTrace{id: rpcTraceSequence.Add(1)}
	ctx = context.WithValue(ctx, rpcTraceKey{}, trace)
	debugLogf("[stats] RPC arrival conn=%d rpc=%d method=%s", traceConnectionID(ctx), trace.id, info.FullMethodName)
	ctx = context.WithValue(ctx, cleMethode{}, info.FullMethodName)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		uid, pid, ip := dashIdentityAndIP(md, "")
		if uid != "" {
			return context.WithValue(ctx, dashCtxKey{}, dashCaller{uid: uid, pid: pid, ip: ip})
		}
	}
	return ctx
}

func (connTracer) HandleRPC(ctx context.Context, s stats.RPCStats) {
	method, _ := ctx.Value(cleMethode{}).(string)
	trace, _ := ctx.Value(rpcTraceKey{}).(*rpcTrace)
	if trace != nil {
		switch v := s.(type) {
		case *stats.Begin:
			debugLogf("[stats] RPC start conn=%d rpc=%d method=%s client_stream=%t server_stream=%t", traceConnectionID(ctx), trace.id, method, v.IsClientStream, v.IsServerStream)
		case *stats.InPayload:
			trace.requests.Add(1)
			trace.requestBytes.Add(int64(v.Length))
			debugLogf("[stats] RPC request conn=%d rpc=%d method=%s bytes=%d wire_bytes=%d", traceConnectionID(ctx), trace.id, method, v.Length, v.WireLength)
			if req, ok := v.Payload.(interface{ GetCurrentTime() *timestamppb.Timestamp }); ok {
				ts := req.GetCurrentTime()
				if ts != nil && ts.CheckValid() == nil {
					now := time.Now().UTC()
					debugLogf("[NPLN clock] rpc=%d method=%s client_time=%s server_time=%s client_minus_server=%s", trace.id, method, ts.AsTime().Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), ts.AsTime().Sub(now))
				} else {
					debugLogf("[NPLN clock] rpc=%d method=%s current_time_missing_or_invalid", trace.id, method)
				}
			}
		case *stats.OutPayload:
			trace.responses.Add(1)
			trace.responseBytes.Add(int64(v.Length))
			debugLogf("[stats] RPC response conn=%d rpc=%d method=%s bytes=%d wire_bytes=%d", traceConnectionID(ctx), trace.id, method, v.Length, v.WireLength)
		case *stats.End:
			debugLogf("[stats] RPC finish conn=%d rpc=%d method=%s status=%s requests=%d request_bytes=%d responses=%d response_bytes=%d duration=%s context_err=%v context_cause=%v error=%v", traceConnectionID(ctx), trace.id, method, status.Code(v.Error), trace.requests.Load(), trace.requestBytes.Load(), trace.responses.Load(), trace.responseBytes.Load(), v.EndTime.Sub(v.BeginTime), ctx.Err(), context.Cause(ctx), v.Error)
		}
	}
	switch v := s.(type) {
	case *stats.InHeader:
		debugLogf("[stats] RPC InHeader method=%s remote=%v", v.FullMethod, v.RemoteAddr)
		// Single accounting point for the monitoring dashboard: InHeader carries the request
		// metadata directly, so it sees EVERY call — the typed services, the replay handler and
		// the long-lived streams alike — without touching any handler.
		peer := ""
		if v.RemoteAddr != nil {
			peer = v.RemoteAddr.String()
		}
		uid, pid, ip := dashIdentityAndIP(v.Header, peer)
		dashNoteRPC(v.FullMethod, uid, pid, ip)
		noterConnexionDuJoueur(ctx, uid)
	case *stats.InPayload, *stats.OutPayload:
		// S3 holds long-lived streams (PresenceService/KeepAlive, LobbyMessaging/RecvMessage) and
		// talks over them for the whole session. InHeader fires ONCE per stream, so counting only
		// headers made a settled player look idle and, after the TTL, vanish from the dashboard
		// while still online. Every frame on an open stream is activity: refresh on it.
		if c, ok := ctx.Value(dashCtxKey{}).(dashCaller); ok {
			dashTouch(c.uid, c.pid, c.ip)
		}
	case *stats.End:
		methode, _ := ctx.Value(cleMethode{}).(string)
		duree := v.EndTime.Sub(v.BeginTime)
		debugLogf("[stats] RPC End method=%s duree=%s err=%v", methode, duree.Round(time.Millisecond), v.Error)
		// Les flux vivent toute la session : leur duree ne dit rien. Ce qu'on traque, ce sont les
		// appels ordinaires qui trainent, parce que c'est ce qui fait renoncer le jeu.
		if duree >= seuilRPCLent && !fluxLong(methode) {
			uid := ""
			if c, ok := ctx.Value(dashCtxKey{}).(dashCaller); ok {
				uid = c.uid
			}
			log.Printf("[NPLN lent] %s a mis %s (uid=%s) — le jeu renonce vers 25 s",
				methode, duree.Round(time.Millisecond), uid)
		}
	}
}

func (connTracer) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {

	suiviConnexions.Lock()
	suiviConnexions.seq++
	id := suiviConnexions.seq
	suiviConnexions.Unlock()
	debugLogf("[stats] CONN tag conn=%d remote=%v local=%v", id, info.RemoteAddr, info.LocalAddr)
	return context.WithValue(ctx, cleConnexion{}, &connexion{id: id})
}

func (connTracer) HandleConn(ctx context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		debugLogf("[stats] CONN begin conn=%d (h2 established)", traceConnectionID(ctx))
	case *stats.ConnEnd:
		debugLogf("[stats] CONN end conn=%d context_err=%v context_cause=%v", traceConnectionID(ctx), ctx.Err(), context.Cause(ctx))
		if uid := fermerConnexion(ctx); uid != "" {
			dashJoueurParti(uid)
		}
	}
}

// fluxLong reconnait les appels qui restent ouverts toute la session : leur duree est normale et
// n'a rien a voir avec une lenteur.
//
// ⚠️ LA PREMIERE VERSION ENUMERAIT DES NOMS A LA MAIN, et elle laissait passer la moitie des flux :
// SubscribePresences et SubscribeFriendUsers n'y figuraient pas, si bien que le journal les
// signalait comme lents a chaque fermeture de session — 1 min 7 s pour une souscription de
// presence, ce qui est sa duree de vie normale. Ce bruit tombe au pire moment, quand on cherche un
// vrai blocage.
//
// Le module en portait deja la reponse : isServerStreamingMethod reconnait les flux d'abonnement
// par leur forme (RecvMessage, Subscribe, Watch, GetEvent, Stream) et isClientHeartbeat le
// battement de presence. On s'appuie dessus plutot que de tenir une liste qui vieillit. S'y ajoute
// « Track », les tickets de matchmaking, qui restent ouverts tant que la file tourne.
func fluxLong(methode string) bool {
	return isServerStreamingMethod(methode) ||
		isClientHeartbeat(methode) ||
		strings.Contains(methode, "Track")
}
