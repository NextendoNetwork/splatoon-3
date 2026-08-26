// Nextendo NPLN server — a private gRPC implementation of Nintendo's NPLN online
// platform, the stack Splatoon 3 (tenant dce9377b) and other newer Switch titles
// use instead of NEX. Transport is gRPC over HTTP/2+TLS; the client sends
// `authorization: bearer <token>`, `npln-tenant-id` and `uid` metadata.
//
// This first cut registers the Auth service (the entry point). Other services
// (Friends, Datastore/hydro, Matchmaking, ...) get added as we capture what S3
// actually calls, using the kinnay protobufs already compiled into ./proto.
package main

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	authpb "npln.nintendo.net/npln-practice/proto/auth/v1"
	friendspb "npln.nintendo.net/npln-practice/proto/friends/v1"
	gspb "npln.nintendo.net/npln-practice/proto/gamesync/v1"
	mmpb "npln.nintendo.net/npln-practice/proto/matchmaking/v1"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// logMetadata logs the NPLN auth metadata (tenant/uid/authorization) of each call —
// invaluable for reversing which services S3 hits and with what identity.
func logMetadata(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)
	get := func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	auth := get("authorization")
	if len(auth) > 24 {
		auth = auth[:24] + "…"
	}
	log.Printf("[NPLN RPC] %s tenant=%q uid=%q auth=%q", method, get("npln-tenant-id"), get("uid"), auth)
}

// traceInterceptor logs every incoming RPC (method + metadata) so we can see the
// exact call sequence S3 makes.
func traceInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	logMetadata(ctx, info.FullMethod)
	resp, err := handler(ctx, req)
	if err != nil {
		log.Printf("[NPLN RPC] %s -> ERROR %v", info.FullMethod, err)
	}
	return resp, err
}

// buildServer creates a gRPC server with every NPLN service + our hybrid codec,
// replay handler and tracing. creds == nil yields a PLAINTEXT (h2c) server (used for
// the Traefik-terminated no-SNI route); otherwise it's TLS.
func buildServer(creds credentials.TransportCredentials) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(chaineUnaire(typeGrpcUnaire, traceInterceptor)),
		// npln-grpc-type sur CHAQUE reponse, comme la passerelle de Nintendo (voir npln_grpc_type.go).
		grpc.StreamInterceptor(typeGrpcFlux),
		grpc.ForceServerCodec(newHybridCodec()),
		grpc.UnknownServiceHandler(replayHandler),
		grpc.StatsHandler(connTracer{}),
		// S3's native NPLN client (grpc-core) sends HTTP/2 keepalive pings often, and also
		// while no RPC is in flight — it holds long-lived streams (LobbyMessaging, Presence)
		// and pings to keep the NAT mapping alive. grpc-go's DEFAULT enforcement is hostile
		// to that (MinTime 5min, PermitWithoutStream false): it counts the pings as abuse and
		// after two strikes sends GOAWAY(ENHANCE_YOUR_CALM) and tears the connection down.
		// That killed every in-flight RPC — the schedules, the save and ActivateUser all died
		// mid-call (npl1 reported 2321-4992 for each) — so the game never received a rotation
		// and fell back to "Stage information is not available offline".
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	s := grpc.NewServer(opts...)
	authpb.RegisterAuthServer(s, &authServer{})
	// Présence DYNAMIQUE : sans ce service, le rejeu servait la présence des amis du compte de
	// capture, donc deux joueurs Nextendo ne pouvaient jamais se voir en jeu.
	friendspb.RegisterPresenceServiceServer(s, &presenceServer{})
	// ALWAYS register the dynamic Nextendo friend server. It matches the logged-in identity, so the
	// game's UserContext resolves its friend/block snapshot; the captured replay references the CAPTURE
	// user and stalls — for MPJ that stall parks the session-setup fiber (block-list never "ready"),
	// giving "0/4" with no member icon and blocking room-code/invite/end-session. (NPLN_REPLAY_FRIENDS
	// was a diagnostic override that got stuck ON in the container; it is intentionally ignored now.)
	friendspb.RegisterFriendsServer(s, &friendsServer{}) // dynamic friend list (precedence over replay)
	mmpb.RegisterMatchmakerServer(s, newMatchmaker())
	// ⚠️ GetDocument reste UNAIRE et repond NotFound pour un document absent. Les deux façons
	// d'imiter le « Succeeded / content-length: 0 » de la capture ont ete essayees et FONT
	// CRASHER S3 (2162-0001 a 72 s de boot, thread nn.npln.Worker) : un Document vide comme un
	// message de longueur nulle, et un flux termine en OK sans aucun message. Voir
	// ugcstore_absent.go, garde comme trace de la mesure — ma lecture de la capture est donc
	// fausse quelque part, et tant qu'elle n'est pas comprise le NotFound reste le seul
	// comportement avec lequel le jeu demarre.
	// Relire les documents ecrits par les joueurs avant le dernier redemarrage : sans ca, un jeu qui
	// relit un document qu'il a lui-meme cree recoit du vide et s'abat (voir ugcstore.go).
	chargerDocuments()
	registerUgcstore(s, &ugcstoreServer{})
	mmpb.RegisterGameSessionServiceServer(s, newGameSessionServer())
	gspb.RegisterGamesyncServer(s, newGamesyncServer()) // session transport: IssueToken + KeepUserSession (the :7575 gs.nintendo.net endpoint)
	toyohrpb.RegisterScheduleServer(s, &scheduleServer{})
	// FestService passe par notre propre enregistrement : il lui manque SelectFestBreakingNews,
	// que le vrai serveur honore pendant une fete (voir fest_breaking_news.go).
	var fest toyohrpb.FestServiceServer = &festServer{}
	registerFestService(s, &fest)
	return s
}

func main() {
	addr := envOr("NPLN_LISTEN", ":7443")
	certFile := envOr("CERT_FILE", `C:\Dev\Dev\reverse eden\server\certs\local_server_cert.pem`)
	keyFile := envOr("KEY_FILE", `C:\Dev\Dev\reverse eden\server\certs\local_server_key.pem`)

	// MODE LOCAL (test S3 sans Traefik, DNS→127.0.0.1) : tout gRPC+REST sur un seul :443 démuxé ALPN.
	if os.Getenv("NPLN_LOCAL") != "" {
		log.Printf("Nextendo NPLN server — MODE LOCAL (DNS→127.0.0.1), gRPC+REST combinés")
		startLocalCombined(envOr("NPLN_LOCAL_ADDR", ":443"), certFile, keyFile)
		return
	}

	// The auth capture proved NPLN does NOT use mutual TLS (the console never presents
	// a client cert — auth is token-based via DeviceAuthorization/BAAS). So we don't
	// request a client cert (matching real Nintendo). We still load the cert as a
	// chain (leaf+CA) explicitly.
	tlsCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("load TLS cert: %v", err)
	}
	// avecJournalDesCles : voir tls_cles_journal.go — eteint par defaut, allumable
	// a chaud par le drapeau « clestls » pour dechiffrer une capture de paquets.
	creds := credentials.NewTLS(avecJournalDesCles(&tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"h2"},
		MinVersion:   tls.VersionTLS12,
		// [Nextendo-diag] Journaliser le SNI de chaque ClientHello.
		//
		// Splatoon 3 porte une URL GameSync CODEE EN DUR dans son rodata (symbole « GsUrlString »,
		// 27 octets) : « gamesync.npln.nintendo.net ». Le jeu joint
		// donc l'hote de session en presentant CE nom-la, pas celui du locataire. Or notre
		// certificat ne le couvre pas : ses SAN vont jusqu'a « *.npln.SRV.nintendo.net » et
		// « *.nintendo.net » — et un joker ne vaut que pour UN label, donc aucun des deux ne couvre
		// « gamesync.npln.nintendo.net ». Avant de toucher au certificat, on MESURE le nom demande.
		GetConfigForClient: func(hi *tls.ClientHelloInfo) (*tls.Config, error) {
			log.Printf("[NPLN TLS] ClientHello sni=%q depuis %v alpn=%v", hi.ServerName, hi.Conn.RemoteAddr(), hi.SupportedProtos)
			return nil, nil
		},
	}))

	// [Nextendo-diag] Compter les trames HTTP/2 sur le flux DECHIFFRE (drapeau a chaud « frames »).
	// Sans cela on ne connait que les TAILLES des envois cote emulateur, jamais leur nature — et le
	// hall repete un triplet 59/89/102 octets des milliers de fois sans qu'aucun appel applicatif
	// n'atteigne le serveur. Voir frames_diag.go.
	creds = tracerLesTrames(creds)

	// GAMESYNC-ONLY mode (runs on the HOST, bound to :7575): the client's Pia/NPLN session
	// transport connects DIRECTLY to GameSession.Host:Port (203.0.113.7:7575, SNI
	// gs.nintendo.net) — NOT through Traefik — and calls nn.npln.gamesync.v1.Gamesync. This
	// mode serves the full gRPC server (incl. Gamesync) over TLS on 7575 only, so it can run
	// alongside the container that serves :443.
	if os.Getenv("NPLN_GAMESYNC_ONLY") != "" {
		addr := envOr("NPLN_GAMESYNC_LISTEN", ":7575")
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("gamesync listen %s: %v", addr, err)
		}
		// Own dashboard port: this is a SECOND process (systemd on the host) with its own
		// counters, so it must not fight the container's listener.
		go startDashboard(envOr("NPLN_DASH_LISTEN", ":8090"), os.Getenv("DASH_TOKEN"))
		log.Printf("Nextendo NPLN GAMESYNC server listening on %s (gRPC/TLS, SNI gs.nintendo.net) — session transport", addr)
		log.Fatal(buildServer(creds).Serve(lis))
	}

	s := buildServer(creds) // TLS on :443 (SNI passthrough connections)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	// Monitoring: /api/stats for nexdash, in the same shape the NEX games publish.
	go startDashboard(envOr("NPLN_DASH_LISTEN", ":8089"), os.Getenv("DASH_TOKEN"))

	// REST entry point (vermillion gateway + penne) — what S3 actually hits FIRST, in JSON,
	// captured in captures/s3/. Runs alongside the gRPC server (matchmaking, not yet captured).
	go startHTTPGateway(envOr("NPLN_HTTP_LISTEN", ":7444"), certFile, keyFile)

	log.Printf("Nextendo NPLN server listening on %s (gRPC/TLS) — Splatoon 3 tenant dce9377b", addr)
	log.Printf("registered gRPC services: %s", strings.Join([]string{
		"nn.npln.auth.v1.Auth",
		"nn.npln.friends.v1.Friends",
		"nn.npln.matchmaking.v1.Matchmaker",
		"nn.npln.matchmaking.v1.GameSessionService",
		"nn.npln.toyohr.v1.Schedule",
		"nn.npln.toyohr.v1.FestService",
	}, ", "))

	// h2c (plaintext gRPC) listener for the NO-SNI case. Ryujinx's S3 opens the NPLN
	// TLS connection WITHOUT an SNI (its bundled OpenSSL doesn't emit one under emulation),
	// so Traefik can't passthrough-route it (passthrough routes by SNI). Instead Traefik
	// TLS-terminates the no-SNI connection and forwards the decrypted HTTP/2 here. Same
	// gRPC server, just without our TLS layer (Traefik already did TLS with S3).
	go func() {
		h2cLis, e := net.Listen("tcp", envOr("NPLN_H2C_LISTEN", ":8080"))
		if e != nil {
			log.Printf("h2c listen: %v", e)
			return
		}
		log.Printf("NPLN h2c (plaintext gRPC) listening on %s — for Traefik-terminated no-SNI route", h2cLis.Addr())
		if e := buildServer(nil).Serve(h2cLis); e != nil { // nil creds = plaintext h2c
			log.Printf("h2c serve: %v", e)
		}
	}()

	log.Fatal(s.Serve(lis))
}
