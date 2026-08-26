// HTTP/REST entry point for NPLN, reconstructed from real-Switch captures (captures/s3/,
// runs s3_console_1944_vermillion / _1950_penneok). Splatoon 3 first talks JSON/REST to two
// host families — NOT gRPC (the gRPC matchmaking/datastore services come later, see main.go):
//
//	gw.hac.lp1.vermillion.srv.nintendo.net  — the "vermillion" gateway: device init + account config
//	val.hac.lp1.penne.srv.nintendo.net      — "penne": login tickets + the frontline FQDN
//	fro-N.hac.lp1.penne.srv.nintendo.net    — the persistent presence connection (TODO: streaming)
//
// Hosts are routed to this one server by Traefik SNI; we dispatch on the request path (and Host
// for the fro-N persistent connection). Every request is logged so we can see S3's exact sequence
// and catch any endpoint we haven't implemented yet.
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// persistentFrontline holds the HTTP response body open until the client
// disconnects, logging all binary frames S2/S3 sends. This is required:
// nnPenne expects the POST / connection to stay alive as a bidirectional
// stream (room-code registration). Closing it immediately (the old stub)
// causes nnPenne to error → 2307-2103 (S2) / 2321-* (S3).
func persistentFrontline(w http.ResponseWriter, r *http.Request) {
	log.Printf("[NPLN penne] fro POST host=%s X-Protocol-Version=%s from %s — holding open",
		r.Host, r.Header.Get("X-Protocol-Version"), r.RemoteAddr)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher, canFlush := w.(http.Flusher)
	if canFlush {
		flusher.Flush()
	}

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buf := make([]byte, 8192)
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				log.Printf("[NPLN penne] fro <- %d bytes: %x", n, buf[:n])
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("[NPLN penne] fro body read: %v", err)
				}
				break
			}
		}
	}()

	hb := time.NewTicker(30 * time.Second)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			log.Printf("[NPLN penne] fro client disconnected host=%s", r.Host)
			return
		case <-readDone:
			log.Printf("[NPLN penne] fro body ended host=%s", r.Host)
			return
		case <-hb.C:
			if canFlush {
				flusher.Flush()
			}
		}
	}
}

const frontlineFQDN = "fro-3.hac.lp1.penne.srv.nintendo.net"

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func randToken(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// penneParams mirrors Nintendo's captured persistent_connection_params_simple verbatim.
func penneParams() map[string]any {
	return map[string]any{
		"awake":        map[string]any{"count": 2, "disable": false, "idle": 60, "interval": 10},
		"ignore_rst":   true,
		"retry_count":  2,
		"rtt_max":      1000,
		"sleep":        map[string]any{"count": 1440, "disable": true, "idle": 60, "interval": 10},
		"wait_sec":     3600,
		"wowl_timeout": 200,
	}
}

// ---- vermillion gateway ----

// penneEnregistres retient les penne_id que le client a effectivement fait enregistrer (beach ou
// dragons). Sert uniquement a l'essai « initreject » ci-dessous.
var penneEnregistres = struct {
	sync.Mutex
	vus map[string]bool
}{vus: map[string]bool{}}

func noterPenneEnregistre(corps []byte) {
	var v struct {
		PenneID string `json:"penne_id"`
		PenneId string `json:"penneId"`
	}
	if json.Unmarshal(corps, &v) != nil {
		return
	}
	id := v.PenneID
	if id == "" {
		id = v.PenneId
	}
	if id == "" {
		return
	}
	penneEnregistres.Lock()
	penneEnregistres.vus[id] = true
	penneEnregistres.Unlock()
}

func penneEstEnregistre(id string) bool {
	penneEnregistres.Lock()
	defer penneEnregistres.Unlock()
	return penneEnregistres.vus[id]
}

// POST /v1/devices/initialize — req {"penneId":"..."}; sur la capture d'une console DEJA
// enregistree, le vrai serveur repond 204 No Content.
//
// ESSAI « initreject ». Sur l'emulateur, le client fabrique un penneId neuf toutes les quelques
// secondes et l'envoie ici ; nous repondons 204 (« c'est bon ») a chaque fois, et il recommence
// pourtant. La capture console montre pourquoi : avant d'appeler initialize, elle ENREGISTRE son
// penne_id sur beach puis dragons, et ouvre sa session penne. Un vrai serveur ne peut pas accepter
// un penneId que personne n'a enregistre. En repondant 404 dans ce cas, on regardait si le client
// basculait alors sur la branche d'enregistrement — celle qu'il ne prend jamais chez nous.
//
// ⚠️ MESURE NEGATIVE (2026-08-12, 23:49 UTC). Il ne bascule pas. Face au 404 il refait exactement
// la meme chose : un penneId neuf, puis GET vermillion-device-id, en boucle, sans jamais appeler
// beach ni dragons. Le choix de s'enregistrer ne depend donc PAS de la reponse a initialize.
// L'essai reste disponible (drapeau "initreject") mais il est eteint : 204 est la forme capturee.
func vermillionDeviceInitialize(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	corps := strings.TrimSpace(string(body))

	if soirFlag("initreject") {
		var v struct {
			PenneId string `json:"penneId"`
		}
		_ = json.Unmarshal(body, &v)
		if v.PenneId != "" && !penneEstEnregistre(v.PenneId) {
			log.Printf("[NPLN vermillion] devices/initialize %s -> 404 (penneId jamais enregistre, essai initreject)", corps)
			http.NotFound(w, r)
			return
		}
	}

	log.Printf("[NPLN vermillion] devices/initialize body=%s", corps)
	w.WriteHeader(http.StatusNoContent)
}

// GET /v1/devices/vermillion-device-id — the per-device NPLN id.
// SHAPE VERIFIED against capture s3_console_1944_vermillion (RESP 200):
//
//	{"vermillionDeviceId":"QdbC8zfuKXqYMAKjmdQsXg=="}   (camelCase key, base64 of 16 bytes)
//
// Our first guess ({"vermillion_device_id":"nx2dev-<random>"}) used the WRONG key so S3 never got a
// device id → looped vermillion → 2321-4992. Fixing the key let S3 advance, but a GENERATED base64
// value containing '/' then hard-crashed S3 (it appears to use the id as a string in a path). So we
// now return a FIXED, known-good value — the exact one real Nintendo returned in the capture (16
// bytes, no '/' or '+'). Single console for now; TODO: per-device unique value that avoids '/'+'+'.
func vermillionDeviceID(w http.ResponseWriter, r *http.Request) {
	id := "QdbC8zfuKXqYMAKjmdQsXg=="
	if soirFlag("devid") {
		if d := deviceIDduClient(r); d != "" {
			id = idVermillionPour(d)
			log.Printf("[NPLN vermillion] vermillion-device-id pour did=%s -> %s", d, id)
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"vermillionDeviceId": id})
}

// deviceIDduClient lit le « did:… » que le SDK place dans son User-Agent :
//
//	NintendoSDK Firmware/22.5.0-1.0 (platform:NX; did:63671daf59441c13; eid:lp1)
func deviceIDduClient(r *http.Request) string {
	ua := r.Header.Get("user-agent")
	i := strings.Index(ua, "did:")
	if i < 0 {
		return ""
	}
	reste := ua[i+4:]
	if j := strings.IndexAny(reste, "; )"); j >= 0 {
		reste = reste[:j]
	}
	return strings.TrimSpace(reste)
}

// idVermillionPour derive un identifiant vermillion PROPRE A L'APPAREIL.
//
// Jusqu'ici nous rendions a tout le monde la valeur capturee sur UNE console — celle de son
// proprietaire. Un client qui recoit l'identifiant d'un autre appareil ne peut pas le rapprocher de
// ce qu'il a enregistre ; c'est un candidat serieux pour la boucle observee (device-id, puis
// initialize avec une identite neuve, indefiniment).
//
// Contrainte tiree d'une mesure ancienne : une valeur base64 contenant '/' faisait CRASHER S3, qui
// s'en sert comme segment de chemin. On re-derive donc jusqu'a tomber sur 16 octets dont le base64
// ne contient ni '/' ni '+' — deterministe, donc stable d'un demarrage a l'autre.
func idVermillionPour(did string) string {
	for n := 0; n < 64; n++ {
		somme := sha256.Sum256([]byte(fmt.Sprintf("nextendo-vermillion:%s:%d", did, n)))
		v := base64.StdEncoding.EncodeToString(somme[:16])
		if !strings.ContainsAny(v, "/+") {
			return v
		}
	}
	return "QdbC8zfuKXqYMAKjmdQsXg==" // repli : la valeur capturee, connue pour ne pas casser
}

// PUT /v1/devices/penne-id — S3 associates this device with its penne id (the penne login identity).
// Was UNHANDLED (404) -> S3 could not finish device setup -> looped devices/initialize forever (never
// reached the gRPC lobby). Real server almost certainly replies 204 No Content (same as initialize).
func vermillionDevicePenneID(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	log.Printf("[NPLN vermillion] devices/penne-id (PUT) body=%s", strings.TrimSpace(string(body)))
	w.WriteHeader(http.StatusNoContent)
}

// GET /v1/accounts/config — returns {"payload": base64(JSON config)}.
// ★ THE critical field is online_license.is_available: it MUST be true. S3 works online for a
// licensed account; on our private server we always grant it. (The MITM returning is_available:false
// — because the broken-run login ticket failed — is exactly what gated S3 at the lobby -> 2321-4992.)
func vermillionAccountsConfig(w http.ResponseWriter, r *http.Request) {
	inner := map[string]any{
		"version":          map[string]int{"major": 1, "minor": 1, "micro": 0},
		"online_license":   map[string]bool{"is_available": true},
		"activity":         map[string]bool{"has_received_guidance": true, "has_inserted": true, "has_transferred_to_vphym": true},
		"disabled_content": map[string]any{"content_meta_ids": []string{}},
		"hidden_key":       map[string]any{"application_ids": []string{}},
	}
	b, _ := json.Marshal(inner)
	writeJSON(w, http.StatusOK, map[string]string{"payload": base64.StdEncoding.EncodeToString(b)})
}

// GET /v1/accounts/<sub> — catch-all for vermillion account sub-resources OTHER than "config".
// S3 v11.2.0's FRESH device/account bootstrap (no cached NPLN creds) calls GET /v1/accounts/vphyms
// right after devices/vermillion-device-id. We have NO capture of it (all our captures came from
// already-provisioned accounts that skipped this path), but a 404 makes S3 treat account setup as
// FAILED and restart the whole bootstrap with a NEW penneId — an infinite vermillion loop that never
// reaches penne login / the tenant gRPC (stuck on the ink loading screen). Returning a valid 200 with
// the same {"payload": base64(json)} envelope the sibling config endpoint uses breaks the loop; the
// empty inner ("{}") carries no wrong data to mis-parse. If S3 needs specific fields it will surface a
// new, more precise error we can then fill in. A subtree route ("/v1/accounts/") also future-proofs
// any other account sub-resource S3 starts calling. NOTE: "/v1/accounts/config" keeps its own exact
// route, which Go's ServeMux prefers over this subtree, so config is unaffected.
// ⚠️ 2026-08-12 : cette reponse est la SEULE INVENTION du chemin que S3 emprunte avant sa premiere
// RPC NPLN, et c'est exactement la que l'emulateur s'abat — 2162-0001 dans nn.npln.Worker, pile sur
// la branche d'echec du parseur de chemin de ressource (Thunder.nss:0x7d5ec4), 18 secondes AVANT le
// moindre appel gRPC. Un payload vide ne contient aucun nom de ressource a analyser ; c'est
// coherent avec un parseur qui echoue puis abandonne.
//
// On rend donc la forme commutable A CHAUD, par un fichier lu a chaque appel (pas d'variable
// d'environnement : elle imposerait de recreer le conteneur, et donc une coupure). Ecrire "404"
// dans /data/vphyms.mode fait repondre 404 comme avant l'invention — le bootstrap reboucle, mais
// s'il n'y a plus d'abort, la cause est etablie sans le moindre doute.
func vermillionModeAbsent() string {
	b, err := os.ReadFile("/data/vphyms.mode")
	if err != nil {
		return "vide"
	}
	return strings.TrimSpace(string(b))
}

func vermillionAccountsSub(w http.ResponseWriter, r *http.Request) {
	switch vermillionModeAbsent() {
	case "404":
		log.Printf("[NPLN vermillion] accounts sub %s %s -> 404 (mode d'essai : isoler l'abort)", r.Method, r.URL.Path)
		http.NotFound(w, r)
	default:
		log.Printf("[NPLN vermillion] accounts sub %s %s -> empty payload 200 (no capture; break bootstrap loop)", r.Method, r.URL.Path)
		writeJSON(w, http.StatusOK, map[string]string{"payload": base64.StdEncoding.EncodeToString([]byte("{}"))})
	}
}

// POST /v1/acds/search et GET /v1/nearness-checks — deux endpoints que S3 v11.2.0 appelle et que
// nous laissions en 404.
//
// LA MESURE : le 404 sur /v1/acds/search est IMMEDIATEMENT suivi d'un devices/initialize portant
// un penneId NEUF (19:23:17 -> 19:23:20, penneId pz1hak11j0av08mznawrxrrtqlvnhln19). C'est la
// signature exacte du redemarrage de bootstrap deja documente plus haut pour /v1/accounts/vphyms :
// une ressource absente fait considerer a S3 que la mise en place du compte a echoue, et il
// recommence tout avec une nouvelle identite d'appareil — 1090 devices/initialize et 1186
// accounts/config en six heures, une boucle qui n'aboutit jamais.
//
// AUCUNE CAPTURE ne contient ces deux appels (la console capturee etait deja provisionnee), donc
// la FORME de la reponse est une hypothese, pas une mesure : on reprend l'enveloppe
// {"payload": base64(json)} de ses voisines, avec un contenu vide — aucune donnee fausse a
// mal interpreter, et surtout plus de 404. On journalise le corps de la requete : c'est comme ca
// qu'on saura ce que le jeu cherche vraiment, et qu'on remplacera cette hypothese par une mesure.
func vermillionGuessedOK(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	log.Printf("[NPLN vermillion] %s %s corps=%q -> payload vide 200 (forme SUPPOSEE, pas de capture)",
		r.Method, r.URL.Path, strings.TrimSpace(string(body)))
	writeJSON(w, http.StatusOK, map[string]string{"payload": base64.StdEncoding.EncodeToString([]byte("{}"))})
}

// ---- penne ----

// POST /v1/login_tickets — req {"id":penneId,"password":...}; resp = a ticket + the frontline FQDN
// + the keep-alive params. Shape verified against capture s3_console_1950_penneok (RESP 200).
func penneLoginTickets(w http.ResponseWriter, r *http.Request) {
	now := time.Now().Unix()
	writeJSON(w, http.StatusOK, map[string]any{
		"expires_at":                          now + 345600, // ~4 days (captured expires-issued delta)
		"frontline_fqdn":                      frontlineFQDN,
		"issued_at":                           now,
		"persistent_connection_params_simple": penneParams(),
		"ticket":                              randToken(48),
	})
}

// GET /v1/frontlines — resp = the current frontline FQDN + params.
func penneFrontlines(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"current_time":                        time.Now().Unix(),
		"frontline_fqdn":                      frontlineFQDN,
		"persistent_connection_params_simple": penneParams(),
	})
}

// POST / on fro-N — persistent presence stream (delegates to persistentFrontline above).

// logHTTP logs every request (method + host + path) and flags unhandled paths so we know what
// S3 calls next that we still have to implement.
func logHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[NPLN HTTP] %s %s%s", r.Method, r.Host, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

// buildRestMux builds the vermillion + penne REST routes (+ fro-N stub + catch-all). Reused by
// both the standalone gateway (behind Traefik) and the local combined :443 server (local.go).
// traceREST journalise CE QUE LE CLIENT ENVOIE sur la passerelle : methode, chemin, en-tetes
// utiles et corps. Sans ca on ne voyait que nos propres reponses, et le bootstrap qui boucle
// (un penneId neuf toutes les quelques secondes, jamais de login penne) restait muet sur ce que
// le client attend. Le corps est plafonne : ces requetes sont petites, et un dump geant noierait
// le reste.
// traceRESTSiDemande n'active la trace que si le drapeau "trace" est pose : c'est un diagnostic,
// il n'a rien a faire dans le chemin nominal.
func traceRESTSiDemande(next http.Handler) http.Handler {
	tracee := traceREST(next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if soirFlag("trace") {
			tracee.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func traceREST(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ⚠️ NE JAMAIS lire le corps d'un flux. La connexion persistante fro-N est un POST dont le
		// corps reste ouvert toute la session : le lire ici bloque la requete avant meme d'entrer
		// dans son handler, et la presence penne ne s'etablit jamais. On ne capture donc que les
		// corps ANNONCES et petits — c'est-a-dire exactement les requetes JSON du bootstrap, les
		// seules qu'on cherchait a voir.
		var body []byte
		const maxCorps = 2048
		if r.Body != nil && r.ContentLength > 0 && r.ContentLength <= maxCorps {
			body, _ = io.ReadAll(io.LimitReader(r.Body, maxCorps))
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		interesting := []string{"authorization", "x-device-id", "x-vermillion-device-id", "user-agent", "content-type"}
		var hdr []string
		for _, h := range interesting {
			if v := r.Header.Get(h); v != "" {
				// L'User-Agent porte « did:<deviceId> », l'identifiant d'appareil dont le client se sert
				// pour construire l'URL d'enregistrement de son penne_id. Le tronquer a 48 caracteres le
				// coupait juste avant : on garde de quoi le lire.
				if len(v) > 160 {
					v = v[:160] + "…"
				}
				hdr = append(hdr, h+"="+v)
			}
		}
		// L'IP source est indispensable : plusieurs clients tapent la meme passerelle en meme temps
		// (la console du salon et l'emulateur), et sans elle leurs sequences s'entrelacent dans le
		// journal — on croit voir une machine incoherente la ou il y en a deux.
		src := r.Header.Get("x-forwarded-for")
		if src == "" {
			src = r.RemoteAddr
		}
		log.Printf("[NPLN REST] %s | %s %s | %s | corps=%s", src, r.Method, r.URL.Path,
			strings.Join(hdr, " "), strings.TrimSpace(string(body)))
		next.ServeHTTP(w, r)
	})
}

func buildRestMux() http.Handler {
	mux := http.NewServeMux()
	// vermillion gateway
	mux.HandleFunc("/v1/devices/initialize", vermillionDeviceInitialize)
	mux.HandleFunc("/v1/devices/vermillion-device-id", vermillionDeviceID)
	mux.HandleFunc("/v1/devices/penne-id", vermillionDevicePenneID)
	mux.HandleFunc("/v1/accounts/config", vermillionAccountsConfig)
	mux.HandleFunc("/v1/accounts/", vermillionAccountsSub) // vphyms + any other account sub-resource (config wins via exact match)
	mux.HandleFunc("/v1/acds/search", vermillionGuessedOK) // 404 ici = bootstrap relance avec un penneId neuf
	mux.HandleFunc("/v1/nearness-checks", vermillionGuessedOK)
	// Enregistrement du penne_id — l'etape qui manquait, et sans laquelle penne n'a JAMAIS ete
	// atteint (0 login_tickets en des mois de journaux, pour 159 PUT penne-id).
	mux.HandleFunc("/v1/devices/", vermillionDeviceSub) // .../<deviceId>/penne_id/register
	mux.HandleFunc("/v2/penne_id", dragonsPenneID)
	// penne
	mux.HandleFunc("/v1/login_tickets", penneLoginTickets)
	mux.HandleFunc("/v1/frontlines", penneFrontlines)
	// fro-N persistent connection (Host fro-N...) + catch-all for not-yet-seen endpoints
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodPost && strings.HasPrefix(r.Host, "fro-") {
			persistentFrontline(w, r)
			return
		}
		log.Printf("[NPLN HTTP] !! UNHANDLED %s %s%s", r.Method, r.Host, r.URL.Path)
		http.NotFound(w, r)
	})
	return traceRESTSiDemande(logHTTP(mux))
}

// startHTTPGateway serves the captured vermillion + penne REST endpoints (and the fro-N stub) on
// addr over TLS. Run alongside the gRPC server (which handles the not-yet-captured matchmaking).
func startHTTPGateway(addr, certFile, keyFile string) {
	log.Printf("[NPLN HTTP] gateway (vermillion + penne) listening on %s (TLS)", addr)
	srv := &http.Server{Addr: addr, Handler: buildRestMux()}
	if err := srv.ListenAndServeTLS(certFile, keyFile); err != nil {
		log.Fatalf("[NPLN HTTP] %v", err)
	}
}

// ---- enregistrement du penne_id (beach + dragons) ----
//
// CE QUI A ETE MESURE (captures/s3/s3_console_1944_vermillion.txt, console reelle contre Nintendo).
// Avant de pouvoir ouvrir sa session penne, le jeu enregistre l'identite d'appareil qu'il vient de
// fabriquer, sur DEUX hotes que nous ne servions pas :
//
//	POST beach.hac.lp1.eshop.nintendo.net/v1/devices/<deviceId>/penne_id/register  {"penne_id":"pz…"}
//	PUT  dragons.hac.lp1.dragons.nintendo.net/v2/penne_id                          {"penne_id":"pz…"}
//	POST val.hac.lp1.penne.srv.nintendo.net/v1/login_tickets  {"id":"pz…","password":"…"}  -> 200
//
// Le mot de passe n'apparait dans AUCUNE reponse serveur de la capture : le client le fabrique
// lui-meme, en meme temps que l'identifiant. L'enregistrement n'a donc pas a lui rendre de secret —
// il a juste a REUSSIR. Chez nous il echouait (ces hotes tombaient sur d'autres services, 200 et
// 404), le client considerait l'appareil non enregistre, en refabriquait un et recommencait : c'est
// la boucle qu'on observait, avec un penneId neuf toutes les quelques secondes, et jamais un seul
// login_tickets en des mois de journaux.
//
// ⚠️ La FORME des reponses est une hypothese, pas une mesure : le journal mitmproxy de l'epoque
// n'a pas conserve ces deux corps. On repond donc le minimum credible — un succes, un objet vide —
// et on journalise la requete pour corriger des qu'une capture les montrera.

// vermillionDeviceSub traite les sous-ressources de /v1/devices/ non prises par une route exacte.
func vermillionDeviceSub(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))

	if strings.HasSuffix(r.URL.Path, "/penne_id/register") {
		noterPenneEnregistre(body)
		log.Printf("[NPLN penne] enregistrement du penne_id (beach) %s corps=%s",
			r.URL.Path, strings.TrimSpace(string(body)))
		writeJSON(w, http.StatusOK, map[string]any{})
		return
	}

	log.Printf("[NPLN HTTP] !! sous-ressource devices non geree %s %s%s corps=%s",
		r.Method, r.Host, r.URL.Path, strings.TrimSpace(string(body)))
	http.NotFound(w, r)
}

// dragonsPenneID : PUT /v2/penne_id sur dragons — le second enregistrement.
func dragonsPenneID(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	noterPenneEnregistre(body)
	log.Printf("[NPLN penne] enregistrement du penne_id (dragons) %s corps=%s",
		r.Method, strings.TrimSpace(string(body)))
	writeJSON(w, http.StatusOK, map[string]any{})
}
