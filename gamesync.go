package main

// Gamesync (nn.npln.gamesync.v1.Gamesync) — the NPLN "game session" / session-transport
// service. After matchmaking (Create/Track), the client's Pia/NPLN layer opens a SECOND gRPC
// connection to GameSession.Host:Port (our relay :7575, SNI gs.nintendo.net) and drives the
// actual session ESTABLISHMENT here: IssueToken (exchange the matchmaking id-token for a
// gamesync session token) then KeepUserSession (a bidirectional keep-alive stream that holds
// the session "connected") plus a Firestore-Listen document store the host WATCHES.
//
// THE create-room blocker (reversed 2026-07-31 from bq.nss/main.flat): after subscribing, the
// host does NOT write — it passively WAITS for its own UserSession to appear in the watched
// docs, then reads GameSessionMutableData ("Get SessionMutableData" -> "GameSession Data
// Success!"). Diagnostic strings in the binary: "Timed out to get my UserSession.",
// "UserSession collection changed: %s", "GameSessionMutableData(exists: ...". We answered
// DELETED/empty, so bq::RecreateSessionFiber's CreateNplnSession blocked forever ("connecting"
// screen at 60fps). The fix is to PUSH those documents as EXIST with the exact Firestore
// MapValue the game's DocumentReader parses. The keys are short codes baked into the reader:
//
//	UserSession  (docs/__pgn/All/__stu/<uss>, and members of the __pus collection):
//	   uid(str) ussid/ucsid/upcsid(int) pgn(str) st(int state) att(map) ltc(map) tn(str team)
//	   reader @0xd88084; getters GetString@0xa20c6c GetInt@0xa20b60 are null-safe (default on
//	   missing field) — so the only hard requirement is a NON-nil, well-typed Value per key.
//	GameSessionMutableData (docs/__stg/All):
//	   gsid(str) addr(str) p(int port) mcn(str) maxu(int) cp(int) ip(bool) pw(str) prp(map)
//	   reader @0xd6273c.
//
// The empty-Fields variants crashed the nn::npln worker (null oneof); every Value below sets a
// concrete oneof, which is what the worker's Firestore parser needs.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	gspb "npln.nintendo.net/npln-practice/proto/gamesync/v1"
)

// gsSessionInfo is the minimum the host needs to see itself present in the room. Decoded from
// the matchmaking id-token (IssueToken) plus our own relay config; cached so the later
// KeepUserSession watches can synthesize the documents.
type gsSessionInfo struct {
	uid      string // "u-…" (host's NPLN user id) — the UserSession.uid the host matches on
	gsName   string // tenants/…/gameSessions/<uuid>
	gsid     string // <uuid> (last segment of gsName) — GameSessionMutableData.gsid
	config   string // base matchmaking config, bridged in the signed ticket from the matchmaker
	host     string // relay host (addr)
	port     int    // relay port (p)
	maxp     int    // max participants (maxu)
	password string // room password (empty = none); surfaced as rs.pw so the host shows the lock
	isPublic bool   // room is public (no password gate); surfaced as rs.ip
	// rang = numero d'ordre du participant DANS sa partie (1, 2, 3...), attribue a l'arrivee.
	// Il alimente ussid/ucsid/upcsid, qui valaient 1 pour TOUT LE MONDE : a deux joueurs les deux
	// sessions etaient indiscernables et se repliaient sur le meme emplacement de participant, si
	// bien qu'aucun des deux ne voyait l'autre pendant la recherche (mesure du 2026-08-14).
	rang int
}

// roomSettingsByGsid bridges the matchmaking server (which learns the room password/public flag at
// CreateGameSessionCreationTicket) and the gamesync server (which builds the __stg room-settings the
// host reads back). Keyed by gsid (last segment of the gameSession name). Without this, the __stg
// readback hardcoded ip=true/pw="" and the host always displayed "password off".
// reglagesSalon : ce qu'on retient d'un salon entre le matchmaking et le gamesync.
//
// `props` = les proprietes ENVOYEES PAR LE JEU a la creation (userName, game_mode…), enrichies des
// deux cles serveur par s3SessionProperties. Mesure du 2026-08-15 (capture du salon prive Nintendo,
// docs/__gs/m champ `prp`) : Nintendo les republie telles quelles dans le document de reglages —
// c'est de la que le jeu relit le pseudo de l'hote et le mode choisi.
type reglagesSalon struct {
	password string
	isPublic bool
	config   string // nom de la configuration demandee (« private_match_config », « coop_private_config »…)
	props    *commonpb.MapValue
}

var roomSettingsByGsid = struct {
	sync.Mutex
	m map[string]reglagesSalon
}{m: map[string]reglagesSalon{}}

func rememberRoomSettings(gsid, password string, isPublic bool, config string, props *commonpb.MapValue) {
	roomSettingsByGsid.Lock()
	roomSettingsByGsid.m[gsid] = reglagesSalon{password: password, isPublic: isPublic, config: config, props: props}
	roomSettingsByGsid.Unlock()
}

// proprietesSalon rend les proprietes republiees dans __gs/m.prp (nil si le salon est inconnu).
func proprietesSalon(gsid string) *commonpb.MapValue {
	roomSettingsByGsid.Lock()
	rs := roomSettingsByGsid.m[gsid]
	roomSettingsByGsid.Unlock()
	return rs.props
}

func roomSettingsFor(gsid string) (password string, isPublic bool, ok bool) {
	roomSettingsByGsid.Lock()
	rs, ok := roomSettingsByGsid.m[gsid]
	roomSettingsByGsid.Unlock()
	return rs.password, rs.isPublic, ok
}

// modeDuMatch rend la configuration du salon d'ou vient ce match. Le suffixe d'un match porte
// « <horodatage>_<gsid> » : le gsid en est la partie qui suit le premier souligne.
func modeDuMatch(suffixe string) string {
	if i := strings.Index(suffixe, "_"); i >= 0 {
		return roomConfigFor(suffixe[i+1:])
	}
	return ""
}

// roomConfigFor rend le nom de configuration du salon (dernier segment), ou "" s'il est inconnu.
func roomConfigFor(gsid string) string {
	roomSettingsByGsid.Lock()
	rs := roomSettingsByGsid.m[gsid]
	roomSettingsByGsid.Unlock()
	return rs.config
}

type gamesyncServer struct {
	gspb.UnimplementedGamesyncServer
	mu      sync.Mutex
	sess    map[string]*gsSessionInfo // keyed by userSession uuid (last seg)
	last    *gsSessionInfo            // most-recent — fallback when a watch predates its IssueToken
	lastUss string                    // most-recent userSession uuid (collection-member fallback)
	// closing[uss] is set when a userSession's KeepUserSession watch stream has ENDED and not reopened.
	// While a stream is OPEN we return EMPTY write_results (the client leaves its own presence write
	// PENDING, so the watch-pushed member stays -> room shows 1/4). After the stream closes (End-Session
	// teardown) the client AWAITS a final presence write and wild-derefs on an empty result -> so once
	// closing, we return POPULATED write_results (no crash). The crashing write (update+transform __pus)
	// provably arrives AFTER "closed by client", so this split fixes the count AND End-Session together.
	closing map[string]bool
	// store is a REAL document store (like Nintendo's gamesync), keyed by full doc name -> fields.
	// The canned/stateless responses could not keep the host's presence consistent through the client's
	// delete/re-add heartbeat (member count -> 0/4, so the party can never start). Tracking the actual
	// docs the client writes — and re-serving them in reads + watch — mirrors the real server so the
	// __pus presence collection stays populated. Absent from the map = the doc does not exist.
	store map[string]*commonpb.MapValue
	// wakers[uss] lets a WriteDocuments call nudge that userSession's KeepUserSession watch to immediately
	// re-push the affected collection (event-driven, so a re-added member appears without waiting 3s).
	wakers map[string]chan struct{}
}

func newGamesyncServer() *gamesyncServer {
	return &gamesyncServer{
		sess:    map[string]*gsSessionInfo{},
		closing: map[string]bool{},
		store:   map[string]*commonpb.MapValue{},
		wakers:  map[string]chan struct{}{},
	}
}

// applyWritesToStore mutates the real document store from the client's write ops (call under g.mu held).
// update/merge -> upsert the doc's fields; transform -> ensure the doc exists and apply the transform
// results to its fields; delete -> remove the doc. Mirrors what Nintendo's gamesync does so subsequent
// reads/watches reflect the true state (esp. the __pus presence member surviving the delete/re-add cycle).
// cleStore range un document SOUS SA PARTIE.
//
// ⚠️ Le magasin etait une table PLATE, partagee par tous les joueurs et toutes les parties. Or
// docs/MasterCollection/Session porte un nom FIXE : deux hotes qui ouvrent un salon en meme temps
// ecrivaient tous les deux dessous, le second ecrasant le premier, et chacun relisait la session de
// l'autre. Mesure du 2026-08-15 : quand deux salons coexistent, le SECOND createur — quel qu'il
// soit — annule son propre ticket dans la seconde (Create puis Cancel au meme horodatage, Track ->
// FAILED). Meme cause pour docs/SessionInfo, dont chaque joueur recevait DEUX exemplaires alors que
// la capture Nintendo montre cette collection vide puis peuplee du seul document de l'hote.
// rapportsArbitrage accumule les rapports de fin de partie, par match puis par joueur.
//
// Mesure du 2026-08-16, capture d'une fin de partie Nintendo decodee champ par champ : le verdict
// @@RefereeResult porte un champ `personal_result` qui est une CARTE indexee par la session de
// chaque joueur (1 305 des 2 368 octets du document). Chaque console, elle, ecrit son propre
// rapport dans @@RefereeReport/<horodatage>_<partie>/User/<sa session>.
//
// Le serveur AGREGE donc : il rassemble les rapports individuels en un verdict unique. Je repondais
// a chaque rapport isolement, avec six champs de cadre et rien du contenu — d'ou un ecran de score
// que le jeu ne pouvait pas afficher.
var rapportsArbitrage = struct {
	sync.Mutex
	m map[string]map[string]*commonpb.Value // cle du match -> session du joueur -> ses donnees
}{m: map[string]map[string]*commonpb.Value{}}

// retenirRapport range le `personal_result` PORTE PAR LE RAPPORT d'une console.
//
// ⚠️ LE RAPPORT N'EST PAS UN RAPPORT INDIVIDUEL — decodage du 2026-08-16, deux captures montantes
// independantes. Le document que la console ecrit ne porte que DEUX champs de haut niveau :
// `team_result` et `personal_result`. Et son `personal_result` est deja la carte de TOUS les
// joueurs de la partie (1 269 o a deux joueurs, 6 089 o a huit), pas seulement le sien.
//
// Nous rangions le rapport ENTIER sous l'identifiant de son auteur : la carte des huit joueurs se
// retrouvait imbriquee d'un cran de trop, sous une seule cle. D'ou l'anomalie de poids mesuree —
// environ 1 551 octets par joueur chez nous contre 1 305 pour la carte COMPLETE chez Nintendo.
//
// On fusionne donc entree par entree : chaque console decrit tous les joueurs, mais n'est complete
// que sur elle-meme (elle seule ajoute son display_order et son report_score).
func retenirRapport(cleMatch string, resultatsPersonnels *commonpb.Value) int {
	src := resultatsPersonnels.GetMapValue().GetFields()
	if cleMatch == "" || len(src) == 0 {
		return 0
	}
	rapportsArbitrage.Lock()
	defer rapportsArbitrage.Unlock()
	if rapportsArbitrage.m[cleMatch] == nil {
		rapportsArbitrage.m[cleMatch] = map[string]*commonpb.Value{}
	}
	for joueur, entree := range src {
		champs := entree.GetMapValue().GetFields()
		if len(champs) == 0 {
			continue
		}
		fusion := map[string]*commonpb.Value{}
		if deja := rapportsArbitrage.m[cleMatch][joueur]; deja != nil {
			for k, v := range deja.GetMapValue().GetFields() {
				fusion[k] = v
			}
		}
		for k, v := range champs {
			fusion[k] = v
		}
		rapportsArbitrage.m[cleMatch][joueur] = gsMap(fusion)
	}
	return len(rapportsArbitrage.m[cleMatch])
}

// parametresMatch retient l'IDENTITE du match, telle que le jeu nous l'a donnee au coup d'envoi.
//
// Mesure du 2026-08-16, meme partie privee suivie d'un bout a l'autre : au demarrage nous publions un
// verdict de 27 champs — stage, mode, game_rule, battle_id, users, color_pattern, season… — parce que
// la demande @@RefereeRequest les porte. A la fin, le rapport @@RefereeReport d'un joueur n'apporte
// qu'UN seul champ nouveau (team_result), et notre verdict retombait a 11 champs : l'ecran de score
// recevait un resultat sans stage (remis a 0), sans mode, sans regle et sans liste de joueurs.
//
// D'ou l'erreur de communication a l'affichage du score alors que la partie s'etait jouee entiere.
// On memorise donc les parametres du coup d'envoi et on les rejoue dans le verdict de fin : ce ne
// sont pas des valeurs inventees, ce sont celles que les consoles elles-memes ont annoncees.
var parametresMatch = struct {
	sync.Mutex
	m map[string]map[string]*commonpb.Value // cle du match -> parametres annonces au demarrage
}{m: map[string]map[string]*commonpb.Value{}}

func retenirParametres(cleMatch string, champs map[string]*commonpb.Value) {
	if cleMatch == "" || len(champs) == 0 {
		return
	}
	copie := make(map[string]*commonpb.Value, len(champs))
	for k, v := range champs {
		copie[k] = v
	}
	parametresMatch.Lock()
	parametresMatch.m[cleMatch] = copie
	parametresMatch.Unlock()
}

func parametresDe(cleMatch string) map[string]*commonpb.Value {
	parametresMatch.Lock()
	defer parametresMatch.Unlock()
	return parametresMatch.m[cleMatch]
}

// champsServeurDuJoueur : les huit champs qu'une console ne remonte JAMAIS et que le serveur doit
// fabriquer. Decodage du 2026-08-16 : une entree de `personal_result` porte TOUJOURS vingt-trois
// champs chez Nintendo (46 entrees relues sur 8 documents, jeu de cles rigoureusement identique) ;
// une console n'en ecrit au mieux que quinze. Les valeurs par defaut ci-dessous sont celles lues
// dans les captures, pas des valeurs choisies.
var champsServeurDuJoueur = map[string]func() *commonpb.Value{
	"contribution":     func() *commonpb.Value { return gsInt(0) },
	"did_get_noroshi1": func() *commonpb.Value { return gsBool(false) },
	"did_get_noroshi2": func() *commonpb.Value { return gsBool(false) },
	"noroshi_try_num":  func() *commonpb.Value { return gsInt(0) },
	"outfit_bonus":     func() *commonpb.Value { return gsDouble(0) },
	"party_ticket":     func() *commonpb.Value { return gsInt(0) },
	"team_byname": func() *commonpb.Value {
		return gsMap(map[string]*commonpb.Value{
			"template": gsInt(0), "unity0_tag": gsStr(""), "unity1_tag": gsStr(""),
			"unity2_tag": gsStr(""), "property_tag": gsStr(""), "group_tag": gsStr(""),
		})
	},
}

// champsFacultatifsDuJoueur : presents chez Nintendo meme quand la console ne les a pas ecrits —
// la valeur est alors NULLE, pas absente. Mesure : un joueur deconnecte d'une partie a huit portait
// bien ses vingt-trois cles, avec display_order et report_score a null dans un document status=1.
var champsFacultatifsDuJoueur = []string{
	"awards", "catalog_point", "death_num", "disconnected", "display_order", "group_leader",
	"individual_stats", "kill_assist_num", "kill_num", "npln_user_id", "paint_point",
	"report_score", "single_ticket", "special_num", "team",
}

// champsDuVerdict : les VINGT-CINQ champs de haut niveau du document @@RefereeResult, releves dans
// les octets le 2026-08-16. Le meme jeu de cles, sans une variation, sur les huit documents et les
// quarante-six entrees de joueur decodes — parties privees a deux comme parties Nintendo a huit.
//
// Ma mesure precedente annoncait vingt et un champs : elle etait fausse, et prise qui plus est sur
// l'etat de DEMARRAGE du document (status=-1) plutot que sur le verdict de fin.
// champsDuVerdictCoop : les ONZE champs de haut niveau du document @@RefereeResult d'une partie
// SALMON RUN, releves sur la capture Nintendo du 2026-08-20 00:34 (relais-session, hote de session
// 34.44.179.9:7287, verdict de 2007 octets pousse sur KeepUserSession).
//
// Sept sont communs au versus. QUATRE ne le sont pas — challenge_grade_point, scenario, shift_id,
// shift_time — et l'inventaire versus les aurait supprimes, rendant un verdict que le jeu ne peut
// pas lire. A l'inverse, le coop ignore stage, game_rule, teams, mode, les mmr et tout le bloc fest.
//
// Le detail vit dans les sous-messages : team_result porte final_wave, wave_detail, result et les
// trois compteurs d'ecailles (gold/silver/copper) ; personal_result porte par joueur report_score,
// paint_point, catalog_point, personal_wave_result, group_leader et disconnected.
// champsPropresAuCoop : les champs qu'une partie SALMON RUN est seule a porter. Aucun verdict de
// versus ne les contient — c'est ce qui en fait une signature sure, releve sur la capture Nintendo
// du 2026-08-20 comme sur nos propres parties. job_id en fait partie bien qu'il ne survive pas a
// l'inventaire : le jeu l'envoie, Nintendo ne le republie pas, mais sa presence reste un aveu.
var champsPropresAuCoop = []string{"challenge_grade_point", "job_id", "scenario", "shift_id", "shift_time"}

// verdictCoop reconnait une partie Salmon Run a son contenu, sans rien demander au salon.
func verdictCoop(champs map[string]*commonpb.Value) bool {
	for _, k := range champsPropresAuCoop {
		if _, present := champs[k]; present {
			return true
		}
	}
	return false
}

var champsDuVerdictCoop = map[string]bool{
	"challenge_grade_point": true, "parent_npln_user_id": true, "personal_result": true,
	"request_id": true, "scenario": true, "shift_id": true, "shift_time": true,
	"status": true, "team_result": true, "trace_id": true, "users": true,
}

var champsDuVerdict = map[string]bool{
	"bankara_open_schedule_id": true, "color_pattern": true, "est_mmr_team1": true,
	"est_mmr_team2": true, "est_mmr_team3": true, "event_parameter": true, "fest_id": true,
	"fest_region": true, "fest_resource_id": true, "game_mode": true, "game_rule": true,
	"league_part_id": true, "mm_region": true, "mode": true, "parent_npln_user_id": true,
	"personal_result": true, "request_id": true, "season_id": true, "stage": true, "status": true,
	"team_result": true, "teams": true, "tournament_id": true, "trace_id": true, "users": true,
}

// enDouble reecrit en flottants les compteurs entiers de `report_score`.
//
// Mesure du 2026-08-16 : la console encode ces neuf compteurs en ENTIER (« 12 02 18 00 ») et
// Nintendo les republie en DOUBLE (« 12 09 29 00 00 … »). Republier l'entier tel quel, c'est
// changer le type d'un champ que le jeu relit ensuite.
func enDouble(v *commonpb.Value) *commonpb.Value {
	switch {
	case v.GetMapValue() != nil:
		out := map[string]*commonpb.Value{}
		for k, sv := range v.GetMapValue().GetFields() {
			out[k] = enDouble(sv)
		}
		return gsMap(out)
	case v.GetValueType() == nil:
		return v
	}
	if _, entier := v.GetValueType().(*commonpb.Value_IntegerValue); entier {
		return gsDouble(float64(v.GetIntegerValue()))
	}
	return v
}

func rapportIndiqueDeconnexion(v *commonpb.Value) bool {
	switch v.GetValueType().(type) {
	case *commonpb.Value_BooleanValue:
		return v.GetBooleanValue()
	case *commonpb.Value_IntegerValue:
		return v.GetIntegerValue() != 0
	case *commonpb.Value_DoubleValue:
		return v.GetDoubleValue() != 0
	default:
		return false
	}
}

func resultatIndiqueDeconnexion(v *commonpb.Value) bool {
	switch v.GetValueType().(type) {
	case *commonpb.Value_IntegerValue:
		return v.GetIntegerValue() == 2
	case *commonpb.Value_DoubleValue:
		return v.GetDoubleValue() == 2
	default:
		return false
	}
}

// resultatsIndividuels rend la carte `personal_result` du verdict : la fusion des rapports connus
// pour ce match, completee des champs que seul le serveur produit.
//
// `vainqueur` vient de team_result.winner : c'est de lui que se deduit le `deemed_result` de chaque
// joueur. Mesure : dans une partie a deux, un joueur porte 0 et l'autre 1 ; un joueur deconnecte
// d'une partie a huit porte 2. On lit donc l'equipe du joueur et on tranche, sans rien inventer.
func resultatsIndividuels(cleMatch string, vainqueur int64, coop bool) (*commonpb.Value, int) {
	rapportsArbitrage.Lock()
	defer rapportsArbitrage.Unlock()
	src := rapportsArbitrage.m[cleMatch]
	if len(src) == 0 {
		return nil, 0
	}

	// EN SALMON RUN, LE VERDICT RECOPIE LE RAPPORT — on n'ajoute RIEN.
	//
	// Capture Nintendo du 2026-08-20 : le `personal_result` du verdict coop porte exactement les
	// sept champs du rapport (report_score, paint_point, catalog_point, personal_wave_result,
	// group_leader, disconnected, npln_user_id) et pas un de plus. Les vingt-trois champs versus
	// ajoutes plus bas n'y figurent nulle part.
	//
	// ⚠️ Et surtout : en coop `report_score` est un MESSAGE (cheating, cheat_reason_flags, survival,
	// disconnect, degradation, nocontrol, transportion), pas un nombre. Lui appliquer enDouble le
	// detruirait — c'est justement ce bloc qui porte l'anti-triche de Nintendo.
	if coop {
		out := make(map[string]*commonpb.Value, len(src))
		for joueur, entree := range src {
			out[joueur] = entree
		}
		return gsMap(out), len(out)
	}

	out := make(map[string]*commonpb.Value, len(src))
	for joueur, entree := range src {
		champs := map[string]*commonpb.Value{}
		for k, v := range entree.GetMapValue().GetFields() {
			if k == "report_score" {
				v = enDouble(v)
			}
			champs[k] = v
		}
		for k, defaut := range champsServeurDuJoueur {
			if _, deja := champs[k]; !deja {
				champs[k] = defaut()
			}
		}
		for _, k := range champsFacultatifsDuJoueur {
			if _, deja := champs[k]; !deja {
				champs[k] = gsNul()
			}
		}
		if rapportIndiqueDeconnexion(champs["disconnected"]) || resultatIndiqueDeconnexion(champs["deemed_result"]) {
			// Treat a disconnect as a loss so it does not produce the game's disconnect result.
			// Always replace result 2, even if this report omitted/cleared `disconnected`; otherwise
			// a client-supplied disconnect result could still trigger the game's disconnect penalty.
			champs["deemed_result"] = gsInt(1)
		} else if _, deja := champs["deemed_result"]; !deja {
			verdict := int64(1)
			switch {
			case vainqueur != 0 && champs["team"].GetIntegerValue() == vainqueur:
				verdict = 0
			}
			champs["deemed_result"] = gsInt(verdict)
		}
		out[joueur] = gsMap(champs)
	}
	return gsMap(out), len(out)
}

func gsTableau(vals ...*commonpb.Value) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: vals}}}
}

// arbitrer rend le verdict que le jeu attend pour DEMARRER une partie.
//
// Mesure du 2026-08-15 : une fois les deux joueurs prets, chacun ecrit
// « docs/@@RefereeRequest/<horodatage>_<partie> » et le profil de chaque participant, puis surveille
// « docs/@@RefereeResult ». Personne ne repondant, le compte a rebours expirait et les deux consoles
// rendaient une erreur de communication a l'ecran « les joueurs se preparent ».
//
// La forme du verdict vient de la capture d'une vraie partie Nintendo (@@RefereeResult, 2368 o) :
// les equipes, la saison, le festival, et surtout `request_id` qui reprend EXACTEMENT le nom de la
// demande — c'est par la que le jeu apparie le verdict a sa requete.
func (g *gamesyncServer) arbitrer(gsid, nomDemande string, ops []*gspb.WriteOperation) (string, string, *commonpb.MapValue) {
	// Le jeu s'adresse a l'arbitre DEUX fois, et attend un verdict a chaque fois :
	//   @@RefereeRequest  au demarrage — sans reponse, le compte a rebours expire au lobby ;
	//   @@RefereeReport   a la fin      — sans reponse, l'erreur tombe sur l'ecran de resultat.
	// Mesure du 2026-08-15 : une partie privee jouee de bout en bout s'est achevee sur une erreur de
	// communication a l'affichage du score, et le seul document ecrit a cet instant etait le rapport.
	suffixe, finDePartie := "", false
	for _, prefixe := range []string{"docs/@@RefereeRequest/", "docs/@@RefereeReport/"} {
		if s := strings.TrimPrefix(nomDemande, prefixe); s != nomDemande {
			suffixe = s
			finDePartie = prefixe == "docs/@@RefereeReport/"
			break
		}
	}
	if suffixe == "" {
		return "", "", nil
	}

	// ⚠️ NE GARDER QUE LE PREMIER SEGMENT.
	//
	// Mesure du 2026-08-15 : le rapport de fin est un SOUS-DOCUMENT —
	// « @@RefereeReport/<horodatage>_<partie>/User/<identifiant> ». En reprenant le suffixe entier,
	// nous publiions « @@RefereeResult/<horodatage>_<partie>/User/<identifiant> », soit un niveau
	// PLUS BAS que ce que le client surveille : il observe la collection docs/@@RefereeResult et n'en
	// voit que les membres DIRECTS. Le verdict etait donc bien produit — huit rapports, huit verdicts
	// dans le journal — et n'atteignait personne. D'ou l'erreur de communication sur l'ecran de score
	// alors que tous les compteurs semblaient bons.
	//
	// La demande de demarrage, elle, n'a pas ce suffixe, et c'est pourquoi le lancement fonctionnait.
	if i := strings.IndexByte(suffixe, '/'); i > 0 {
		suffixe = suffixe[:i]
	}

	// ⚠️ LA PARTIE EST DANS LE NOM — ne pas la deduire.
	//
	// Le suffixe vaut « <horodatage>_<partie> ». Or gsidPourEcriture deduit la partie du chemin du
	// document, et ce chemin porte DEUX uuid (celui de la partie et celui du joueur, apres /User/) :
	// la deduction attrapait l'un ou l'autre, donc le verdict etait range dans le cloisonnement d'une
	// autre partie que celle ou les joueurs le cherchent. Mesure du 2026-08-16 : sur dix-huit lectures
	// de la collection @@RefereeResult, dix-sept ne voyaient RIEN et une seule voyait le document.
	//
	// L'identifiant est ecrit noir sur blanc apres le premier « _ » : on le lit au lieu de le deviner.
	if i := strings.IndexByte(suffixe, '_'); i > 0 && i+1 < len(suffixe) {
		if candidat := suffixe[i+1:]; len(candidat) == 36 {
			gsid = candidat
		}
	}

	// Repartir les participants en deux equipes, dans l'ordre stable de leur rang.
	var uids []string
	g.mu.Lock()
	for _, s := range g.sess {
		if s != nil && s.gsid == gsid && s.uid != "" {
			uids = append(uids, s.uid)
		}
	}
	g.mu.Unlock()
	sort.Strings(uids)

	equipe1, equipe2 := []*commonpb.Value{}, []*commonpb.Value{}
	for i, u := range uids {
		if i%2 == 0 {
			equipe1 = append(equipe1, gsStr(u))
		} else {
			equipe2 = append(equipe2, gsStr(u))
		}
	}

	champs := map[string]*commonpb.Value{
		"request_id":     gsStr(suffixe),
		"league_part_id": gsStr(""),
		"season_id":      gsInt(16),
		"fest_id":        gsStr(""),
		"trace_id":       gsStr(strings.ReplaceAll(gsid, "-", "")),
		"teams": gsMap(map[string]*commonpb.Value{
			"team1": gsTableau(equipe1...),
			"team2": gsTableau(equipe2...),
			"team3": gsTableau(),
		}),
	}

	// REPRENDRE LE CONTENU DU RAPPORT.
	//
	// Decodage du 2026-08-16 : le rapport que la console ecrit ne porte que DEUX champs de haut
	// niveau — `team_result` (la carte des onze compteurs de la partie) et `personal_result` (la
	// carte de TOUS les joueurs). Le verdict, lui, en porte vingt-cinq. On reprend donc ces deux
	// cartes telles quelles et on fabrique le reste.
	for _, op := range ops {
		d := op.GetUpdateDocument().GetDocument()
		if d.GetName() != nomDemande {
			continue
		}
		for k, v := range d.GetFields().GetFields() {
			if _, deja := champs[k]; !deja {
				champs[k] = v
			}
		}

		// ⚠️ NE PAS IMBRIQUER LE RAPPORT SOUS SON AUTEUR. Le `personal_result` du rapport decrit deja
		// toute la partie ; c'est LUI la carte du verdict, pas une entree de celle-ci.
		if pr := d.GetFields().GetFields()["personal_result"]; pr != nil {
			if n := retenirRapport(suffixe, pr); n > 0 {
				log.Printf("[NPLN gamesync] arbitrage : rapport de %s fusionne — %d joueur(s) connu(s) pour le match %s",
					lastSeg(d.GetName()), n, suffixe)
			}
		}
	}

	// RENDRE AU VERDICT DE FIN L'IDENTITE DU MATCH.
	//
	// Le coup d'envoi et l'arrivee parlent du MEME match — meme `request_id`. Ce que le jeu nous a
	// annonce au depart (stage, mode, regle, joueurs) vaut encore a l'arrivee ; le rapport de fin ne
	// le repete pas, il ne porte que le resultat. On complete donc sans jamais ecraser : le cadre du
	// serveur passe en premier, puis le rapport de fin, puis les parametres du depart.
	//
	// ⚠️ La memorisation se fait ICI, avant les trois champs que le serveur tranche plus bas : sinon
	// le `deemed_result` du coup d'envoi (0, faute de resultat connu a cet instant) serait repris a
	// l'arrivee et figerait le verdict sur une egalite, alors que le rapport de fin porte le vrai.
	if finDePartie {
		repris := 0
		for k, v := range parametresDe(suffixe) {
			if _, deja := champs[k]; !deja {
				champs[k] = v
				repris++
			}
		}
		if repris > 0 {
			log.Printf("[NPLN gamesync] arbitrage : %d parametre(s) du coup d'envoi repris dans le verdict de fin du match %s", repris, suffixe)
		}
	} else {
		retenirParametres(suffixe, champs)
	}

	// `winner` vit DANS team_result, qui est une carte de onze compteurs — pas un entier. Nous le
	// lisions comme un entier de haut niveau : la lecture rendait toujours zero.
	vainqueur := champs["team_result"].GetMapValue().GetFields()["winner"].GetIntegerValue()

	// LE MODE NE SUFFIT PAS A SE DEDUIRE DU SALON — MESURE DU 2026-08-21.
	//
	// modeDuMatch lit roomSettingsByGsid, que l'apparieur remplit. Mais l'apparieur et l'arbitre
	// sont DEUX PROCESSUS : /data/nplns3 dans le conteneur npln pour le premier, /root/nplns3-gamesync
	// en systemd (NPLN_GAMESYNC_ONLY=1) pour le second. La carte est en memoire : ce que l'un y ecrit,
	// l'autre ne le lit jamais. cfgMatch revient donc TOUJOURS vide cote arbitre.
	//
	// En versus c'etait sans consequence, l'inventaire versus etant deja le defaut. En Salmon Run
	// c'etait fatal : la premiere partie coop du 21/08 s'est vu retirer [challenge_grade_point job_id
	// scenario shift_id shift_time], et le jeu a recu un verdict de Guerre de territoire — sans note,
	// sans quart, sans scenario. Les joueurs ont fini la partie avec zero point, et leur catalogue
	// n'a pas bouge (catalog_point vit dans personal_result, construit lui aussi en forme versus).
	//
	// Le verdict se nomme lui-meme : ces cinq champs, aucune partie versus ne les porte. On lit donc
	// le contenu, ce qui ne depend d'aucun partage de memoire entre les deux processus.
	cfgMatch := modeDuMatch(suffixe)
	coop := strings.Contains(cfgMatch, "coop") || verdictCoop(champs)
	if coop && !strings.Contains(cfgMatch, "coop") {
		log.Printf("[NPLN gamesync] arbitrage : match %s reconnu COOP a ses champs (le salon ne le disait pas)", suffixe)
	}

	if carte, n := resultatsIndividuels(suffixe, vainqueur, coop); carte != nil {
		champs["personal_result"] = carte
		log.Printf("[NPLN gamesync] arbitrage : personal_result de %d joueur(s), equipe gagnante=%d, match %s", n, vainqueur, suffixe)
	}

	// ⚠️ LE CHAMP QUI MANQUAIT : `status`.
	//
	// Decodage du 2026-08-16 : Nintendo ne publie pas deux documents mais UN SEUL, pousse trois fois
	// sous le meme nom, et c'est `status` qui les distingue — -1, puis 0 au coup d'envoi, puis 1
	// quand la partie est arbitree. `personal_result` est present dans les trois etats : ce n'est
	// donc pas lui le signal de fin, contrairement a ce que je croyais.
	//
	// Nous n'avons JAMAIS envoye ce champ. L'ecran de resultat recevait un verdict complet mais sans
	// l'unique marqueur qui dit « la partie est terminee », et attendait un etat qui n'arrivait pas.
	if finDePartie {
		champs["status"] = gsInt(1)
	} else {
		champs["status"] = gsInt(0)
	}
	if _, deja := champs["tournament_id"]; !deja {
		champs["tournament_id"] = gsStr("")
	}

	// CREDITER LA CARTE DE POINTS DU QUART — le Bonus Meter de Salmon Run.
	//
	// Mesure du 2026-08-21 : le jeu lit ses deux cartes (PointCardTotal/Data et
	// PointCardRegular/<creneau>) et ne les ecrit JAMAIS. Chez Nintendo c'est le serveur qui les
	// tient — la capture montre « 0 puis 509 octets » sur la carte du creneau. Sans ce credit, le
	// compteur des joueurs reste a zero quoi qu'ils fassent. Voir coop_carte_points.go.
	//
	// On credite AVANT le filtre d'inventaire : job_type_string n'y survit pas, et la carte le porte.
	if coop && finDePartie {
		crediterQuartCoop(suffixe, champs, func(session string) string {
			g.mu.Lock()
			defer g.mu.Unlock()
			if info := g.sess[session]; info != nil {
				return info.uid
			}
			return ""
		})
	}

	// N'ENVOYER QUE LES VINGT-CINQ CHAMPS DE NINTENDO — mais SEULEMENT en versus.
	//
	// La demande @@RefereeRequest en porte quatre que le verdict ne reprend pas : battle_id,
	// event_type, event_detail et season. Les reconduire, c'etait ajouter au verdict des champs
	// qu'aucune capture n'y montre. On s'en tient a l'inventaire lu.
	//
	// ⚠️ Cet inventaire vient d'une capture de GUERRE DE TERRITOIRE. Applique a un match Salmon Run,
	// il supprimerait TOUS ses champs propres (vagues, oeufs, boss) et rendrait un verdict vide.
	// Nous n'avons aucune capture de verdict coop : plutot que de deviner sa forme, on ne retire
	// rien et on NOMME ce que le jeu nous envoie — c'est le jeu lui-meme qui nous apprend
	// l'inventaire, exactement comme pour le versus.
	inventaire := champsDuVerdict
	if coop {
		inventaire = champsDuVerdictCoop
	}
	ecartes := make([]string, 0, 4)
	for k := range champs {
		if !inventaire[k] {
			ecartes = append(ecartes, k)
			delete(champs, k)
		}
	}
	if len(ecartes) > 0 {
		sort.Strings(ecartes)
		log.Printf("[NPLN gamesync] arbitrage : %d champ(s) hors inventaire %s ecarte(s) du match %s = %v",
			len(ecartes), lastSeg(cfgMatch), suffixe, ecartes)
	}

	doc := &commonpb.MapValue{Fields: champs}
	noms := make([]string, 0, len(champs))
	for k := range champs {
		noms = append(noms, k)
	}
	sort.Strings(noms)
	manquants := []string{}
	for k := range champsDuVerdict {
		if _, ok := champs[k]; !ok {
			manquants = append(manquants, k)
		}
	}
	sort.Strings(manquants)
	log.Printf("[NPLN gamesync] verdict %s : status=%d, %d octets, %d/25 champs = %s | MANQUE: %s",
		suffixe, champs["status"].GetIntegerValue(), proto.Size(doc), len(noms),
		strings.Join(noms, " "), strings.Join(manquants, " "))

	// L'HISTORIQUE EN JEU. Le jeu ne lit pas ses batailles passees dans la sauvegarde : il
	// interroge la collection GameRecord/users/<uid>/VsResults, et c'est le SERVEUR qui doit
	// y deposer un document par bataille — la console n'en ecrit aucun. Voir
	// vsresults_historique.go. Le suffixe est deja au format battle_id de Nintendo.
	if finDePartie {
		publierHistoriqueDeBataille(suffixe, gsid, uids, doc)
		// L'INTERMEDE se compte ici : c'est la seule fois ou l'on connait le vainqueur d'une
		// bataille de fete. Voir fest_intermede.go — c'est lui qui designera le defenseur.
		festID := doc.GetFields()["fest_id"].GetStringValue()
		compterLaBatailleDeFete(festID, doc)
		compterLaFerveurDeLaBataille(festID, doc)
	}

	return gsid, "docs/@@RefereeResult/" + suffixe, doc
}

// gsidPourEcriture resout la partie a laquelle appartient un lot d'ecritures.
//
// ⚠️ REGRESSION DU 2026-08-15, corrigee ici. Le cloisonnement du magasin deduisait la partie du nom
// du document. Or le jeu ecrit « docs/SessionInfo/<uid> », indexe par l'identifiant de COMPTE et non
// par celui de session : la deduction echouait et retombait sur « la session la plus recente du
// serveur » — celle de n'importe qui. L'ecriture partait dans la mauvaise partition, le joueur ne la
// voyait jamais revenir sur sa collection, et son jeu attendait une trentaine de secondes avant
// d'abandonner. D'ou une creation de salon prive qui marchait ou non selon qui s'etait connecte
// juste avant.
//
// On essaie donc, dans l'ordre : l'identifiant de session porte par le chemin, puis l'identifiant de
// COMPTE, rapproche des sessions connues. Rien n'est devine.
func (g *gamesyncServer) gsidPourEcriture(ops []*gspb.WriteOperation) string {
	if uss := ussFromOps(ops); uss != "" {
		if info := g.lookup(uss); info != nil {
			return info.gsid
		}
	}
	for _, uid := range uidsDansOps(ops) {
		g.mu.Lock()
		for _, s := range g.sess {
			if s != nil && s.uid == uid && s.gsid != "" {
				gsid := s.gsid
				g.mu.Unlock()
				return gsid
			}
		}
		g.mu.Unlock()
	}
	return ""
}

func (g *gamesyncServer) sessionPourUID(uid string) (string, string) {
	if uid == "" {
		return "", ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if info := g.sess[g.lastUss]; info != nil && info.uid == uid {
		return g.lastUss, info.gsid
	}
	var uss string
	var meilleur *gsSessionInfo
	for cle, info := range g.sess {
		if info == nil || info.uid != uid {
			continue
		}
		if meilleur == nil || info.rang > meilleur.rang || info.rang == meilleur.rang && cle < uss {
			uss = cle
			meilleur = info
		}
	}
	if meilleur == nil {
		return "", ""
	}
	return uss, meilleur.gsid
}

func gamesyncIdentityFromCtx(ctx context.Context) (string, string, string) {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, authorization := range md.Get("authorization") {
		authorization = strings.TrimSpace(authorization)
		authorization = strings.TrimPrefix(strings.TrimPrefix(authorization, "Bearer "), "bearer ")
		parts := strings.Split(authorization, ".")
		if len(parts) != 3 || !verifyNplnAccessToken(parts[0], parts[1], parts[2]) {
			continue
		}
		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			continue
		}
		var claims struct {
			Sub string `json:"sub"`
			Gss struct {
				GameSession string `json:"game_session"`
				UserSession string `json:"user_session"`
			} `json:"gss"`
			Gamesync struct {
				GSID string `json:"gsid"`
				USID string `json:"usid"`
				UID  string `json:"uid"`
			} `json:"gamesync"`
		}
		if json.Unmarshal(payload, &claims) != nil {
			continue
		}
		uid, uss, gsid := claims.Sub, "", ""
		if claims.Gss.UserSession != "" {
			uss = lastSeg(claims.Gss.UserSession)
			gsid = lastSeg(claims.Gss.GameSession)
		}
		if claims.Gamesync.USID != "" {
			uss = claims.Gamesync.USID
		}
		if claims.Gamesync.GSID != "" {
			gsid = claims.Gamesync.GSID
		}
		if claims.Gamesync.UID != "" {
			uid = claims.Gamesync.UID
		}
		if uss != "" || uid != "" {
			return uid, uss, gsid
		}
	}
	return "", "", ""
}

func (g *gamesyncServer) contexteEcriture(ctx context.Context, ops []*gspb.WriteOperation) (string, string) {
	uss := ussFromOps(ops)
	gsid := g.gsidPourEcriture(ops)
	uid, tokenUss, tokenGsid := gamesyncIdentityFromCtx(ctx)
	if tokenUss != "" {
		g.mu.Lock()
		info := g.sess[tokenUss]
		g.mu.Unlock()
		if info != nil {
			uss = tokenUss
			if gsid == "" {
				gsid = info.gsid
			}
		}
	}
	if uid == "" {
		uid = uidFromCtx(ctx)
	}
	if uss != "" {
		g.mu.Lock()
		info := g.sess[uss]
		g.mu.Unlock()
		if info != nil {
			if gsid == "" {
				gsid = info.gsid
			}
			return uss, gsid
		}
		if session, partie := g.sessionPourUID(uss); session != "" {
			uss = session
			if gsid == "" {
				gsid = partie
			}
		}
	}
	if session, partie := g.sessionPourUID(uid); session != "" {
		if uss == "" {
			uss = session
		}
		if gsid == "" {
			gsid = partie
		}
	}
	if gsid == "" {
		gsid = tokenGsid
	}
	return uss, gsid
}

// uidsDansOps rend les identifiants de compte (« u-… ») cites par les documents ecrits.
func uidsDansOps(ops []*gspb.WriteOperation) []string {
	var out []string
	for _, op := range ops {
		var name string
		switch {
		case op.GetUpdateDocument() != nil:
			name = op.GetUpdateDocument().GetDocument().GetName()
		case op.GetMergeDocument() != nil:
			name = op.GetMergeDocument().GetDocument().GetName()
		case op.GetTransformDocument() != nil:
			name = op.GetTransformDocument().GetName()
		case op.GetDeleteDocument() != nil:
			name = op.GetDeleteDocument().GetName()
		}
		for _, seg := range strings.Split(name, "/") {
			if strings.HasPrefix(seg, "u-") && len(seg) == 22 {
				out = append(out, seg)
			}
		}
	}
	return out
}

// gsidDe rend la partie a laquelle appartient une session utilisateur (vide si inconnue).
// A appeler HORS de g.mu : lookup prend le verrou lui-meme.
func (g *gamesyncServer) gsidDe(uss string) string {
	if info := g.lookup(uss); info != nil {
		return info.gsid
	}
	return ""
}

func cleStore(gsid, nom string) string {
	if gsid == "" {
		return nom // partie inconnue : ancien comportement, plutot que de perdre le document
	}
	return gsid + "\x00" + nom
}

// tracerEcriture — sous le drapeau « pltrace », journalise le nom du document et les champs
// REELLEMENT envoyes par la console, avec la taille des blobs d'octets. C'est le seul moyen de
// comparer notre montant a celui des captures Nintendo (SESSION-002/003/004), ou la console ecrit
// « pl » en octets (9, 78 puis 251).
func tracerEcriture(gsid, genre, nom string, champs *commonpb.MapValue) {
	if !soirFlag("pltrace") || nom == "" {
		return
	}
	cles := make([]string, 0, len(champs.GetFields()))
	for k, v := range champs.GetFields() {
		if b := v.GetBytesValue(); b != nil {
			cles = append(cles, fmt.Sprintf("%s=%do", k, len(b)))
		} else {
			cles = append(cles, k)
		}
	}
	sort.Strings(cles)
	log.Printf("[NPLN ecrit] %s %s { %s }", genre, nom, strings.Join(cles, " "))

	// Le contenu du blob « pl ». Mesure du 2026-08-25 sur les captures Nintendo : les blocs de
	// 78 octets portent un prefixe constant « 32 ab 98 64 8f », puis deux octets, puis ce qui
	// ressemble a un point de contact IPv4 + port. On journalise les octets BRUTS — pas une
	// interpretation — pour pouvoir les recouper avec l'adresse publique reelle de la console.
	if pl := champs.GetFields()["pl"]; pl != nil {
		if b := pl.GetBytesValue(); len(b) >= 20 {
			log.Printf("[NPLN blob] partie=%s console=%s %do %s", gsid, lastSeg(nom), len(b), hex.EncodeToString(b[:24]))
		}
	}
}

func (g *gamesyncServer) applyWritesToStore(gsid string, ops []*gspb.WriteOperation) {
	for _, op := range ops {
		switch {
		case op.GetUpdateDocument() != nil:
			tracerEcriture(gsid, "update", op.GetUpdateDocument().GetDocument().GetName(), op.GetUpdateDocument().GetDocument().GetFields())
		case op.GetMergeDocument() != nil:
			tracerEcriture(gsid, "merge", op.GetMergeDocument().GetDocument().GetName(), op.GetMergeDocument().GetDocument().GetFields())
		case op.GetTransformDocument() != nil:
			if soirFlag("pltrace") {
				ch := make([]string, 0, 4)
				for _, ft := range op.GetTransformDocument().GetFieldTransforms() {
					ch = append(ch, ft.GetFieldPath())
				}
				sort.Strings(ch)
				log.Printf("[NPLN ecrit] transform %s { %s }", op.GetTransformDocument().GetName(), strings.Join(ch, " "))
			}
		}
		switch {
		case op.GetUpdateDocument() != nil:
			d := op.GetUpdateDocument().GetDocument()
			if d.GetName() != "" {
				k := cleStore(gsid, d.GetName())
				if soirFlag("pltrace") && strings.Contains(d.GetName(), "/__stu/") {
					if pl := d.GetFields().GetFields()["pl"]; pl != nil {
						log.Printf("[NPLN pl] ecriture console %s <- %d octets", lastSeg(d.GetName()), len(pl.GetBytesValue()))
					}
				}
				g.store[k] = mergeFields(g.store[k], d.GetFields())
			}
		case op.GetMergeDocument() != nil:
			d := op.GetMergeDocument().GetDocument()
			if d.GetName() != "" {
				k := cleStore(gsid, d.GetName())
				g.store[k] = mergeFields(g.store[k], d.GetFields())
			}
		case op.GetTransformDocument() != nil:
			name := op.GetTransformDocument().GetName()
			if name == "" {
				break
			}
			k := cleStore(gsid, name)
			cur := g.store[k]
			if cur == nil {
				cur = &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
			} else if cur.Fields == nil {
				cur.Fields = map[string]*commonpb.Value{}
			}
			for _, ft := range op.GetTransformDocument().GetFieldTransforms() {
				if fp := ft.GetFieldPath(); fp != "" {
					cur.Fields[fp] = transformResultValue(ft)
				}
			}
			g.store[k] = cur
		case op.GetDeleteDocument() != nil:
			if name := op.GetDeleteDocument().GetName(); name != "" {
				delete(g.store, cleStore(gsid, name))
			}
		}
	}
}

// docsSousCollection rend les documents du store situes DIRECTEMENT sous une collection
// (« docs/SessionInfo » -> « docs/SessionInfo/<uid> », mais pas « docs/SessionInfo/a/b »).
//
// C'est ce qui manquait a Splatoon 3 : il ecrit docs/MasterCollection/Session,
// docs/SessionInfo/<uid> et docs/__us/<uss>, surveille les collections correspondantes, et
// attend de revoir ses propres ecritures. Nous ne repoussions que quatre documents fixes.
// dureeRappel : au bout de combien de temps sans aucune poussee on en refait une d'office, meme si
// rien n'a change. Filet de securite pour un client qui aurait manque un envoi. Reglable par
// « rappel=<secondes> ».
func dureeRappel() time.Duration {
	if v := soirFlagValeur("rappel"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}

	return 30 * time.Second
}

func (g *gamesyncServer) docsSousCollection(gsid, coll string) map[string]*commonpb.MapValue {
	prefixe := cleStore(gsid, strings.TrimRight(coll, "/")+"/")
	coupe := len(prefixe) - len(strings.TrimRight(coll, "/")+"/")
	out := map[string]*commonpb.MapValue{}

	g.mu.Lock()
	defer g.mu.Unlock()
	for nom, champs := range g.store {
		if !strings.HasPrefix(nom, prefixe) {
			continue
		}
		if strings.Contains(nom[len(prefixe):], "/") {
			continue // document d'une sous-collection, pas un membre direct
		}
		// Rendre le nom SANS le prefixe de partie : le client ne connait que « docs/… ».
		out[nom[coupe:]] = champs
	}
	return out
}

// mergeFields overlays src onto dst (a shallow field merge; src wins). Returns a fresh MapValue.
func mergeFields(dst, src *commonpb.MapValue) *commonpb.MapValue {
	out := &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
	if dst != nil {
		for k, v := range dst.GetFields() {
			out.Fields[k] = v
		}
	}
	if src != nil {
		for k, v := range src.GetFields() {
			out.Fields[k] = v
		}
	}
	return out
}

// wake nudges the given userSession's watch stream to re-push (non-blocking).
// reveillerLaPartie reveille TOUTES les consoles de la meme partie, pas seulement celle qui vient
// d'ecrire.
//
// Mesure du 2026-08-25, pendant la LoveFest : les consoles ecrivent bien leur « docs/SessionInfo/
// <uid> » (nom + done_match_step) et leur blob P2P « pl », mais WriteDocuments n'appelait que
// wake(uss) — la session de l'AUTEUR. Les sept autres consoles de la partie n'etaient jamais
// prevenues : chacune restait figee sur l'instantane pris a son arrivee. Le journal le montrait
// noir sur blanc — la collection docs/SessionInfo etait poussee avec 1, 2, 3, 4, 5, 6 ou 7 membres,
// JAMAIS 8, et toujours en « pre-LISTED ». D'ou l'effectif Pia annonce par les consoles (1, puis 2
// et 3 apres le relais du blob) qui n'atteignait jamais 8, et les salons qui se vident.
//
// L'invariant Firestore-Listen releve sur les captures Nintendo l'exige : UPDATED = changement VIVANT
// sur une cible deja LISTED. Il faut donc pousser aux autres au moment ou l'un ecrit.
func (g *gamesyncServer) reveillerLaPartie(uss string) {
	if uss == "" {
		return
	}
	g.wake(uss)
	if soirFlag("reveilsolo") { // coupe-circuit : revenir au reveil du seul auteur
		return
	}

	g.mu.Lock()
	moi := g.sess[uss]
	var canaux []chan struct{}
	if moi != nil && moi.gsid != "" {
		for u, s := range g.sess {
			if u == uss || s == nil || s.gsid != moi.gsid {
				continue
			}
			if ch := g.wakers[u]; ch != nil {
				canaux = append(canaux, ch)
			}
		}
	}
	g.mu.Unlock()

	for _, ch := range canaux {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	if len(canaux) > 0 && soirFlag("pltrace") {
		log.Printf("[NPLN reveil] ecriture de %s -> %d autre(s) console(s) de la partie prevenue(s)", uss, len(canaux))
	}
}

func (g *gamesyncServer) wake(uss string) {
	if uss == "" {
		return
	}
	g.mu.Lock()
	ch := g.wakers[uss]
	g.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// membersInCollection returns the store docs (name->fields) that live directly under a __pus collection
// base (e.g. "docs/__pgn/All/__pus"), i.e. the current presence members. Call under g.mu held.
func (g *gamesyncServer) membersInCollection(collBase string) map[string]*commonpb.MapValue {
	collBase = strings.TrimRight(collBase, "/")
	out := map[string]*commonpb.MapValue{}
	for name, f := range g.store {
		if strings.HasPrefix(name, collBase+"/") {
			out[name] = f
		}
	}
	return out
}

func (g *gamesyncServer) setStreamActive(uss string) {
	if uss == "" {
		return
	}
	g.mu.Lock()
	delete(g.closing, uss)
	g.mu.Unlock()
}

func (g *gamesyncServer) setStreamClosed(uss string) {
	if uss == "" {
		return
	}
	g.mu.Lock()
	g.closing[uss] = true
	g.mu.Unlock()
}

func (g *gamesyncServer) isClosing(uss string) bool {
	if uss == "" {
		return false
	}
	g.mu.Lock()
	c := g.closing[uss]
	g.mu.Unlock()
	return c
}

// ---- Firestore Value helpers (each sets a concrete oneof — required by the nn::npln worker) ----

func gsStr(s string) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_StringValue{StringValue: s}}
}
func gsInt(n int64) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_IntegerValue{IntegerValue: n}}
}
func gsBool(b bool) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_BooleanValue{BooleanValue: b}}
}
func gsMap(f map[string]*commonpb.Value) *commonpb.Value {
	if f == nil {
		f = map[string]*commonpb.Value{}
	}
	return &commonpb.Value{ValueType: &commonpb.Value_MapValue{MapValue: &commonpb.MapValue{Fields: f}}}
}
func gsDouble(f float64) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_DoubleValue{DoubleValue: f}}
}

// gsNul : la valeur NULLE explicite. Un champ que Nintendo publie a null n'est pas un champ absent —
// le verdict de fin porte toujours ses vingt-trois cles par joueur, certaines a null.
func gsNul() *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_NullValue{NullValue: structpb.NullValue_NULL_VALUE}}
}

// userSessionFields builds the UserSession MapValue the host's DocumentReader (@0xd88084)
// parses: uid/ussid/ucsid/upcsid/pgn (read there) plus st/att/ltc/tn (read by the fuller
// accessor set @0xd919xx). uss = the userSession uuid.
func userSessionFields(info *gsSessionInfo, uss string) *commonpb.MapValue {
	uid := capturedUser
	rang := 1
	if info != nil && info.uid != "" {
		uid = info.uid
	}
	if info != nil && info.rang > 0 {
		rang = info.rang
	}
	// Attributs REELS du participant, achemines depuis le matchmaking (voir attributsParUss).
	//
	// ⚠️ Sous drapeau « attrsroster », ETEINT par defaut. Ces attributs viennent du ticket de
	// matchmaking : ils contiennent des tableaux et des flottants, alors que le lecteur de ce
	// document attend des types precis. Or le crash mesure le 2026-08-14 est un abandon VOLONTAIRE
	// du jeu — nn::diag::detail::AbortImpl appele depuis le fil nn.npln.Worker, resultat
	// 2162-0001 — et le commentaire d'en-tete de ce fichier attribue deja ce crash exact a une
	// valeur mal typee dans ces documents. L'injection n'ayant apporte aucun gain mesurable, on ne
	// garde pas un risque de crash pour rien.
	att, ltc, tn := gsMap(nil), gsMap(nil), ""
	if a := participantAttrs(uss); a != nil && soirFlag("attrsroster") {
		if a.att != nil {
			att = gsMap(a.att.GetFields())
		}
		if a.ltc != nil {
			ltc = gsMap(a.ltc.GetFields())
		}
		tn = a.tn
	}
	return &commonpb.MapValue{Fields: map[string]*commonpb.Value{
		"uid":    gsStr(uid),
		"ussid":  gsInt(int64(rang)), // userSessionSeqId — DISTINCT par participant
		"ucsid":  gsInt(int64(rang)), // userConnectionSeqId
		"upcsid": gsInt(int64(rang)), // upcsid
		"pgn":    gsStr("All"),
		"st":     gsInt(2), // state = ACTIVE
		"tn":     gsStr(tn),
		"att":    att,
		"ltc":    ltc,
	}}
}

// stateUserFields builds the per-user State document (docs/__pgn/All/__stu/<uss>), which has a
// DIFFERENT schema than the UserSession: its reader (@0xd88960) parses suid/susid/sussid/suscid
// (strings) + pl/mp (maps). Previously we wrongly fed it userSessionFields (uid/ussid/…), so the
// per-user state readback was empty on the create/keep path.
func stateUserFields(info *gsSessionInfo, uss string) *commonpb.MapValue {
	uid := capturedUser
	if info != nil && info.uid != "" {
		uid = info.uid
	}
	champs := map[string]*commonpb.Value{
		"suid":  gsStr(uid),
		"susid": gsStr(uss),
		// sussid/suscid sont des ENTIERS dans la capture Nintendo (1 et 10001), pas des chaines.
		"sussid": gsInt(1),
		"suscid": gsInt(suscidDeBase + 1),
		// « pl » en OCTETS et non en map : c'est le type que porte la capture Nintendo (tag 0x42).
		// Ce repli ne sert qu'avant la premiere ecriture de la console ; passe ce moment c'est le
		// blob relaye qui prend la place (voir fieldsForDocCtx).
		"pl": gsBytes(nil),
	}
	if soirFlag("plmapvide") { // coupe-circuit : rendre a « pl » son ancienne map vide
		champs["pl"] = gsMap(nil)
	}
	// « mp » : ZERO occurrence dans les six captures Nintendo (montant et descendant des trois
	// sessions) — nous l'inventions. Le drapeau le remet si un lecteur du jeu le reclamait.
	if soirFlag("mpinvente") {
		champs["mp"] = gsMap(nil)
	}
	return &commonpb.MapValue{Fields: champs}
}

// gameSessionMutableFields builds the GameSessionMutableData MapValue (docs/__stg/All), the
// data the host reads right after finding its UserSession (reader @0xd6273c). Solo/empty room:
// one participant (the host), pointing at our relay.
// gameSessionMutableFields construit les donnees mutables de la partie que l'hote relit.
// effectif = nombre de participants REELLEMENT publies (0 = inconnu, on garde la capacite figee).
//
// Drapeau a chaud « salleplein » : annoncer maxu/bfmax egaux a l'effectif present, pour que le jeu
// considere la salle comme COMPLETE. Mesure du 2026-08-14 : a deux joueurs la recherche tourne
// indefiniment sans la moindre erreur — le jeu attend simplement d'atteindre la capacite annoncee,
// figee a 4. Une Guerre de territoire se joue a 4 contre 4 ; sans huit consoles, c'est le seul
// moyen de voir une partie DEMARRER.
func gameSessionMutableFieldsN(info *gsSessionInfo, effectif int) *commonpb.MapValue {
	gsid, addr, port, maxp := "", "", 0, 4
	password, isPublic := "", true
	mcn := ""
	if effectif > 0 && soirFlag("salleplein") {
		maxp = effectif
	}
	if info != nil {
		gsid, addr, port = info.gsid, info.host, info.port
		if info.maxp > 0 && !(effectif > 0 && soirFlag("salleplein")) {
			maxp = info.maxp
		}
		password, isPublic = info.password, info.isPublic
		mcn = info.config
		if mcn == "" {
			mcn = roomConfigFor(info.gsid)
		}
	}
	return &commonpb.MapValue{Fields: map[string]*commonpb.Value{
		"gsid": gsStr(gsid),
		"addr": gsStr(addr),
		"p":    gsInt(int64(port)),
		// mcn = nom de la configuration de la partie. Il partait VIDE : le lecteur
		// GameSessionMutableData du jeu (@0xd6273c) l'analyse entre le port et maxu, et une chaine
		// vide ne lui dit pas quel mode il est en train de tenir. Mesure du 2026-08-14 : le salon
		// prive se cree bien (Result "succeeded"), puis le jeu attend ~90 s — 89 s et 92 s sur deux
		// essais — avant de rendre une erreur APPLICATIVE, ce qui est le profil d'une condition
		// jamais satisfaite, pas d'une panne reseau.
		"mcn":  gsStr(mcn),
		"maxu": gsInt(int64(maxp)),
		// rs = the room-settings sub-map. The host's __stg reader (@0xd6273c) reads `rs`
		// as a NESTED map and parses it field-by-field with @0xd62214 on the CreateNplnSession
		// path: cp/ip/ebf(bool) pw(str) bfmin/bfmax(int) prp(map). Sending these flat at the
		// top level (as before) left the session-settings readback empty, so the gamesync
		// sequence never advanced the operation phase to 6 → IsSessionConnected never true
		// → create-session progress stuck at 1 forever.
		"rs": gsMap(map[string]*commonpb.Value{
			"cp":    gsBool(false),      // (bool)
			"ip":    gsBool(isPublic),   // is public (false when a password is set -> host shows the lock)
			"pw":    gsStr(password),    // password ("1234" etc.) -> host shows "password on"
			"ebf":   gsBool(false),      // (bool)
			"bfmin": gsInt(1),           // (int) — the host alone satisfies the lower bound
			"bfmax": gsInt(int64(maxp)), // (int)
			"prp":   gsMap(nil),         // properties
		}),
	}}
}

// gameSessionMutableFields : effectif inconnu (comportement d'origine).
func gameSessionMutableFields(info *gsSessionInfo) *commonpb.MapValue {
	return gameSessionMutableFieldsN(info, 0)
}

func gsBytes(b []byte) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_BytesValue{BytesValue: b}}
}

func gsTime(t time.Time) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
}

// dureeVieSalon : l'ecart mesure entre create_time et __gs/n.ett dans la capture Nintendo du salon
// prive (1786921254 - 1786748454 = 172800 s). Exactement 48 heures.
const dureeVieSalon = 48 * time.Hour

// champsGs construit les six documents docs/__gs/* au schema EXACT de Nintendo.
//
// Mesure du 2026-08-15 — capture du salon prive (relais-session, SESSION-002-136.114.7.187,
// messages 16 a 21 du flux descendant, cible 6) :
//
//	__gs/f   fiche fixe      addr, gsid, maxu, mcn, p, rs (32 octets), tid
//	__gs/m   reglages vivants bfmax, bfmin, cp, ebf, ip, prp{...}, pw
//	__gs/s   copie stable     memes champs que m
//	__gs/r   effectif         ipc (presents), dpc (partis)
//	__gs/n   expiration       etn (bool), ett (timestamp) = creation + 48 h
//	__gs/ck  mot de passe     k
//
// Nous servions a la place huit champs INVENTES — name, host, hostUser, owner, creator,
// gameSession, gameSessionId, id — dont AUCUN n'apparait dans un seul de ces six documents. Le jeu
// lisait donc six documents presents mais vides de sens, en particulier __gs/r.ipc, le compteur de
// joueurs sur lequel il s'appuie pour afficher la salle.
//
// `effectif` = nombre de participants reellement publies (0 = inconnu : on annonce l'hote seul).
func (g *gamesyncServer) champsGs(sous string, info *gsSessionInfo, effectif int) *commonpb.MapValue {
	gsid, addr, port, mcn := "", "", 0, ""
	password := ""
	var prp *commonpb.MapValue
	if info != nil {
		gsid, addr, port = info.gsid, info.host, info.port
		mcn = info.config
		if mcn == "" {
			mcn = roomConfigFor(info.gsid)
		}
		password, _, _ = roomSettingsFor(info.gsid)
		prp = proprietesSalon(info.gsid)
	}
	maxu := int(s3RoomCapacity(mcn, 8))
	if info != nil && info.maxp > 0 {
		maxu = info.maxp
	}
	if effectif < 1 {
		effectif = 1 // l'hote est toujours la
	}

	// Drapeau a chaud « salleplein » : annoncer une capacite egale a l'effectif REEL.
	//
	// Mesure du 2026-08-15, Guerre de territoire a deux testeurs : la partie se forme, les deux
	// joueurs partagent le meme gsid et le roster se publie — puis le jeu reste « en attente ». Il
	// lit ipc=2 pour maxu=8 et attend six personnes qui n'existent pas. Les donnees mutables
	// honoraient deja ce drapeau ; ces six documents-ci, ecrits ce soir, ne le faisaient pas, si
	// bien que la salle se decrivait a moitie pleine dans un document et complete dans l'autre.
	//
	// ⚠️ JAMAIS sur un salon prive. Mesure du 2026-08-15, capture d'une jonction REELLE chez Nintendo
	// (JoinGameSessionResponse, game_session) : max_participant_count = 10 et
	// current_participant_count = 2. Nintendo garde donc la CAPACITE du mode et annonce l'effectif
	// reel a cote — il ne retrecit pas la salle. Rabaisser maxu a 2 ici ferait decrire un salon prive
	// de deux places la ou le jeu en attend dix, et c'est l'ecran meme que nous cherchons a reparer.
	// Le drapeau reste utile en Guerre de territoire, ou l'on ne reunira jamais huit consoles.
	if soirFlag("salleplein") && !strings.Contains(mcn, "private_match") {
		maxu = effectif
	}

	// Les reglages : identiques dans __gs/m et __gs/s (verifie champ par champ dans la capture).
	//
	// bfmax vaut 7 pour un salon prive de capacite 10 — ce n'est donc PAS la capacite, mais le
	// nombre de places encore ouvertes au remplissage (10 moins les 2 spectateurs et l'hote, ou une
	// borne propre au mode). On reproduit la valeur telle que mesuree plutot que de la deduire.
	reglages := func() *commonpb.MapValue {
		f := map[string]*commonpb.Value{
			"bfmax": gsInt(int64(maxu - 3)),
			"bfmin": gsInt(1),
			"cp":    gsBool(true), // can participate — VRAI chez Nintendo ; nous envoyions faux
			"ebf":   gsBool(false),
			"ip":    gsBool(true),
			"pw":    gsStr(password),
			"prp":   gsMap(nil),
		}
		if maxu-3 < 1 {
			f["bfmax"] = gsInt(1)
		}
		if prp != nil {
			f["prp"] = gsMap(prp.GetFields())
		}
		return &commonpb.MapValue{Fields: f}
	}

	switch sous {
	case "f":
		// rs = 32 octets de secret de salon. Nintendo en tire une valeur aleatoire ; nous la derivons
		// du gsid pour qu'elle soit STABLE entre deux lectures du meme salon (le jeu recoupe ce
		// document entre la surveillance et les lectures ponctuelles : deux valeurs differentes pour
		// un meme salon le feraient echouer).
		somme := sha256.Sum256([]byte("nextendo-gs-rs:" + gsid))
		return &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			"addr": gsStr(addr),
			"gsid": gsStr(gsid),
			"maxu": gsInt(int64(maxu)),
			"mcn":  gsStr(mcn),
			"p":    gsInt(int64(port)),
			"rs":   gsBytes(somme[:]),
			// tid = l'identifiant NU du locataire (« t-dce9377b-lp1 »), sans le prefixe « tenants/ ».
			"tid": gsStr(lastSeg(npnTenant)),
		}}
	case "m", "s":
		return reglages()
	case "r":
		// ipc = joueurs presents, dpc = joueurs partis. C'est LE compteur d'effectif du salon.
		return &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			"ipc": gsInt(int64(effectif)),
			"dpc": gsInt(0),
		}}
	case "n":
		return &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			"etn": gsBool(false),
			"ett": gsTime(time.Now().Add(dureeVieSalon)),
		}}
	case "ck":
		// k = la cle de controle du mot de passe. Vide chez Nintendo quand le salon est libre.
		return &commonpb.MapValue{Fields: map[string]*commonpb.Value{"k": gsStr(password)}}
	}
	return nil
}

// decodeSessionToken pulls the host identity + game-session out of the matchmaking id-token
// (an ES256 JWT minted by CreateGameSessionCreationTicket). We only READ claims here (no verify
// — it's our own token), so a plain base64 payload decode is enough.
func decodeSessionToken(tok string) (uid, gsName string) {
	uid, gsName, _ = decodeSessionTokenWithRoute(tok)
	return uid, gsName
}

func decodeSessionTokenWithRoute(tok string) (uid, gsName string, route gssSessionRoute) {
	parts := strings.Split(tok, ".")
	if len(parts) < 2 {
		return "", "", route
	}
	pb, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", route
	}
	var m struct {
		Sub string `json:"sub"`
		// Nintendo's shape (capture: TrackGameSessionCreationTicket.bin, MatchedUserSession.3):
		// iss "gss" + a `gamesync` block holding the RAW uuids the client needs.
		Gamesync struct {
			GSID string `json:"gsid"`
			USID string `json:"usid"`
			UID  string `json:"uid"`
			TID  string `json:"tid"`
			MCN  string `json:"mcn"`
			Host string `json:"relay_host"`
			Port int32  `json:"relay_port"`
		} `json:"gamesync"`
		// Our former shape, kept so a token minted before this change still resolves.
		Gss struct {
			GameSession string `json:"game_session"`
			UserSession string `json:"user_session"`
		} `json:"gss"`
	}
	if json.Unmarshal(pb, &m) != nil {
		return "", "", route
	}
	if g := m.Gamesync; g.GSID != "" {
		route = gssSessionRoute{Config: g.MCN, Host: g.Host, Port: g.Port}
		uid = g.UID
		if uid == "" {
			uid = m.Sub
		}
		// gsid is the bare uuid; rebuild the full resource name the rest of the server expects.
		tid := g.TID
		if tid == "" {
			tid = strings.TrimPrefix(npnTenant, "tenants/")
		}
		return uid, "tenants/" + tid + "/gameSessions/" + g.GSID, route
	}
	return m.Sub, m.Gss.GameSession, route
}

// participantsDeLaPartie rend les userSession (uuid -> info) qui partagent le MEME gameSession que
// celle passee en argument, la sienne comprise.
//
// Mesure du 2026-08-14, Guerre de territoire a deux joueurs : contrairement au salon prive, le jeu
// n'ECRIT RIEN — il surveille docs/__us, docs/__gs, docs/@@RefereeResult et __pus, puis ATTEND que
// le serveur lui publie les participants. Nous ne poussions a chacun que SA propre session : les
// deux joueurs etaient bien dans la meme partie sans jamais se voir.
func (g *gamesyncServer) participantsDeLaPartie(uss string) map[string]*gsSessionInfo {
	g.mu.Lock()
	defer g.mu.Unlock()

	moi := g.sess[uss]
	if moi == nil || moi.gsid == "" {
		return nil
	}
	out := map[string]*gsSessionInfo{}
	for u, info := range g.sess {
		if info != nil && info.gsid == moi.gsid {
			out[u] = info
		}
	}
	return out
}

type membreDoc struct {
	nom    string
	champs *commonpb.MapValue
}

// membresDeCollection synthetise les membres d'une collection surveillee.
//
// Mesure du 2026-08-15, flux MONTANT du salon prive (SESSION-002-136.114.7.187, flux 3, 20
// demandes) : le jeu ne demande presque jamais une liste de documents nommes — il surveille des
// COLLECTIONS. Ses onze cibles :
//
//	1  documents  docs/__us/<moi>            (puis supprimee aussitot)
//	2  collection docs/SessionInfo
//	3  collection docs/Event
//	4  collection docs/SessionInfo
//	5  collection docs/__us                  <- le roster
//	6  collection docs/__gs                  <- les six documents du salon
//	7  collection docs/MasterCollection
//	8  collection docs/@@RefereeResult
//	9  documents  docs/__pgn/All/__stu/<moi>
//	10 documents  docs/__stg/All
//	11 collection docs/__pgn/All/__pus
//
// Nous ne synthetisions de membre que pour __pus ; partout ailleurs nous ne servions que ce que le
// jeu avait lui-meme ECRIT. Or en Guerre de territoire il n'ecrit rien : docs/__us et docs/__gs
// partaient donc vides a chaque instantane. C'est la raison pour laquelle deux joueurs d'une meme
// partie ne se sont jamais vus, et pour laquelle le salon prive n'a jamais recu ses six documents.
//
// L'ordre est alphabetique, comme chez Nintendo (ck, f, m, n, r, s pour __gs ; uuid croissants pour
// __us).
func (g *gamesyncServer) membresDeCollection(coll, uss string) []membreDoc {
	participants := g.participantsDeLaPartie(uss)
	if len(participants) == 0 && uss != "" {
		participants = map[string]*gsSessionInfo{uss: g.lookup(uss)}
	}

	var noms []string
	switch {
	case strings.HasSuffix(coll, "/__us"), strings.HasSuffix(coll, "/__pus"):
		for u := range participants {
			noms = append(noms, coll+"/"+u)
		}
	case strings.HasSuffix(coll, "/__gs"):
		for _, s := range []string{"ck", "f", "m", "n", "r", "s"} {
			noms = append(noms, coll+"/"+s)
		}
	case strings.HasSuffix(coll, "/MasterCollection"):
		noms = append(noms, coll+"/Session")
	default:
		// docs/Event, docs/SessionInfo, docs/@@RefereeResult : legitimement vides tant que le jeu n'a
		// rien ecrit. Dans la capture, docs/SessionInfo est d'ailleurs valide VIDE, puis le document
		// de l'hote y arrive en UPDATED une fois qu'il l'a ecrit.
		return nil
	}

	sort.Strings(noms)
	out := make([]membreDoc, 0, len(noms))
	for _, n := range noms {
		// fieldsForDoc pour que chaque membre passe par le MEME constructeur que les lectures
		// ponctuelles : l'hote recoupe les deux, ils doivent coincider au champ pres.
		out = append(out, membreDoc{nom: n, champs: g.fieldsForDocCtx(n, uss)})
	}
	return out
}

// effectifPublie rend le nombre de participants presents dans la partie de `uss` — la valeur que
// Nintendo porte dans docs/__gs/r.ipc.
//
// Le chemin « docs/__gs/f » ne porte AUCUN identifiant de session : on ne peut pas compter a partir
// du nom du document. On resout donc la partie par lookup (qui retombe sur la session la plus
// recente quand uss est vide), puis on compte les sessions qui partagent son gsid.
func (g *gamesyncServer) effectifPublie(uss string) int {
	info := g.lookup(uss)
	if info == nil || info.gsid == "" {
		return 0
	}
	g.mu.Lock()
	n := 0
	for _, s := range g.sess {
		if s != nil && s.gsid == info.gsid {
			n++
		}
	}
	g.mu.Unlock()

	noterEffectifServi(uss, info.gsid, n)

	return n
}

// suiviEffectif retient le dernier effectif annonce a chaque console, pour ne journaliser QUE les
// CHANGEMENTS — le recalcul a lieu a chaque envoi, en journaliser chacun noierait tout le reste.
//
// POURQUOI CETTE TRACE EXISTE. Mesure du 2026-08-16 : sur 22 matchs formes a huit joueurs, 2
// seulement sont parvenus jusqu'a l'arbitre ; les vingt autres se sont vides joueur par joueur, par
// des fermetures propres cote client. Tout le reste est identique entre un salon qui demarre et un
// salon qui meurt — meme roster servi, memes ecritures des huit consoles, meme volume de poussees.
//
// Le seul chiffre qu'on ne voyait PAS est celui-ci : `ipc`, l'effectif que porte le document
// docs/__gs/r et que chaque console relit pour savoir si son salon est complet. Il est recalcule a
// chaque envoi a partir des sessions CONNECTEES : une console servie pendant qu'un pair se
// reconnecte lit un salon incomplet. On ne peut ni le confirmer ni l'ecarter sans le mesurer.
var suiviEffectif = struct {
	sync.Mutex
	m map[string]int
}{m: map[string]int{}}

func noterEffectifServi(uss, gsid string, n int) {
	suiviEffectif.Lock()
	ancien, connu := suiviEffectif.m[uss]
	suiviEffectif.m[uss] = n
	suiviEffectif.Unlock()

	if connu && ancien == n {
		return
	}

	if connu {
		log.Printf("[NPLN gamesync] effectif annonce : %d -> %d joueur(s) | uss=%s partie=%s", ancien, n, uss, gsid)
	} else {
		log.Printf("[NPLN gamesync] effectif annonce : %d joueur(s) (premiere lecture) | uss=%s partie=%s", n, uss, gsid)
	}
}

func oublierEffectifServi(uss string) {
	suiviEffectif.Lock()
	delete(suiviEffectif.m, uss)
	suiviEffectif.Unlock()
}

// lookup resolves the session info for a watched doc/collection whose path carries the
// userSession uuid; falls back to the most-recent session (solo case).
func (g *gamesyncServer) lookup(uss string) *gsSessionInfo {
	g.mu.Lock()
	defer g.mu.Unlock()
	if info := g.sess[uss]; info != nil {
		return info
	}
	return g.last
}

func (g *gamesyncServer) issueToken(userSession string) *gspb.Token {
	tok := mintSessionToken("", tenantOfName(userSession), gsNameOfUserSession(userSession), userSession)
	return &gspb.Token{
		UserSession:  userSession,
		AccessToken:  tok,
		RefreshToken: tok,
		Ttl:          durationpb.New(8 * time.Hour),
	}
}

func (g *gamesyncServer) IssueToken(ctx context.Context, req *gspb.IssueTokenRequest) (*gspb.IssueTokenResponse, error) {
	log.Printf("[NPLN gamesync] IssueToken user_session=%q mmtoken=%.20s…", req.GetUserSession(), req.GetMatchmakingIdToken())

	// Decode the matchmaking token to learn WHO the host is and WHICH game session this is, then
	// cache it under the userSession uuid so the KeepUserSession watches can synthesize the docs.
	if g.sess == nil {
		g.mu.Lock()
		if g.sess == nil {
			g.sess = map[string]*gsSessionInfo{}
		}
		g.mu.Unlock()
	}
	uid, gsName, route := decodeSessionTokenWithRoute(req.GetMatchmakingIdToken())
	relayHost, relayPort, _, _, _, _, _, _, _ := mmConfig()
	if route.Host != "" {
		relayHost = route.Host
	}
	if route.Port > 0 {
		relayPort = route.Port
	}
	gsid := lastSeg(gsName)
	config := route.Config
	if config == "" {
		config = roomConfigFor(gsid)
	}
	// Rebuild the matchmaker's process-local settings from the signed ticket. The 443
	// matchmaker and this :7575 listener are separate processes, so its roomSettingsByGsid
	// map is otherwise empty here; that left mcn blank in the session documents.
	if config != "" {
		rememberRoomSettings(gsid, "", true, config, nil)
	}
	info := &gsSessionInfo{
		uid:    uid,
		gsName: gsName,
		gsid:   gsid,
		config: config,
		host:   relayHost,
		port:   int(relayPort),
		// The signed ticket carries the config so the separate process can give the session its
		// real capacity and mode, rather than falling back to an empty config.
		maxp:     int(s3RoomCapacity(config, 8)),
		isPublic: true, // default: public/no-password (overridden below if the room set one)
	}
	if pw, pub, ok := roomSettingsFor(info.gsid); ok {
		info.password = pw
		info.isPublic = pub
	}
	uss := lastSeg(req.GetUserSession())

	// [Nextendo] Un joueur qui OUVRE une nouvelle session a forcement abandonne les precedentes.
	//
	// Mesure du 2026-08-14 : quand le jeu echoue, il ne se fige pas — il REBOUCLE (deux IssueToken
	// et deux KeepUserSession en dix minutes) tout en gardant ses anciens flux ouverts. Nos sessions
	// n'etant liberees qu'a la fermeture du flux, les precedentes restaient vivantes : le roster
	// publiait des participants morts et le monitoring affichait un salon occupe par des fantomes.
	// On ne devine aucun delai ici : l'arrivee d'une nouvelle session du MEME uid suffit a conclure.
	var perimees []string
	g.mu.Lock()
	for u, autre := range g.sess {
		if u != uss && autre != nil && autre.uid == uid && uid != "" {
			perimees = append(perimees, u)
		}
	}
	g.mu.Unlock()
	for _, u := range perimees {
		log.Printf("[NPLN gamesync] session %s perimee (le joueur %s en a ouvert une nouvelle)", u, uid)
		g.oublierSession(u)
	}

	g.mu.Lock()
	// Rang du participant dans SA partie : le plus grand rang deja attribue, plus un. Une session
	// qui se reconnecte garde le sien.
	if ancien := g.sess[uss]; ancien != nil && ancien.rang > 0 {
		info.rang = ancien.rang
	} else {
		max := 0
		for _, autre := range g.sess {
			if autre != nil && autre.gsid == info.gsid && autre.rang > max {
				max = autre.rang
			}
		}
		info.rang = max + 1
	}
	g.sess[uss] = info
	g.last = info
	g.lastUss = uss
	g.mu.Unlock()
	log.Printf("[NPLN gamesync] IssueToken cached session uss=%s uid=%s gsid=%s config=%s maxp=%d relay=%s:%d",
		uss, info.uid, info.gsid, info.config, info.maxp, info.host, info.port)

	return &gspb.IssueTokenResponse{Token: g.issueToken(req.GetUserSession())}, nil
}

func (g *gamesyncServer) RefreshToken(ctx context.Context, req *gspb.RefreshTokenRequest) (*gspb.RefreshTokenResponse, error) {
	log.Printf("[NPLN gamesync] RefreshToken user_session=%q", req.GetUserSession())
	return &gspb.RefreshTokenResponse{Token: g.issueToken(req.GetUserSession())}, nil
}

// fieldsForDoc picks the right Firestore MapValue for a document name by the collection it lives
// in. uss = the userSession uuid (from the doc path, when present) so the host's own session is
// filled in. The SAME builder feeds the watch snapshots (KeepUserSession) AND the point reads
// (GetDocument/ReadDocuments) — the host cross-checks them, so they must agree.
func (g *gamesyncServer) fieldsForDoc(docName string) *commonpb.MapValue {
	return g.fieldsForDocCtx(docName, "")
}

// fieldsForDocCtx : idem, avec la session du FLUX en secours.
//
// Les documents de salon (docs/__gs/f, docs/__gs/r…) ne portent aucun identifiant de session dans
// leur chemin. Sans contexte, on retombait sur la session la plus recente du serveur — c'est-a-dire,
// des que deux salons coexistent, potentiellement celle de quelqu'un d'autre.
func (g *gamesyncServer) fieldsForDocCtx(docName, ussCtx string) *commonpb.MapValue {
	uss := ussFromDocPath(docName)
	if uss == "" {
		uss = ussCtx
	}
	info := g.lookup(uss)
	switch {
	case strings.Contains(docName, "/__stu/"):
		// Mesure sur les captures Nintendo (SESSION-002/003/004, montant ET descendant) : le champ
		// « pl » du document d'etat n'est pas une map — c'est un BLOB D'OCTETS (Value champ 8, tag
		// 0x42) de 9, 78 ou 251 octets que la CONSOLE ecrit et que le serveur RELAIE aux autres.
		// C'est la charge utile par laquelle les consoles se trouvent : cote Nintendo la console
		// ecrit [9, 78, 251] et relit [251, 251, 78, 78] — donc du contenu qui vient d'AILLEURS.
		//
		// Nous rendions ici stateUserFields, qui remplace ce blob par une map VIDE. Resultat mesure
		// le 2026-08-25 pendant la fete : chaque console n'annonçait qu'elle-meme
		// (« effectif annonce par la console : 1 », 13 fois en 10 min) alors que le serveur en
		// asseyait huit, et le salon fondait de 8 a 0 en ~35 s sans qu'aucune partie ne se joue.
		// On rend donc le document REELLEMENT ecrit des qu'il existe.
		if !soirFlag("plsansrelais") {
			return g.champsEtatUtilisateur(docName, info, uss)
		}
		return stateUserFields(info, uss) // per-user state (distinct schema: suid/susid/…/pl)
	case strings.Contains(docName, "/__pus/"):
		return userSessionFields(info, uss) // the host's own user session (presence member)
	case strings.Contains(docName, "/__us/"):
		// The canonical UserSession doc. The NPLN "get my UserSession" step (@0xd69de0) builds the
		// lookup key "__us/<uss>" and requires a PRESENT, non-empty document there; without a real
		// UserSession here (previously this fell through to the empty default) the step times out and
		// the session phase stalls at 3. Must be a valid UserSession, same schema as the __pus member.
		return userSessionFields(info, uss)
	case strings.Contains(docName, "/__stg/"):
		return gameSessionMutableFields(info) // shared session mutable data
	case strings.Contains(docName, "/__gs/") && !strings.HasSuffix(docName, "/__gs/s"):
		// Les cinq documents de salon au schema REEL de Nintendo (f, m, r, n, ck). __gs/s est traite
		// juste apres : le mod client s'en sert comme relais de « ma UserSession », un usage qui lui
		// est propre et qu'on ne casse pas ici. Sous drapeau « gsvraischema », on lui rend aussi son
		// vrai contenu (copie des reglages) pour pouvoir comparer a chaud.
		if m := g.champsGs(lastSeg(docName), info, g.effectifPublie(uss)); m != nil {
			return m
		}
		return &commonpb.MapValue{Fields: map[string]*commonpb.Value{"id": gsStr("")}}
	case strings.HasSuffix(docName, "/__gs/s") && soirFlag("gsvraischema"):
		if m := g.champsGs("s", info, g.effectifPublie(uss)); m != nil {
			return m
		}
		return &commonpb.MapValue{Fields: map[string]*commonpb.Value{"id": gsStr("")}}
	case strings.HasSuffix(docName, "/__gs/s"):
		// The game ALSO reads the on-screen room code from __gs.s: the decoded message's +0x50 std::string
		// is copied into [bexDetail+0xf70] (GetNplnShortGameSessionID -> Room Info "Room ID"). +0x50 is
		// empty now (Room ID blank); of this doc's string fields only `tn` is empty, so it's the likeliest
		// +0x50 slot. Put the deterministic real code there (same value CreateGameSessionShortAlias
		// registers on :443, so the REAL server-assigned code shows). If the Room ID stays blank, +0x50 is
		// a different sub-key and we widen the probe. (This doc is also served as a valid UserSession.)
		// The room code is written into [0xf70] client-side by the subsdk (the exact __gs.s sub-field
		// isn't resolvable server-side without runtime memory inspection); this doc stays a valid
		// UserSession. The subsdk derives the SAME deterministic code from the session id it reads.
		return userSessionFields(info, uss)
	default:
		// Unknown doc: present + empty (well-typed) rather than absent, so a watch never blocks.
		return &commonpb.MapValue{Fields: map[string]*commonpb.Value{"id": gsStr("")}}
	}
}

// docChangeFor synthesizes the DocumentChange{EXIST} for a watched document name.
func (g *gamesyncServer) docChangeFor(tid, docName, uss string, now *timestamppb.Timestamp) *gspb.DocumentChange {
	return &gspb.DocumentChange{
		TargetId:           tid,
		DocumentChangeType: gspb.DocumentChange_EXIST,
		Document:           &gspb.Document{Name: docName, Fields: g.fieldsForDoc(docName), CreateTime: now, UpdateTime: now},
	}
}

// KeepUserSession — the bidirectional keep-alive that holds the session "connected" AND delivers
// the Firestore-Listen snapshots the host waits on. The client streams echos (heartbeats) +
// target updates (its watches); for each watch we push the current document state (EXIST) THEN
// mark the target LISTED. That is the exact signal bq::RecreateSessionFiber's "get my
// UserSession" wait consumes to proceed past CreateNplnSession.
func (g *gamesyncServer) KeepUserSession(stream grpc.BidiStreamingServer[gspb.KeepUserSessionRequest, gspb.KeepUserSessionResponse]) error {
	log.Printf("[NPLN gamesync] KeepUserSession stream opened")

	// All stream.Send() must be serialized: the main loop replies AND the periodic pusher goroutine
	// both send on this one bidi stream (concurrent gRPC sends are unsafe).
	var sendMu sync.Mutex
	send := func(resp *gspb.KeepUserSessionResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(resp)
	}

	// Watched targets we can push LIVE updates to (recorded as their watches arrive).
	type wt struct {
		tid  string
		name string // full doc name, or collection base (for __pus, member = name+"/"+uss)
		coll bool
	}
	var st sync.Mutex
	streamUss := ""
	watched := map[string]wt{} // "stu" | "stg" | "pus"

	// [Nextendo] TOUTES les collections surveillees, pas seulement celle de MPJ.
	//
	// watched["pus"] etait ecrase par CHAQUE cible collection : avec l'unique collection de Mario
	// Party Jamboree (docs/__pgn/All/__pus) c'etait equivalent, mais Splatoon 3 en surveille sept
	// — docs/SessionInfo, docs/Event, docs/MasterCollection, docs/__us, docs/@@RefereeResult,
	// docs/__gs, docs/__pgn/All/__pus — et seule la derniere survivait.
	// Mesure du 2026-08-14 : le jeu ECRIT docs/MasterCollection/Session, docs/SessionInfo/<uid> et
	// docs/__us/<uss>, puis attend de les revoir arriver sur ces cibles. Nous ne repoussions que
	// quatre documents fixes, jamais ses ecritures : il attendait 92 s puis rendait une erreur
	// applicative (ErrorFrom "Application"), session pourtant creee avec succes.
	var colls []wt
	// When this watch stream ends (End-Session teardown, or a reconnect), mark its userSession "closing"
	// so subsequent presence writes get POPULATED write_results (the client awaits them post-close and
	// wild-derefs on empty). A reopened stream clears the flag again (setStreamActive below).
	wake := make(chan struct{}, 8) // WriteDocuments -> nudge this stream to re-broadcast the store now
	defer func() {
		g.setStreamClosed(streamUss)
		if streamUss != "" {
			g.mu.Lock()
			if g.wakers[streamUss] == wake {
				delete(g.wakers, streamUss)
			}
			g.mu.Unlock()

			// [Nextendo] LIBERER la session quand le joueur s'en va.
			//
			// setStreamClosed ne faisait que poser un drapeau : la session restait dans g.sess POUR
			// TOUJOURS. Consequences mesurees le 2026-08-14, toutes signalees par l'utilisateur :
			// le roster continuait de publier des joueurs partis, le monitoring affichait un lobby
			// occupe par des absents, et annuler une recherche rendait une erreur de communication
			// puis bloquait le jeu sur « Connexion a Internet… », faute de session liberee.
			g.oublierSession(streamUss)
		}
	}()

	// Periodic pusher: the client is a PASSIVE Firestore-Listen watcher — after LISTED it sends nothing
	// and only WAITS for live DocumentChanges. Pia runs its NAT check AFTER the initial snapshot, then
	// waits for the session state (its own member in __pus + the game-session mutable data) to be pushed
	// LIVE so it can reach "connected". We cannot detect the local NAT-check timing, so re-push every 3s.
	ctx := stream.Context()
	// doPush re-broadcasts the current session docs (esp. the __pus presence members) LIVE, then flushes
	// a snapshot boundary (TargetChange) so the client COMMITS the buffered DocumentChanges into its
	// visible collection. RE finding: a DocumentChange alone is buffered and never counted — only a
	// following TargetChange makes the Firestore-Listen cache apply it. This is what re-materializes the
	// host's own __pus member after its delete/re-add heartbeat (member count -> 1/4 instead of 0/4).
	debutFlux := time.Now()
	fermerPourSoupape := false

	// Le verdict est renvoye tant qu'il CHANGE (il grandit a mesure que les rapports arrivent),
	// puis trois fois de plus, puis silence. Mesure : nous en poussions 118 par partie, 1,1 Mo vers
	// un seul client ; Nintendo en pousse TROIS et toute sa capture pese 15 594 octets. Ce document
	// n'existe qu'apres la fin du match : le plafonner ne touche ni la formation des salons ni Pia.
	verdictEtat := map[string][32]byte{}
	verdictRepets := map[string]int{}
	verdictTaire := func(nom string, champs *commonpb.MapValue) bool {
		if !strings.Contains(nom, "@@RefereeResult") {
			return false
		}

		b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(champs)
		if err != nil {
			return false
		}

		h := sha256.Sum256(b)
		if verdictEtat[nom] != h {
			verdictEtat[nom] = h
			verdictRepets[nom] = 0

			return false
		}

		verdictRepets[nom]++

		return verdictRepets[nom] > 3
	}

	doPush := func() {
		// [Nextendo] SOUPAPE — clore un flux qui n'aboutira pas.
		//
		// Ce n'est pas un correctif, c'est un garde-fou assume. Quand la partie ne peut pas
		// demarrer — typiquement faute de joueurs, un controle que le jeu fait DANS SON BINAIRE et
		// qu'aucune reponse serveur ne peut satisfaire — le client n'abandonne pas : il ecrit son
		// document d'etat en boucle (588 ecritures en 3 minutes, mesure du 2026-08-14) et reste
		// affiche sur « Connexion a Internet… », obligeant a relancer l'emulateur.
		// En refermant le flux on lui rend la main : il repart en erreur propre ou rouvre une
		// session, au lieu de tourner indefiniment. Duree reglable par « fluxmax=<secondes> ».
		if d := dureeMaxFlux(); d > 0 && time.Since(debutFlux) > d {
			st.Lock()
			u := streamUss
			st.Unlock()
			log.Printf("[NPLN gamesync] flux %s ouvert depuis %s sans aboutir — on le referme (soupape)",
				u, time.Since(debutFlux).Truncate(time.Second))
			fermerPourSoupape = true
			return
		}

		st.Lock()
		uss := streamUss
		stu, hasStu := watched["stu"]
		stg, hasStg := watched["stg"]
		pus, hasPus := watched["pus"]
		us, hasUs := watched["us"]
		gss, hasGss := watched["gss"]
		st.Unlock()
		if uss == "" {
			return
		}
		now := timestamppb.Now()
		info := g.lookup(uss)
		pushed := 0
		dejaPousses := map[string]bool{} // evite de renvoyer deux fois le meme document
		var frontieres []string          // cibles a valider par une frontiere d'instantane
		vues := map[string]bool{}

		// Type de changement pour les republications EN DIRECT (apres l'instantane initial).
		//
		// Mesure du 2026-08-15 : dans l'instantane, Nintendo envoie EXIST ; ensuite, sur une cible
		// deja validee par LISTED, tout changement passe en UPDATED (capture de la Guerre de
		// territoire, messages 024-025 pour les arrivees tardives, 052-053 pour un depart). Nous
		// envoyions EXIST dans les deux cas. Drapeau « rosterexist » pour revenir a l'ancien
		// comportement sans redeployer.
		typeVivant := gspb.DocumentChange_UPDATED
		if soirFlag("rosterexist") {
			typeVivant = gspb.DocumentChange_EXIST
		}
		// marque enregistre un nom comme deja pousse et le rend tel quel, pour que le rejeu des
		// collections ci-dessous ne renvoie pas une seconde fois le meme document.
		marque := func(nom string) string { dejaPousses[nom] = true; return nom }

		if hasPus {
			// Single canned host member EXIST (stable behavior — the store re-broadcast + snapshot-boundary
			// re-LIST flipped the client to the GUEST view, losing host controls, so reverted).
			if send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
				TargetId: pus.tid, DocumentChangeType: gspb.DocumentChange_EXIST,
				Document: &gspb.Document{Name: strings.TrimRight(pus.name, "/") + "/" + uss, Fields: userSessionFields(info, uss), CreateTime: now, UpdateTime: now},
			}}}) == nil {
				pushed++
			}
		}
		if hasUs {
			_ = send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
				TargetId: us.tid, DocumentChangeType: gspb.DocumentChange_UPDATED,
				Document: &gspb.Document{Name: marque(us.name), Fields: userSessionFields(info, uss), CreateTime: now, UpdateTime: now},
			}}})
			pushed++
		}
		if hasGss {
			// __gs/s repurposed as the host's UserSession (subsdk redirects get-my-UserSession here).
			_ = send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
				TargetId: gss.tid, DocumentChangeType: gspb.DocumentChange_UPDATED,
				Document: &gspb.Document{Name: marque(gss.name), Fields: userSessionFields(info, uss), CreateTime: now, UpdateTime: now},
			}}})
			pushed++
		}
		if hasStu {
			_ = send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
				TargetId: stu.tid, DocumentChangeType: gspb.DocumentChange_UPDATED,
				Document: &gspb.Document{Name: marque(stu.name), Fields: g.champsEtatUtilisateur(stu.name, info, uss), CreateTime: now, UpdateTime: now},
			}}})
			pushed++

			// Puis l'etat de CHAQUE AUTRE participant, un envoi par pair, sur ce meme document.
			// C'est la boite aux lettres relevee dans la capture Nintendo : quatre blobs de pairs
			// arrivent sur l'unique document que la console surveille. Voir gamesync_boite_pia.go.
			if !soirFlag("sansboitepia") {
				boites := g.boitesAuxLettresPia(uss)
				for _, b := range boites {
					_ = send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
						TargetId: stu.tid, DocumentChangeType: gspb.DocumentChange_UPDATED,
						Document: &gspb.Document{Name: marque(stu.name), Fields: b, CreateTime: now, UpdateTime: now},
					}}})
					pushed++
				}
				if len(boites) > 0 && soirFlag("pltrace") {
					log.Printf("[NPLN boite] %s : etat de %d pair(s) transmis", uss, len(boites))
				}
			}
		}
		if hasStg {
			_ = send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
				TargetId: stg.tid, DocumentChangeType: gspb.DocumentChange_UPDATED,
				Document: &gspb.Document{Name: marque(stg.name), Fields: gameSessionMutableFieldsN(info, len(g.participantsDeLaPartie(uss))), CreateTime: now, UpdateTime: now},
			}}})
			pushed++
		}
		if pushed > 0 {
			// [Nextendo] Rejouer les ECRITURES du jeu sur les collections qu'il surveille.
			//
			// Les quatre documents ci-dessus sont ceux que NOUS synthetisons. Splatoon 3, lui, ecrit sa
			// propre session (docs/MasterCollection/Session, docs/SessionInfo/<uid>, docs/__us/<uss>) et
			// attend de la revoir arriver sur ses cibles : sans ce renvoi il attendait 92 s puis rendait
			// une erreur applicative, alors que la session etait bien creee.
			st.Lock()
			listeColls := append([]wt(nil), colls...)
			st.Unlock()

			// Etat des AUTRES participants : le jeu publie le sien dans docs/__pgn/All/__stu/<uss>
			// (360 ecritures mesurees en deux minutes le 2026-08-14) et attend d'y lire celui de ses
			// pairs. Sans ce renvoi, chacun ne connait que lui-meme, et la session P2P ne s'etablit
			// jamais — P2PSessionId reste 0 et l'horloge P2P a -1 jusqu'au MatchTimeoutOther.
			if hasStu {
				base := stu.name
				if i := strings.LastIndex(base, "/"); i > 0 {
					base = base[:i]
				}
				for u, autre := range g.participantsDeLaPartie(uss) {
					nom := base + "/" + u
					if dejaPousses[nom] {
						continue
					}
					dejaPousses[nom] = true
					if send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
						TargetId: stu.tid, DocumentChangeType: gspb.DocumentChange_UPDATED,
						Document: &gspb.Document{Name: nom, Fields: g.champsEtatUtilisateur(nom, autre, u), CreateTime: now, UpdateTime: now},
					}}}) == nil {
						pushed++
					}
				}
			}

			// Publier le ROSTER : sur les collections de sessions utilisateur (__us et __pus), chaque
			// joueur doit recevoir la session de TOUS les participants de la partie, pas seulement la
			// sienne — c'est ainsi qu'il decouvre ses adversaires et coequipiers.
			participants := g.participantsDeLaPartie(uss)
			for _, c := range listeColls {
				base := strings.TrimRight(c.name, "/")
				if !strings.HasSuffix(base, "__us") && !strings.HasSuffix(base, "__pus") {
					continue
				}
				if !vues[c.tid] {
					vues[c.tid] = true
					frontieres = append(frontieres, c.tid)
				}
				for u, info := range participants {
					nom := base + "/" + u
					if dejaPousses[nom] {
						continue
					}
					dejaPousses[nom] = true
					if send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
						TargetId: c.tid, DocumentChangeType: typeVivant,
						Document: &gspb.Document{Name: nom, Fields: userSessionFields(info, u), CreateTime: now, UpdateTime: now},
					}}}) == nil {
						pushed++
					}
				}
			}

			// Republier les documents du SALON quand l'effectif bouge. Mesure du 2026-08-15, capture
			// de la Guerre de territoire (messages 054, 058, 059) : a chaque arrivee ou depart,
			// Nintendo reemet docs/__gs/r — le compteur de joueurs — puis docs/__gs/m et docs/__gs/s.
			//
			// Nous ne republiions jamais cette collection : ipc restait fige a sa valeur du premier
			// instantane, et le jeu continuait de croire la salle telle qu'il l'avait decouverte.
			for _, c := range listeColls {
				base := strings.TrimRight(c.name, "/")
				if !strings.HasSuffix(base, "/__gs") {
					continue
				}
				if !vues[c.tid] {
					vues[c.tid] = true
					frontieres = append(frontieres, c.tid)
				}
				for _, sous := range []string{"r", "m", "s"} {
					nom := base + "/" + sous
					if dejaPousses[nom] {
						continue
					}
					dejaPousses[nom] = true
					champs := g.fieldsForDocCtx(nom, uss)
					if champs == nil {
						continue
					}
					if send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
						TargetId: c.tid, DocumentChangeType: typeVivant,
						Document: &gspb.Document{Name: nom, Fields: champs, CreateTime: now, UpdateTime: now},
					}}}) == nil {
						pushed++
					}
				}
			}

			for _, c := range listeColls {
				// Diagnostic cible : sur la collection d'arbitrage, dire QUELLE partie on interroge.
				// C'est la seule facon de voir si la cle de lecture diverge de celle d'ecriture — trois
				// tentatives de correction ont echoue faute de cette comparaison.
				if strings.HasSuffix(c.name, "@@RefereeResult") {
					log.Printf("[NPLN gamesync] lecture @@RefereeResult [partie=%s] -> %d document(s) dans le magasin",
						g.gsidDe(uss), len(g.docsSousCollection(g.gsidDe(uss), c.name)))
				}
				for nom, champs := range g.docsSousCollection(g.gsidDe(uss), c.name) {
					if dejaPousses[nom] {
						continue
					}
					dejaPousses[nom] = true
					// Mesure du 25/08 sur trois captures Nintendo : EXIST n'apparait JAMAIS apres le
					// LISTED d'une cible, UPDATED jamais avant. Nous poussions le verdict en EXIST sur
					// une cible listee depuis le debut du match — la seule combinaison qu'ils n'emettent
					// pas. Drapeau « rosterexist » pour revenir.
					if verdictTaire(nom, champs) {
						continue
					}
					if send(&gspb.KeepUserSessionResponse{ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
						TargetId: c.tid, DocumentChangeType: typeVivant,
						Document: &gspb.Document{Name: nom, Fields: champs, CreateTime: now, UpdateTime: now},
					}}}) == nil {
						pushed++
					}
				}
			}

			// [Nextendo] FRONTIERE D'INSTANTANE apres la poussee du roster.
			//
			// Constat de reverse deja inscrit dans ce fichier : « a DocumentChange alone is buffered and
			// never counted — only a following TargetChange makes the Firestore-Listen cache apply it ».
			// Nos membres pousses en direct restaient donc en tampon chez le client : les deux joueurs
			// etaient bien dans la meme partie, chacun recevait la session de l'autre, et pourtant aucun
			// ne voyait l'autre a l'ecran (mesure du 2026-08-14, Guerre de territoire a deux).
			//
			// On emet UPDATED (et non LISTED) et UNIQUEMENT sur les cibles de collections de joueurs :
			// un re-LIST complet avait deja ete essayé puis annulé — il basculait l'hote d'un salon
			// prive en vue invité, lui faisant perdre ses commandes. UPDATED valide le tampon sans
			// rejouer l'instantane, et les cibles ciblees ici ne portent pas le role d'hote.
			// Les trois captures Nintendo ne montrent AUCUN TargetChange{UPDATED} isole : il ouvre un
			// instantane et est toujours ferme par un LISTED. Drapeau « frontiere » pour le remettre.
			if soirFlag("frontiere") && pushed > 0 {
				for _, tid := range frontieres {
					_ = send(&gspb.KeepUserSessionResponse{
						ResponseType: &gspb.KeepUserSessionResponse_TargetChange{
							TargetChange: &gspb.TargetChange{TargetId: tid, TargetChangeType: gspb.TargetChange_UPDATED},
						},
					})
				}
			}

			log.Printf("[NPLN gamesync] live push (uss=%s, docs=%d, %d frontiere(s))", uss, pushed, len(frontieres))
		}
	}
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()

		// [Nextendo 2026-08-25] POUSSER SUR ECRITURE, PAS SUR MINUTEUR.
		//
		// Mesure de notre propre flux dechiffre contre la capture Nintendo : 1 139 263 octets vers un
		// seul client en 200 s, contre 15 594 octets pour UNE PARTIE ENTIERE chez eux. Le document
		// d'etat des autres joueurs (docs/__pgn/All/__stu) part 2355 fois chez nous, 7 fois chez eux.
		// Le jeu ne recevait plus que ca : son P2P n'echangeait que six paquets, 246 octets, avant
		// d'abandonner, pendant qu'on saturait la meme socket a 6 Ko/s.
		//
		// La ou nous republiions TOUT toutes les trois secondes, Nintendo n'envoie un document que
		// lorsqu'il change. Le mecanisme existe deja ici : « wake » est declenche par chaque
		// WriteDocuments. On le garde intact — c'est lui qui donne a Pia son rattrapage pendant
		// l'etablissement, et le supprimer avait empeche toute partie de demarrer. On se contente de
		// desarmer le MINUTEUR : il ne repousse plus que si rien n'est parti depuis dureeRappel.
		//
		// Drapeau « pousseminuteur » pour revenir au flot continu sans redeployer.
		derniere := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if soirFlag("pousseminuteur") || time.Since(derniere) >= dureeRappel() {
					doPush()
					derniere = time.Now()
				}
			case <-wake:
				doPush() // event-driven: a WriteDocuments just changed the store -> re-broadcast now
				derniere = time.Now()
			}
			// La soupape a decide de rendre la main : on arrete de pousser et on laisse le flux
			// se terminer, ce qui libere la session et rend le controle au jeu.
			if fermerPourSoupape {
				return
			}
		}
	}()

	for {
		if fermerPourSoupape {
			return nil
		}
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[NPLN gamesync] KeepUserSession closed by client")
			return nil
		}
		if err != nil {
			log.Printf("[NPLN gamesync] KeepUserSession recv err: %v", err)
			return err
		}
		debugLogf("[NPLN gamesync][DIAG] KeepUserSession req =\n%s", prototext.Format(req))

		if echo := req.GetEcho(); echo != "" {
			if err := send(&gspb.KeepUserSessionResponse{
				ResponseType: &gspb.KeepUserSessionResponse_Echo{Echo: echo},
			}); err != nil {
				return err
			}
		}

		if ut := req.GetUpdateTarget(); ut != nil && ut.GetTarget() != nil {
			tgt := ut.GetTarget()
			tid := lastSeg(tgt.GetName())
			now := timestamppb.Now()

			// ACCUSE DE CREATION. Mesure du 2026-08-15, capture du salon prive Nintendo
			// (SESSION-002-136.114.7.187, messages 0 a 47) : Nintendo ouvre CHAQUE cible par un
			// TargetChange{UPDATED} des sa creation, AVANT le moindre document, puis la valide par
			// LISTED. Les cibles 2, 3, 7, 8, 9, 10 et 11 recoivent meme la paire UPDATED/LISTED sans
			// aucun document entre les deux : c'est un accuse, pas un porteur de contenu.
			//
			// Nous n'emettions rien tant que nous n'avions pas de document a livrer. Une cible vide
			// restait donc muette, et le client attendait un signal qui ne venait jamais.
			if err := send(&gspb.KeepUserSessionResponse{
				ResponseType: &gspb.KeepUserSessionResponse_TargetChange{
					TargetChange: &gspb.TargetChange{TargetId: tid, TargetChangeType: gspb.TargetChange_UPDATED},
				},
			}); err != nil {
				return err
			}

			// A DOCUMENTS target: push each watched document's current state (EXIST).
			if d := tgt.GetDocuments(); d != nil {
				for _, docName := range d.GetDocuments() {
					uss := ussFromDocPath(docName)
					st.Lock()
					if uss != "" {
						streamUss = uss        // remember which userSession this stream is for
						g.setStreamActive(uss) // stream (re)opened for this uss -> not closing
						g.mu.Lock()
						g.wakers[uss] = wake // let WriteDocuments nudge this stream to re-broadcast
						g.mu.Unlock()
					}
					if strings.Contains(docName, "/__stu/") {
						watched["stu"] = wt{tid: tid, name: docName}
					} else if strings.Contains(docName, "/__stg/") {
						watched["stg"] = wt{tid: tid, name: docName}
					} else if strings.HasSuffix(docName, "/__gs/s") {
						// The subsdk redirects get-my-UserSession to __gs/s; re-push it so the
						// change-watch fires and the op sees its (UserSession) content.
						watched["gss"] = wt{tid: tid, name: docName}
					}
					st.Unlock()
					dc := g.docChangeFor(tid, docName, uss, now)
					log.Printf("[NPLN gamesync] watch doc %q -> EXIST (uss=%s)", docName, uss)
					if err := send(&gspb.KeepUserSessionResponse{
						ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: dc},
					}); err != nil {
						return err
					}
				}
			}

			// A COLLECTION target (docs/__pgn/All/__pus): the host's own member MUST be part of the
			// INITIAL SNAPSHOT — delivered BEFORE the TargetChange{LISTED}. The NPLN "get my
			// UserSession" step (@0xd69de0) searches the collection ACCUMULATED when the snapshot
			// completes (LISTED); a member that arrives AFTER LISTED is missed, the async op times
			// out, and the session phase stalls at 3 (never reaching 6 / room never displays). So we
			// push the member first, then LISTED.
			if c := tgt.GetCollection(); c != nil && c.GetCollection() != "" {
				coll := strings.TrimRight(c.GetCollection(), "/")
				uss := streamUss
				if uss == "" {
					g.mu.Lock()
					uss = g.lastUss
					g.mu.Unlock()
				}
				st.Lock()
				colls = append(colls, wt{tid: tid, name: coll, coll: true})
				// Le membre « canned » ci-dessous ne vaut que pour la collection de sessions
				// utilisateur (__pus) : ailleurs, le membre ne porte ni ce nom ni ces champs.
				estPus := strings.HasSuffix(coll, "__pus")
				if estPus {
					watched["pus"] = wt{tid: tid, name: coll, coll: true}
				}
				st.Unlock()
				// INSTANTANE DE LA COLLECTION. On livre d'abord les membres SYNTHETISES (roster,
				// documents de salon…), puis ce que le jeu a reellement ecrit dans le magasin, les
				// seconds ayant le dernier mot sur les premiers.
				vus := map[string]bool{}
				for _, m := range g.membresDeCollection(coll, uss) {
					vus[m.nom] = true
					if err := send(&gspb.KeepUserSessionResponse{
						ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
							TargetId: tid, DocumentChangeType: gspb.DocumentChange_EXIST,
							Document: &gspb.Document{Name: m.nom, Fields: m.champs, CreateTime: now, UpdateTime: now},
						}},
					}); err != nil {
						return err
					}
				}
				membres := g.docsSousCollection(g.gsidDe(uss), coll)
				for nom, champs := range membres {
					if vus[nom] {
						continue
					}
					if err := send(&gspb.KeepUserSessionResponse{
						ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
							TargetId: tid, DocumentChangeType: gspb.DocumentChange_EXIST,
							Document: &gspb.Document{Name: nom, Fields: champs, CreateTime: now, UpdateTime: now},
						}},
					}); err != nil {
						return err
					}
				}
				log.Printf("[NPLN gamesync] collection %q -> %d membre(s) synthetise(s) + %d du magasin (instantane, pre-LISTED)",
					coll, len(vus), len(membres))
				_ = estPus

				// Also deliver the canonical UserSession doc docs/__us/<uss> on THIS persistent target.
				// The client watches docs/__us/<uss> on target 1 then DELETES that watch, purging it
				// from its store; the "get my UserSession" step (@0xd69de0/@0xd6ecc0) later searches the
				// store for key "__us/<uss>". Riding the persistent presence target keeps it present.
				// [Nextendo] La racine est le PREMIER segment du chemin, pas ce qui precede le
				// premier "/__".
				//
				// L'ancienne regle coupait a "/__" : correcte pour Mario Party Jamboree, dont la
				// collection est "docs/__pgn/All/__pus" -> "docs". Mais Splatoon 3 surveille des
				// collections SANS double blanc-souligne — "docs/SessionInfo", "docs/Event" — donc
				// la racine devenait "docs/SessionInfo" et la UserSession canonique partait sous
				// "docs/SessionInfo/__us/<uss>". Or le jeu surveille "docs/__us/<uss>" (mesure du
				// 2026-08-14) : il ne recevait jamais sa propre session la ou il la cherche.
				// Le premier segment rend "docs" dans les DEUX cas, MPJ compris.
				docsRoot := coll
				if i := strings.Index(coll, "/"); i > 0 {
					docsRoot = coll[:i]
				}
				usName := docsRoot + "/__us/" + uss
				st.Lock()
				watched["us"] = wt{tid: tid, name: usName}
				st.Unlock()
				if err := send(&gspb.KeepUserSessionResponse{
					ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{DocumentChange: &gspb.DocumentChange{
						TargetId: tid, DocumentChangeType: gspb.DocumentChange_EXIST,
						Document: &gspb.Document{Name: usName, Fields: userSessionFields(g.lookup(uss), uss), CreateTime: now, UpdateTime: now},
					}},
				}); err != nil {
					return err
				}
				log.Printf("[NPLN gamesync] also delivered %q (canonical UserSession) on persistent target tid=%s", usName, tid)
			}

			// Snapshot (documents + collection members above) delivered -> mark the target LISTED.
			log.Printf("[NPLN gamesync] update_target %q -> TargetChange{LISTED} tid=%s", tgt.GetName(), tid)
			if err := send(&gspb.KeepUserSessionResponse{
				ResponseType: &gspb.KeepUserSessionResponse_TargetChange{
					TargetChange: &gspb.TargetChange{TargetId: tid, TargetChangeType: gspb.TargetChange_LISTED},
				},
			}); err != nil {
				return err
			}
		}

		if dt := req.GetDeleteTarget(); dt != nil {
			log.Printf("[NPLN gamesync] delete_target %q -> TargetChange{DELETED}", dt.GetName())
			_ = send(&gspb.KeepUserSessionResponse{
				ResponseType: &gspb.KeepUserSessionResponse_TargetChange{
					TargetChange: &gspb.TargetChange{TargetId: lastSeg(dt.GetName()), TargetChangeType: gspb.TargetChange_DELETED},
				},
			})
		}
	}
}

// ussFromDocPath extracts the userSession uuid embedded in a watched doc path, e.g.
// "docs/__pgn/All/__stu/17b70e72-…" -> "17b70e72-…". Falls back to "" (lookup then uses last).
func ussFromDocPath(name string) string {
	segs := strings.Split(name, "/")
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		if len(last) >= 8 && strings.Contains(last, "-") {
			return last
		}
	}
	return ""
}

// tenantOfName pulls "tenants/<id>" out of a resource name.
func tenantOfName(name string) string {
	p := strings.SplitN(name, "/", 3)
	if len(p) >= 2 && p[0] == "tenants" {
		return p[0] + "/" + p[1]
	}
	return npnTenant
}

// gsNameOfUserSession strips "/userSessions/<id>" to get the parent gameSession name.
func gsNameOfUserSession(us string) string {
	if i := strings.Index(us, "/userSessions/"); i >= 0 {
		return us[:i]
	}
	return us
}

// ---- document store (fresh/empty for a new session) ----

// isSessionDoc reports whether we synthesize content for this document name (vs. genuinely absent).
func isSessionDoc(name string) bool {
	for _, m := range []string{"/__stu/", "/__pus/", "/__stg/", "/__gs/", "/__us/"} {
		if strings.Contains(name, m) {
			return true
		}
	}
	return false
}

func (g *gamesyncServer) GetDocument(ctx context.Context, req *gspb.GetDocumentRequest) (*gspb.Document, error) {
	name := req.GetName()
	if !isSessionDoc(name) {
		log.Printf("[NPLN gamesync] GetDocument %q -> NotFound (not a session doc)", name)
		return nil, status.Error(codes.NotFound, "document not found")
	}
	// The host read a doc it just saw as EXIST on the watch — return the SAME content, or it treats
	// the read as a contradiction and stalls the create-room fiber.
	now := timestamppb.Now()
	log.Printf("[NPLN gamesync] GetDocument %q -> EXIST", name)
	return &gspb.Document{Name: name, Fields: g.fieldsForDoc(name), CreateTime: now, UpdateTime: now}, nil
}

func (g *gamesyncServer) ReadDocuments(ctx context.Context, req *gspb.ReadDocumentsRequest) (*gspb.ReadDocumentsResponse, error) {
	now := timestamppb.Now()
	out := &gspb.ReadDocumentsResponse{ReadTime: now}
	for _, name := range req.GetDocuments() {
		if isSessionDoc(name) {
			log.Printf("[NPLN gamesync] ReadDocuments %q -> Found", name)
			out.Results = append(out.Results, &gspb.ReadResult{Result: &gspb.ReadResult_Found{
				Found: &gspb.Document{Name: name, Fields: g.fieldsForDoc(name), CreateTime: now, UpdateTime: now},
			}})
		} else {
			out.Results = append(out.Results, &gspb.ReadResult{Result: &gspb.ReadResult_Missing{Missing: name}})
		}
	}
	return out, nil
}

func (g *gamesyncServer) ListDocuments(ctx context.Context, req *gspb.ListDocumentsRequest) (*gspb.ListDocumentsResponse, error) {
	return &gspb.ListDocumentsResponse{}, nil
}

func (g *gamesyncServer) QueryCollectionIds(ctx context.Context, req *gspb.QueryCollectionIdsRequest) (*gspb.QueryCollectionIdsResponse, error) {
	return &gspb.QueryCollectionIdsResponse{}, nil
}

// writeResultsFor returns ONE WriteResult per WriteOperation, matching its type. The client indexes
// write_results[i] for each op it sent; returning an empty write_results left it reading uninitialized
// memory -> a garbage nn::npln Value map -> wild-deref crash when ending the session (the Close-time
// GamesyncClient write). Each result is well-formed and empty (Update/Delete are empty messages;
// Transform returns one empty Value per field-transform).
// transformResultValue returns the COMPUTED result of one field transform (what the client reads back
// into its document/member state). An empty Value here made the client see a null member field -> 0/4.
// gsGlobalSeq is a server-wide monotonic counter that stands in for Gamesync's per-document "global
// values" (e.g. the presence connection seq id `upcsid`). load/store_global_value transforms MUST come
// back as a valid positive integer: returning Null there made the member's presence field null -> the
// host saw itself as a non-member (0/4). Any strictly-increasing value keeps the presence doc valid.
var gsGlobalSeq int64

func transformResultValue(ft *gspb.FieldTransform) *commonpb.Value {
	switch {
	case ft.GetIncrement() != nil:
		return ft.GetIncrement() // no server state kept: new value == the increment (from 0)
	case ft.GetMaximum() != nil:
		return ft.GetMaximum()
	case ft.GetMinimum() != nil:
		return ft.GetMinimum()
	case ft.GetSetServerValue() == gspb.FieldTransform_REQUEST_TIME:
		return &commonpb.Value{ValueType: &commonpb.Value_TimestampValue{TimestampValue: timestamppb.Now()}}
	case ft.GetLoadGlobalValue() != "" || ft.GetStoreGlobalValue() != "":
		// upcsid & friends: a monotonically increasing connection sequence id. Never null.
		return &commonpb.Value{ValueType: &commonpb.Value_IntegerValue{IntegerValue: atomic.AddInt64(&gsGlobalSeq, 1)}}
	case ft.GetClearGlobalValue() != "":
		return &commonpb.Value{ValueType: &commonpb.Value_IntegerValue{IntegerValue: 0}}
	default:
		// Unknown transform: still return a valid non-null scalar so the client's field stays usable.
		return &commonpb.Value{ValueType: &commonpb.Value_IntegerValue{IntegerValue: atomic.AddInt64(&gsGlobalSeq, 1)}}
	}
}

// transformKind is a short label for logging which oneof case a field transform is.
func transformKind(ft *gspb.FieldTransform) string {
	switch {
	case ft.GetIncrement() != nil:
		return "inc"
	case ft.GetMaximum() != nil:
		return "max"
	case ft.GetMinimum() != nil:
		return "min"
	case ft.GetSetServerValue() == gspb.FieldTransform_REQUEST_TIME:
		return "reqtime"
	case ft.GetLoadGlobalValue() != "":
		return "loadg(" + ft.GetLoadGlobalValue() + ")"
	case ft.GetStoreGlobalValue() != "":
		return "storeg(" + ft.GetStoreGlobalValue() + ")"
	case ft.GetClearGlobalValue() != "":
		return "clearg(" + ft.GetClearGlobalValue() + ")"
	default:
		return "?"
	}
}

// ussFromOps returns the userSession uuid carried by the first op whose doc path embeds one (the __pus
// presence writes do; __gs/m does not). Used to decide, per write, whether its session is closing.
func ussFromOps(ops []*gspb.WriteOperation) string {
	for _, op := range ops {
		var name string
		switch {
		case op.GetUpdateDocument() != nil:
			name = op.GetUpdateDocument().GetDocument().GetName()
		case op.GetMergeDocument() != nil:
			name = op.GetMergeDocument().GetDocument().GetName()
		case op.GetTransformDocument() != nil:
			name = op.GetTransformDocument().GetName()
		case op.GetDeleteDocument() != nil:
			name = op.GetDeleteDocument().GetName()
		}
		if u := ussFromDocPath(name); u != "" {
			return u
		}
	}
	return ""
}

// writeResultsFor: ALWAYS return one well-formed WriteResult per op (never a short/empty array — that
// made the client read write_results[i] out of bounds, cache a garbage nn::npln::Value, and wild-deref
// when destroying it at End-Session teardown; the destructor chain is generic npln core we must not hook).
// The catch: a valid DeleteResult for the presence `delete __pus` makes the client COMMIT the removal of
// its own member -> room drops to 0/4. So for DELETE ops we deliberately return an UpdateResult instead
// (type-mismatched but still a valid, fully-formed WriteResult): no OOB read (End-Session no crash), and
// the client does not apply a document deletion -> the host's presence member survives -> room stays 1/4.
// Transforms/updates get their normal results. Fixes the count AND the crash together, purely server-side.
func (g *gamesyncServer) writeResultsFor(ops []*gspb.WriteOperation) []*gspb.WriteResult {
	out := make([]*gspb.WriteResult, 0, len(ops))
	for _, op := range ops {
		switch {
		case op.GetTransformDocument() != nil:
			fts := op.GetTransformDocument().GetFieldTransforms()
			vals := make([]*commonpb.Value, len(fts))
			for i, ft := range fts {
				vals[i] = transformResultValue(ft)
			}
			out = append(out, &gspb.WriteResult{ResultType: &gspb.WriteResult_TransformResult{TransformResult: &gspb.TransformResult{Results: vals}}})
		case op.GetDeleteDocument() != nil:
			out = append(out, &gspb.WriteResult{ResultType: &gspb.WriteResult_DeleteResult{DeleteResult: &gspb.DeleteResult{}}})
		default: // update / merge
			out = append(out, &gspb.WriteResult{ResultType: &gspb.WriteResult_UpdateResult{UpdateResult: &gspb.UpdateResult{}}})
		}
	}
	return out
}

func opDesc(ops []*gspb.WriteOperation) string {
	s := ""
	for _, op := range ops {
		switch {
		case op.GetUpdateDocument() != nil:
			s += "update:" + op.GetUpdateDocument().GetDocument().GetName() + " "
		case op.GetMergeDocument() != nil:
			s += "merge:" + op.GetMergeDocument().GetDocument().GetName() + " "
		case op.GetTransformDocument() != nil:
			s += "transform:" + op.GetTransformDocument().GetName()
			for _, ft := range op.GetTransformDocument().GetFieldTransforms() {
				s += "{" + ft.GetFieldPath() + "=" + transformKind(ft) + "}"
			}
			s += " "
		case op.GetDeleteDocument() != nil:
			s += "delete:" + op.GetDeleteDocument().GetName() + " "
		}
	}
	return s
}

func (g *gamesyncServer) WriteDocuments(ctx context.Context, req *gspb.WriteDocumentsRequest) (*gspb.WriteDocumentsResponse, error) {
	ops := req.GetWriteOperations()
	uss, gsid := g.contexteEcriture(ctx, ops)
	// La partie AVANT le journal, pas apres. Sans elle, une ecriture ne se rattache a rien : le
	// 2026-08-21, pour savoir si les salons qui meurent publient leurs reglages (docs/__gs/m), il a
	// fallu passer par les IP du journal TURN et une fenetre de quinze minutes — attribution
	// approximative, et les parties se chevauchent. gsid etait pourtant deja calcule juste en
	// dessous. On le nomme « partie= », comme les lignes de lecture de l'arbitre, pour que les deux
	// bouts d'une meme partie se recoupent d'un simple filtre.
	log.Printf("[NPLN gamesync] WriteDocuments ops=%d [%s] closing=%v partie=%s uss=%s",
		len(ops), opDesc(ops), g.isClosing(uss), gsid, uss)
	// Ce que les consoles disent de LEURS liens P2P. Elles l ecrivent depuis toujours dans
	// docs/__pgn/All/__stu ; nous le rangions sans le lire. Voir gamesync_pia_telemetrie.go.
	journaliserTelemetriePia(gsid, ops)
	g.mu.Lock()
	g.applyWritesToStore(gsid, ops)
	g.mu.Unlock()

	// RENDRE LE VERDICT D'ARBITRAGE. Le jeu depose sa demande puis attend le resultat sur la
	// collection qu'il surveille ; sans lui, la partie ne demarre jamais.
	for _, op := range ops {
		nom := op.GetUpdateDocument().GetDocument().GetName()
		if nom == "" || (!strings.HasPrefix(nom, "docs/@@RefereeRequest/") && !strings.HasPrefix(nom, "docs/@@RefereeReport/")) {
			continue
		}
		if gsidVerdict, nomResultat, champs := g.arbitrer(gsid, nom, ops); champs != nil {
			g.mu.Lock()
			g.store[cleStore(gsidVerdict, nomResultat)] = champs
			g.mu.Unlock()
			log.Printf("[NPLN gamesync] arbitrage : %q -> %q publie (%d equipe(s), %d champ(s)) [RANGE SOUS partie=%s]",
				nom, nomResultat, len(champs.GetFields()["teams"].GetMapValue().GetFields()),
				len(champs.GetFields()), gsidVerdict)
		}
	}

	resp := &gspb.WriteDocumentsResponse{WriteResults: g.writeResultsFor(ops), CommitTime: timestamppb.Now()}
	g.reveillerLaPartie(uss) // event-driven: re-push la collection a TOUTE la partie, pas au seul auteur
	return resp, nil
}

func (g *gamesyncServer) LazyWriteDocuments(ctx context.Context, req *gspb.LazyWriteDocumentsRequest) (*gspb.LazyWriteDocumentsResponse, error) {
	return &gspb.LazyWriteDocumentsResponse{CommitTime: timestamppb.Now()}, nil
}

func (g *gamesyncServer) BeginTransaction(ctx context.Context, req *gspb.BeginTransactionRequest) (*gspb.BeginTransactionResponse, error) {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return &gspb.BeginTransactionResponse{Transaction: b}, nil
}

func (g *gamesyncServer) CommitTransaction(ctx context.Context, req *gspb.CommitTransactionRequest) (*gspb.CommitTransactionResponse, error) {
	ops := req.GetWriteOperations()
	uss, gsid := g.contexteEcriture(ctx, ops)
	log.Printf("[NPLN gamesync] CommitTransaction ops=%d", len(ops))
	g.mu.Lock()
	g.applyWritesToStore(gsid, ops)
	g.mu.Unlock()
	resp := &gspb.CommitTransactionResponse{WriteResults: g.writeResultsFor(ops), CommitTime: timestamppb.Now()}
	g.reveillerLaPartie(uss)
	return resp, nil
}

func (g *gamesyncServer) RollbackTransaction(ctx context.Context, req *gspb.RollbackTransactionRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (g *gamesyncServer) CreateRound(ctx context.Context, req *gspb.CreateRoundRequest) (*gspb.Round, error) {
	if r := req.GetRound(); r != nil {
		return r, nil
	}
	return &gspb.Round{}, nil
}

// ---------------------------------------------------------------------------------------------
// Attributs des participants : pont matchmaking -> gamesync.
//
// Les champs `att` et `ltc` des UserSession partaient VIDES (gsMap(nil)). Ce sont eux qui portent
// ce que le jeu affiche d'un coequipier — ses attributs de match et ses latences. Le matchmaking
// les a pourtant en main : chaque MatchedUserSession porte la UserDefinition complete de son
// joueur. Sans eux, le roster publie decrivait des participants sans aucune donnee, et aucun
// joueur n'apparaissait a l'ecran de l'autre pendant la recherche (mesure du 2026-08-14).
// Cle : l'uuid de la userSession, celui-la meme que le client presente a IssueToken.

var attributsParUss = struct {
	sync.Mutex
	m map[string]*attrsParticipant
}{m: map[string]*attrsParticipant{}}

type attrsParticipant struct {
	att *commonpb.MapValue
	ltc *commonpb.MapValue
	tn  string // equipe
}

// rememberParticipantAttrs enregistre les attributs d'un participant pour sa userSession.
func rememberParticipantAttrs(ussUUID string, att *commonpb.MapValue, latences map[string]*durationpb.Duration, team string) {
	if ussUUID == "" {
		return
	}
	ltc := &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
	for region, d := range latences {
		if d != nil {
			ltc.Fields[region] = gsInt(int64(d.AsDuration() / time.Millisecond))
		}
	}
	attributsParUss.Lock()
	attributsParUss.m[ussUUID] = &attrsParticipant{att: att, ltc: ltc, tn: team}
	attributsParUss.Unlock()
}

// participantAttrs rend les attributs enregistres pour une userSession (nil si inconnue).
func participantAttrs(ussUUID string) *attrsParticipant {
	attributsParUss.Lock()
	defer attributsParUss.Unlock()
	return attributsParUss.m[ussUUID]
}

// oublierSession retire une userSession du registre et du store, et sort son joueur du salon
// affiche par le monitoring. Appelee quand le flux KeepUserSession se termine — depart volontaire,
// annulation, ou perte de connexion.
func (g *gamesyncServer) oublierSession(uss string) {
	if uss == "" {
		return
	}

	g.mu.Lock()
	info := g.sess[uss]
	delete(g.sess, uss)
	if g.last != nil && info != nil && g.last == info {
		g.last = nil
	}
	// Les documents de CE participant n'ont plus lieu d'etre publies aux autres.
	for nom := range g.store {
		if strings.HasSuffix(nom, "/"+uss) {
			delete(g.store, nom)
		}
	}
	restants := 0
	if info != nil {
		for _, autre := range g.sess {
			if autre != nil && autre.gsid == info.gsid {
				restants++
			}
		}
	}
	g.mu.Unlock()

	attributsParUss.Lock()
	delete(attributsParUss.m, uss)
	attributsParUss.Unlock()

	oublierEffectifServi(uss)

	if info != nil {
		// Sortir aussi le joueur de la fiche de partie que GetGameSession rend aux suivants : sans ca
		// un joueur parti restait annonce, et le salon se remplissait de fantomes au fil des essais.
		oublierParticipant(info.gsid, npnTenant+"/users/"+info.uid)
		dashQuitterSalons(info.uid)
		log.Printf("[NPLN gamesync] session %s liberee (uid=%s) — %d participant(s) restant(s) dans %s",
			uss, info.uid, restants, info.gsid)
	}
}

// champsEtatUtilisateur rend le document d'etat REELLEMENT publie par le joueur s'il existe, et
// seulement a defaut la version synthetique.
//
// Le jeu ecrit docs/__pgn/All/__stu/<uss> en rafale (360 fois en deux minutes, mesure du
// 2026-08-14) : c'est la qu'il depose son etat de connexion. Nous repoussions par-dessus une
// version fabriquee aux champs pl/mp VIDES, ecrasant ce qu'il venait de publier — et nous
// n'envoyions jamais celui des autres participants. Chacun ne connaissait donc que lui-meme.
func (g *gamesyncServer) champsEtatUtilisateur(nom string, info *gsSessionInfo, uss string) *commonpb.MapValue {
	gsid := ""
	if info != nil {
		gsid = info.gsid
	}
	g.mu.Lock()
	stocke := g.store[cleStore(gsid, nom)]
	g.mu.Unlock()

	if stocke != nil && len(stocke.GetFields()) > 0 {
		if soirFlag("pltrace") {
			log.Printf("[NPLN pl] relais %s -> %d octets", lastSeg(nom), len(stocke.GetFields()["pl"].GetBytesValue()))
		}
		return stocke
	}
	return stateUserFields(info, uss)
}
