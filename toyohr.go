package main

// toyohr — the Splatoon-3-specific NPLN game service (nn.npln.toyohr.v1). We serve
// the Schedule (stage/mode rotation) and FestService (Splatfest) by REPLAYING the
// bytes a real Switch received (captured in dist/Nextendo-MITM-Splatoon3v3,
// extracted to ./captured/*.bin), BUT we shift every timestamp forward so the
// rotation is "current now" instead of frozen at the capture date (2026-06-28).
// Without the shift the schedules are all in the past and the hall shows no rotation.

import (
	"context"
	"encoding/binary"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

var rawVsSchedules = capture("captured/SelectVsSchedules.bin")

var rawVsParams = capture("captured/SelectVsParams.bin")

var rawCoopSchedules = capture("captured/SelectCoopSchedules.bin")

var rawSeasonSchedules = capture("captured/SelectSeasonSchedules.bin")

var rawLeagueSchedules = capture("captured/SelectLeagueSchedules.bin")

var rawFestSchedule = capture("captured/SelectFestSchedule.bin")

// Reponse de Nintendo a GetFestDecryptionKey, capturee le 2026-08-12 en meme temps que le
// calendrier ci-dessus : les deux decrivent LE MEME fest (JUEA-00201), donc les cles ouvrent
// bien les paquets annonces. C'est ce couple coherent que sert le drapeau "fest".
var rawFestKey = capture("captured/GetFestDecryptionKey.bin")

// rotationPeriod: S3 VS rotations flip on even 2-hour UTC boundaries. Aligning the
// shift to this keeps every schedule landing on a valid boundary the game accepts.
const rotationPeriod = 2 * time.Hour

// asTimestamp returns the concrete *Timestamp if this reflected message is one.
func asTimestamp(m protoreflect.Message) *timestamppb.Timestamp {
	if m.Descriptor().FullName() == "google.protobuf.Timestamp" {
		if ts, ok := m.Interface().(*timestamppb.Timestamp); ok {
			return ts
		}
	}
	return nil
}

// walkTimestamps visits every google.protobuf.Timestamp nested anywhere in msg.
func walkTimestamps(m protoreflect.Message, fn func(*timestamppb.Timestamp)) {
	if ts := asTimestamp(m); ts != nil {
		fn(ts)
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					walkTimestamps(mv.Message(), fn)
					return true
				})
			}
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
				list := v.List()
				for i := 0; i < list.Len(); i++ {
					walkTimestamps(list.Get(i).Message(), fn)
				}
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			walkTimestamps(v.Message(), fn)
		}
		return true
	})
}

// scheduleDelta computes ONE shift que TOUS les types de calendrier subissent. Un decalage par
// type desynchronise les dates entre elles et S3 rejette l'ensemble au demarrage (2162-0001), donc
// le decalage doit rester unique.
//
// ⚠️ SUR QUOI L'ANCRER — mesure du 2026-08-13, 18:13 UTC, trois joueurs hors ligne d'un coup.
// L'ancrage historique etait la rotation la plus ancienne du calendrier VS. Comme le decalage se
// recalcule a chaque requete, il GRANDIT avec le temps : +24 h le matin, +40 h l'apres-midi, +44 h
// le soir. Or les captures ne commencent pas toutes a la meme date — celle de la ligue commence
// 4 h APRES celle du VS. Passe +40 h, la fenetre de ligue se retrouvait donc dans le FUTUR :
//
//	VsSchedules      13/08 18:00 -> 14/08 18:00   encadre le present
//	LeagueSchedules  13/08 22:00 -> 23/08 20:00   commence dans 4 h  ✗
//
// Le client refuse alors tout le jeu et le redemande en boucle : signature mesuree, 10 requetes de
// calendriers par session chez un joueur bloque contre 3 chez un joueur qui passe. Trois joueurs
// sur trois etaient hors ligne, sur trois machines et trois firmwares differents.
//
// On ancre donc sur la capture qui commence le PLUS TARD, pas sur le VS : aucune fenetre ne peut
// alors commencer dans le futur, et toutes encadrent le present. Le test TestAllSchedulesAreCurrent
// verrouille cette propriete.
func scheduleDelta() time.Duration { return scheduleDeltaAt(time.Now().UTC()) }

// scheduleDeltaAt rend le decalage pour un instant DONNE. Parametrer l'heure est ce qui rend la
// rotation testable hors ligne : on peut demander le decalage a T puis a T+2 h et verifier que la
// position dans l'enregistrement a AVANCE (voir TestLaRotationAvance).
func scheduleDeltaAt(maintenant time.Time) time.Duration {
	debutLePlusTard := time.Time{}

	// ⚠️ Chaque capture DOIT etre decodee avec SON type : la decoder avec un autre met tous ses
	// champs en « inconnus », walkTimestamps n'y voit aucun horodatage, et l'ancrage retombe
	// silencieusement sur le seul VS — c'est-a-dire le bug qu'on corrige.
	captures := []struct {
		brut []byte
		msg  proto.Message
	}{
		{rawVsSchedules, &toyohrpb.SelectVsSchedulesResponse{}},
		{rawCoopSchedules, &toyohrpb.SelectCoopSchedulesResponse{}},
		{rawSeasonSchedules, &toyohrpb.SelectSeasonSchedulesResponse{}},
		{rawLeagueSchedules, &toyohrpb.SelectLeagueSchedulesResponse{}},
	}

	for _, c := range captures {
		debut := debutDeLaCapture(c.brut, c.msg)
		if debut.IsZero() {
			continue
		}
		if debutLePlusTard.IsZero() || debut.After(debutLePlusTard) {
			debutLePlusTard = debut
		}
	}

	if debutLePlusTard.IsZero() {
		return 0
	}

	// La rotation des STAGES n'est plus servie par ce decalage : elle est ENGENDREE
	// (rotation_generee.go). Ce decalage ne sert plus qu'a rendre courants les autres calendriers,
	// et il garde son ancrage historique — la capture qui commence le plus tard posee sur maintenant,
	// pour qu'aucune fenetre ne debute dans le futur.
	return maintenant.Truncate(rotationPeriod).Sub(debutLePlusTard.Truncate(rotationPeriod))
}

// positionDansLEnregistrement rend l'instant de l'ENREGISTREMENT que nous servons pour un instant
// reel donne. C'est la grandeur qui decide des stages affiches : deux instants reels qui retombent
// sur la meme position servent exactement la meme rotation.
func positionDansLEnregistrement(maintenant time.Time) time.Time {
	return maintenant.Add(-scheduleDeltaAt(maintenant))
}

// debutDeLaCapture rend l'horodatage le plus ancien d'une reponse capturee, sentinelles exclues.
// etendueDeLEnregistrement rend le premier et le dernier instant couverts par les calendriers
// enregistrés, tous types confondus — donc la fenêtre dans laquelle une position est valable.
func etendueDeLEnregistrement() (time.Time, time.Time) {
	captures := []struct {
		brut []byte
		msg  proto.Message
	}{
		{rawVsSchedules, &toyohrpb.SelectVsSchedulesResponse{}},
		{rawCoopSchedules, &toyohrpb.SelectCoopSchedulesResponse{}},
		{rawSeasonSchedules, &toyohrpb.SelectSeasonSchedulesResponse{}},
		{rawLeagueSchedules, &toyohrpb.SelectLeagueSchedulesResponse{}},
	}

	var debut, fin time.Time
	for _, c := range captures {
		if proto.Unmarshal(c.brut, c.msg) != nil {
			continue
		}
		walkTimestamps(c.msg.ProtoReflect(), func(ts *timestamppb.Timestamp) {
			if ts.Seconds <= 0 {
				return
			}
			t := ts.AsTime()
			if debut.IsZero() || t.Before(debut) {
				debut = t
			}
			if fin.IsZero() || t.After(fin) {
				fin = t
			}
		})
	}

	return debut, fin
}

// dureeDeLaCapture rend l'étendue totale couverte par les calendriers enregistrés — du plus ancien
// horodatage au plus récent, tous types confondus. C'est la longueur du cycle de rotation que nous
// pouvons servir.
func dureeDeLaCapture() time.Duration {
	captures := []struct {
		brut []byte
		msg  proto.Message
	}{
		{rawVsSchedules, &toyohrpb.SelectVsSchedulesResponse{}},
		{rawCoopSchedules, &toyohrpb.SelectCoopSchedulesResponse{}},
		{rawSeasonSchedules, &toyohrpb.SelectSeasonSchedulesResponse{}},
		{rawLeagueSchedules, &toyohrpb.SelectLeagueSchedulesResponse{}},
	}

	var debut, fin time.Time
	for _, c := range captures {
		if proto.Unmarshal(c.brut, c.msg) != nil {
			continue
		}
		walkTimestamps(c.msg.ProtoReflect(), func(ts *timestamppb.Timestamp) {
			if ts.Seconds <= 0 {
				return
			}
			t := ts.AsTime()
			if debut.IsZero() || t.Before(debut) {
				debut = t
			}
			if fin.IsZero() || t.After(fin) {
				fin = t
			}
		})
	}

	if debut.IsZero() || fin.IsZero() {
		return 0
	}

	return fin.Sub(debut)
}

func debutDeLaCapture(brut []byte, r proto.Message) time.Time {
	if proto.Unmarshal(brut, r) != nil {
		return time.Time{}
	}

	var plusAncien time.Time
	walkTimestamps(r.ProtoReflect(), func(ts *timestamppb.Timestamp) {
		if ts.Seconds <= 0 { // horodatage non pose / sentinelle : ne sert pas de base
			return
		}
		t := ts.AsTime()
		if plusAncien.IsZero() || t.Before(plusAncien) {
			plusAncien = t
		}
	})

	return plusAncien
}

// serveShifted unmarshals captured wire bytes and shifts every timestamp by the shared
// delta so the whole set is "now" while staying internally consistent.
func serveShifted(name string, raw []byte, msg proto.Message, delta time.Duration) {
	if err := proto.Unmarshal(raw, msg); err != nil {
		log.Printf("[NPLN toyohr] %s unmarshal error: %v", name, err)
		return
	}
	if delta != 0 {
		walkTimestamps(msg.ProtoReflect(), func(ts *timestamppb.Timestamp) {
			// NEVER shift an unset/sentinel timestamp. LeagueSchedule.timestamp is
			// captured as the protobuf zero value (0001-01-01, Seconds<0). Shifting it
			// turns "unset" into a garbage near-zero date, which S3 rejects at boot with
			// a Thunder.nss assertion (2162-0001). Real schedule times are all 2026
			// (Seconds ~1.7e9), so guarding on Seconds<=0 only skips the sentinels.
			if ts.Seconds <= 0 {
				return
			}
			shifted := timestamppb.New(ts.AsTime().Add(delta))
			ts.Seconds = shifted.Seconds
			ts.Nanos = shifted.Nanos
		})
	}
	log.Printf("[NPLN toyohr] %s -> shifted +%s (global) (%d bytes)", name, delta, len(raw))
}

// replay unmarshals captured wire bytes into msg without shifting (for non-schedule
// payloads like params, and the fest schedule which must not be date-shifted).
func replay(name string, raw []byte, msg proto.Message) {
	if err := proto.Unmarshal(raw, msg); err != nil {
		log.Printf("[NPLN toyohr] %s replay unmarshal error: %v", name, err)
		return
	}
	log.Printf("[NPLN toyohr] %s -> replay %d bytes", name, len(raw))
}

// ---- Schedule (nn.npln.toyohr.v1.Schedule) ----

type scheduleServer struct {
	toyohrpb.UnimplementedScheduleServer
}

func (s *scheduleServer) SelectVsSchedules(ctx context.Context, req *toyohrpb.SelectVsSchedulesRequest) (*toyohrpb.SelectVsSchedulesResponse, error) {
	r := &toyohrpb.SelectVsSchedulesResponse{}
	serveShifted("SelectVsSchedules", rawVsSchedules, r, scheduleDelta())

	// Rotation ENGENDREE, sous drapeau a chaud « rotavance ». Rejouer la capture condamne a choisir
	// entre une rotation gelee et un horizon trop court (voir rotation_generee.go) ; engendrer les
	// phases donne les deux. Le drapeau retire, on retombe sur le rejeu, immediatement.
	if soirFlag("rotavance") {
		g := rotationEngendree(time.Now().UTC(), r)
		log.Printf("[NPLN toyohr] SelectVsSchedules -> rotation engendree : %d phases, creneau courant %d",
			len(g.GetSchedules()), creneauDe(time.Now().UTC()))
		return g, nil
	}

	return r, nil
}

// [Nextendo] VsParams carries timestamps too and MUST ride the same global shift. The capture
// holds 1773882000 (2026-03-19) and 1726452000 (2024-09-16) while every schedule is shifted onto
// today, so replaying it unchanged left the params pointing months away from the rotation they
// describe. S3 then cannot tie the current rotation to its parameters: the hall showed a lone
// error applet and stopped advancing. This file's own rule already says one shared delta must be
// applied to every payload, or the types desynchronise and the game rejects them -- params were
// simply left out of it.
func (s *scheduleServer) SelectVsParams(ctx context.Context, req *toyohrpb.SelectVsParamsRequest) (*toyohrpb.SelectVsParamsResponse, error) {
	r := &toyohrpb.SelectVsParamsResponse{}
	serveShifted("SelectVsParams", rawVsParams, r, scheduleDelta())
	return r, nil
}

func (s *scheduleServer) SelectCoopSchedules(ctx context.Context, req *toyohrpb.SelectCoopSchedulesRequest) (*toyohrpb.SelectCoopSchedulesResponse, error) {
	r := &toyohrpb.SelectCoopSchedulesResponse{}
	// Shifted to the present (same global delta as VS). S3 rejects a STALE schedule with
	// NPLN error 2321-4992 ("no current rotation") — that's the real stages blocker, not
	// the boot abort (which was UserScreening/violations, now fixed). All 4 schedule types
	// must be current AND share one delta so they stay mutually consistent.
	serveShifted("SelectCoopSchedules", rawCoopSchedules, r, scheduleDelta())
	return r, nil
}

func (s *scheduleServer) SelectSeasonSchedules(ctx context.Context, req *toyohrpb.SelectSeasonSchedulesRequest) (*toyohrpb.SelectSeasonSchedulesResponse, error) {
	r := &toyohrpb.SelectSeasonSchedulesResponse{}
	// Shifted with the same global delta as the others (mutual consistency). Even though the
	// captured season window already spans the present, S3 rejects the whole set unless every
	// schedule shares one time base — shift it too.
	serveShifted("SelectSeasonSchedules", rawSeasonSchedules, r, scheduleDelta())
	return r, nil
}

func (s *scheduleServer) SelectLeagueSchedules(ctx context.Context, req *toyohrpb.SelectLeagueSchedulesRequest) (*toyohrpb.SelectLeagueSchedulesResponse, error) {
	r := &toyohrpb.SelectLeagueSchedulesResponse{}
	// Shifted to the present with the shared global delta (see SelectCoopSchedules).
	serveShifted("SelectLeagueSchedules", rawLeagueSchedules, r, scheduleDelta())
	return r, nil
}

// ---- FestService (nn.npln.toyohr.v1.FestService) ----

type festServer struct {
	toyohrpb.UnimplementedFestServiceServer
}

// capturedFrame returns the message body of a captured_boot/*.grpc file, stripping the
// 5-byte gRPC frame header (1 compression flag + 4-byte big-endian length).
func capturedFrame(file string) ([]byte, bool) {
	data, err := capturedBoot.ReadFile("captured_boot/" + file)
	if err != nil || len(data) < 5 {
		return nil, false
	}
	mlen := int(binary.BigEndian.Uint32(data[1:5]))
	if 5+mlen > len(data) {
		return nil, false
	}
	return data[5 : 5+mlen], true
}

// GetFestDecryptionKey hands S3 the keys that decrypt the fest's BCAT packs.
//
// LES CLES DOIVENT OUVRIR LES PACKS REELLEMENT PRESENTS DANS LE CACHE. Une reponse qui annonce
// une fete dont le cache BCAT de la console ne detient pas les paquets se solde par
// « BcatInvalid » : aligner le NOM de la fete (fest_align.go) ne suffit pas, il faut que les CLES
// correspondent aux paquets.
//
// Le chemin normal est le drapeau « festcles » : une fete que NOUS chiffrons, sous l'identifiant
// que NOUS annonçons, avec nos propres cles (voir fest_cles_maison.go). C'est le seul mode
// supporte par ce depot.
//
// Si le cache de la console contient des paquets que ce serveur n'a pas chiffres, ce sont leurs
// cles qu'il faut servir, pas les notres. Renseignez-les par
// « festclescache=<notice>,<depart>[,<camp>] ». Sans cela on ne fabrique pas de reponse au
// hasard — on rend le nom seul et on le dit.
func clesDuCacheDeFete() (notice, depart, camp string, ok bool) {
	v := soirFlagValeur("festclescache")
	if v == "" {
		return "", "", "", false
	}
	parts := strings.Split(v, ",")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		log.Printf("[NPLN toyohr] festclescache : il faut au moins <notice>,<depart> — ignore")

		return "", "", "", false
	}
	if len(parts) >= 3 {
		camp = parts[2]
	}

	return parts[0], parts[1], camp, true
}

func (s *festServer) GetFestDecryptionKey(ctx context.Context, req *toyohrpb.GetFestDecryptionKeyRequest) (*toyohrpb.FestDecryptionKey, error) {
	// NOS PROPRES CLES D'ABORD. Le drapeau « festcles » sert une fete que NOUS avons chiffree,
	// sous l'identifiant que NOUS annoncons. Sans lui, un seul festival est jouable dans la vie
	// d'un compte : la sauvegarde retient l'identifiant rejoint, et le jeu ne repropose plus de
	// choisir son camp. Voir fest_cles_maison.go.
	if cles := clesDeFeteMaison(); len(cles) > 0 {
		id := identifiantDeFeteMaison(time.Now())
		// La phase decide : deux cles et aucun camp tant que la fete court, trois cles et le
		// vainqueur nomme une fois les resultats ouverts. Voir fest_cloture.go.
		cles, vainqueur := clesAServir(cles, id)
		r := &toyohrpb.FestDecryptionKey{}
		if err := proto.Unmarshal(reponseClesMaison(id, cles, vainqueur), r); err != nil {
			log.Printf("[NPLN toyohr] GetFestDecryptionKey : reponse maison illisible (%v)", err)
		}
		log.Printf("[NPLN toyohr] GetFestDecryptionKey -> %d cle(s) MAISON pour %s", len(cles), id)
		return r, nil
	}

	// Avec le drapeau "fest", on sert la cle capturee du fest annonce juste avant : le couple est
	// coherent par construction, puisque les deux viennent de la meme session de capture.
	if soirFlag("fest") {
		r := &toyohrpb.FestDecryptionKey{}
		if err := proto.Unmarshal(rawFestKey, r); err != nil {
			log.Printf("[NPLN toyohr] GetFestDecryptionKey : capture illisible (%v)", err)
		}

		// NE PAS DONNER LA CLE DU VAINQUEUR AVANT QU'IL Y EN AIT UN.
		//
		// Mesure du 2026-08-22, Splatfest officiel JUEA-00107 EN COURS, console reelle contre
		// Nintendo : la reponse ne porte que TROIS champs — le nom, notice_key et start_key.
		//
		//	tenants/<locataire>/fests/<FESTID>/decryptionKey
		//	<notice_key>
		//	<start_key>
		//
		// Notre capture embarquee, elle, en porte quatre : elle vient d'une fete TERMINEE et
		// ajoute result_winning_team_key ainsi que l'equipe en tete. Les rejouer pendant une fete
		// qui court, c'est annoncer un vainqueur qui n'existe pas encore — et livrer la cle du
		// paquet de victoire avant l'heure.
		//
		// Nintendo ne livre ces deux champs qu'une fois le verdict rendu. On fait pareil.
		if feteEnCours() {
			r.ResultWinningTeamKey = ""
			r.TeamAlphaKey = ""
			r.TeamBravoKey = ""
			r.TeamCharlieKey = ""
			r.WinningTeam = ""
			log.Printf("[NPLN toyohr] GetFestDecryptionKey -> 2 cles (notice + depart), la fete court encore")
			return r, nil
		}
		log.Printf("[NPLN toyohr] GetFestDecryptionKey -> capture Nintendo telle quelle (%d o)", len(rawFestKey))
		return r, nil
	}

	name := npnTenant + "/fests/" + festIDInCacheCourant() + "/decryptionKey"

	// Hand-build the message: the generated FestDecryptionKey type has no fields (the proto is
	// only partially generated), so we emit the wire format the capture showed us.
	notice, depart, camp, ok := clesDuCacheDeFete()
	if !ok {
		log.Printf("[NPLN toyohr] GetFestDecryptionKey : aucune cle pour le cache de %s. "+
			"Utilisez « festcles » pour une fete maison, ou « festclescache » si le cache vient "+
			"d'ailleurs.", festIDInCacheCourant())

		return &toyohrpb.FestDecryptionKey{}, nil
	}

	var b []byte
	b = appendProtoString(b, 1, name)
	b = appendProtoString(b, 2, notice)
	b = appendProtoString(b, 3, depart)
	if camp != "" {
		b = appendProtoString(b, 8, camp)
	}

	r := &toyohrpb.FestDecryptionKey{}
	if err := proto.Unmarshal(b, r); err != nil {
		log.Printf("[NPLN toyohr] GetFestDecryptionKey build: %v", err)
	}
	log.Printf("[NPLN toyohr] GetFestDecryptionKey -> cles de %s (celles des packs du cache)", festIDInCacheCourant())
	return r, nil
}

// appendProtoString writes one length-delimited string field (tag<<3|2, varint len, bytes).
func appendProtoString(b []byte, field int, s string) []byte {
	b = appendVarint(b, uint64(field)<<3|2)
	b = appendVarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func (s *festServer) SelectFestSchedule(ctx context.Context, req *toyohrpb.SelectFestScheduleRequest) (*toyohrpb.SelectFestScheduleResponse, error) {
	r := &toyohrpb.SelectFestScheduleResponse{}

	// [Nextendo] Announcing a Splatfest drags in the whole fest pipeline, and it is what currently
	// breaks the hall: S3 follows the announcement with GetFestDecryptionKey, refuses the keys we
	// hand it, and raises 2321-4992 (the failing method is spelled out in the error's rawContext:
	// "npl1 / GetFestDecryptionKey"). A Splatfest is optional content -- the online square does not
	// need one -- so announce NO fest by default and keep the login path clean. Set NPLN_S3_FEST=1
	// to re-enable once the BCAT packs and the keys are proven to match.
	// ⚠️ MESURE DU 2026-08-12 : NE JAMAIS RENVOYER DE REPONSE VIDE ICI.
	//
	// Dans les deux captures reelles, Nintendo repond TOUJOURS un calendrier de fest — 344 octets,
	// decrivant le fest courant ou le dernier en date, meme quand aucun ne tourne. Nous renvoyions
	// un message vide, et le jeu s'abattait deux secondes plus tard (2162-0001, nn.npln.Worker,
	// Thunder.nss:0x667fe8), juste apres avoir resolu sa sauvegarde. C'est coherent : FestRegion,
	// Fest et FestRecord sont des cles de la sauvegarde, le jeu vient de les charger et demande le
	// calendrier correspondant.
	//
	// Le drapeau "fest" sert la capture de Nintendo TELLE QUELLE (JUEA-00201), avec la cle de
	// dechiffrement du MEME fest capturee dans la meme session. L'ancien chemin (renommer le fest
	// vers le paquet BCAT effectivement present dans le cache) reste derriere NPLN_S3_FEST=1 : il
	// vise la route console, ou le cache local peut contenir d'autres paquets.
	// NOTRE fete d'abord : « festmaison » remplace la capture de Nintendo par un calendrier que nous
	// fabriquons de bout en bout. La reponse ne contient alors AUCUN octet de Nintendo — voir
	// fest_genere.go, ou le decodage de la capture montre qu'elle ne porte que du choix editorial.
	if festMaisonActif() {
		r.Schedule = festMaison(time.Now())
		journaliserFeteMaison(r.Schedule)
		return r, nil
	}

	if soirFlag("fest") {
		if err := proto.Unmarshal(rawFestSchedule, r); err != nil {
			log.Printf("[NPLN toyohr] SelectFestSchedule : capture illisible (%v)", err)
		}

		// ⚠️ COHERENCE DES CALENDRIERS (mesure du 2026-08-12). Le jeu a nomme lui-meme les deux
		// appels fautifs dans son contexte d'erreur : SelectSeasonSchedules et SelectFestSchedule.
		// La raison etait que les horaires venaient d'une capture de JUIN, recalee de plus de mille
		// heures, tandis que le fest venait de la capture de cette nuit, a sa date reelle : les deux
		// ne decrivaient pas la meme saison. Depuis, TOUTES les captures d'horaires proviennent de la
		// meme session que le fest, et le fest subit donc le MEME decalage global qu'elles — un seul
		// corpus, un seul delta, un ensemble coherent.
		// ⚠️ MESURE : decaler le fest le rend invalide pour le jeu — il s'arrete NET juste apres
		// l'avoir recu, sans erreur ni abort. Servi tel quel, il enchaine sur la clé de dechiffrement
		// et le casier. Comme les horaires proviennent maintenant de la MEME nuit que lui, l'ecart
		// residuel n'est que de quelques heures, la ou il etait de 45 jours quand ils venaient de juin.
		// Le drapeau "festdecale" permet de reessayer le decalage si besoin.
		if d := scheduleDelta(); d != 0 && soirFlag("festdecale") {
			n := 0
			walkTimestamps(r.ProtoReflect(), func(ts *timestamppb.Timestamp) {
				if ts.Seconds > 0 {
					v := timestamppb.New(ts.AsTime().Add(d))
					ts.Seconds, ts.Nanos = v.Seconds, v.Nanos
					n++
				}
			})
			log.Printf("[NPLN toyohr] SelectFestSchedule -> capture decalee de %s comme les horaires (%d dates)", d, n)
			return r, nil
		}

		// Le fest de la capture (JUEA-00201) etait EN COURS le jour de la capture : le jeu reclame
		// donc legitimement ses paquets BCAT, que nous n'avons pas — d'ou l'erreur 2002-6400 quand il
		// ouvre un cache de livraison vide. Le drapeau "festfutur" repousse toutes ses dates de deux
		// mois : la structure reste complete (ce que le jeu exige, un calendrier vide le faisait
		// planter), mais le fest n'est pas encore d'actualite, donc rien a telecharger.
		if soirFlag("festfutur") {
			const report = 60 * 24 * time.Hour
			n := 0
			walkTimestamps(r.ProtoReflect(), func(ts *timestamppb.Timestamp) {
				if ts.Seconds > 0 {
					d := timestamppb.New(ts.AsTime().Add(report))
					ts.Seconds, ts.Nanos = d.Seconds, d.Nanos
					n++
				}
			})
			log.Printf("[NPLN toyohr] SelectFestSchedule -> capture repoussee de 60 jours (%d dates)", n)
			return r, nil
		}

		log.Printf("[NPLN toyohr] SelectFestSchedule -> capture Nintendo telle quelle (%d o)", len(rawFestSchedule))
		return r, nil
	}

	if os.Getenv("NPLN_S3_FEST") != "1" {
		log.Printf("[NPLN toyohr] SelectFestSchedule -> aucun fest annonce (NPLN_S3_FEST!=1)")
		return r, nil
	}

	// Announce the fest the console's BCAT cache actually holds, not the one in our capture.
	// Naming a fest whose packs the cache does not hold makes S3 ask BCAT for resources it cannot
	// find, which it reports as "BcatInvalid". See fest_align.go.
	// Still not date-shifted: we only fix WHICH fest is named, not when it runs.
	alignFestToBcat("SelectFestSchedule", rawFestSchedule, r)
	return r, nil
}
