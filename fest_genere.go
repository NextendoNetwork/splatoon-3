package main

// Un Splatfest ENTIEREMENT NOTRE, sans un octet de Nintendo dans la reponse d'horaires.
//
// Le decodage de captured/SelectFestSchedule.bin (344 octets) montre que cette reponse ne porte
// AUCUNE oeuvre : deux noms de ressource, quatre codes de region, une cible, un identifiant de
// paquet BCAT et son empreinte, cinq horodatages, une fenetre de publication par region, et deux
// numeros de stage tricolore. Que du choix editorial.
//
// Les noms d'equipes, les banniere et les repliques des idoles ne sont PAS ici : ils vivent dans le
// paquet BCAT que `FestGameData` designe. Ce fichier ne fabrique donc que la MECANIQUE de la fete.
//
// Les ecarts entre phases sont ceux de la capture, a la seconde :
//
//	ouverture   -662400 s avant le debut   (7 jours 16 h — l'annonce)
//	debut          0
//	mi-parcours +86400 s                   (24 h)
//	fin         +172800 s                  (48 h)
//	cloture     +180000 s                  (50 h — les resultats, 2 h apres la fin)
//
// et la fenetre de publication d'une region s'ouvre 25200 s (7 h) avant l'annonce pour courir 42
// jours, ce qui la fait deborder largement la fete elle-meme.

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	commonpb "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// Ecarts releves sur la capture, en secondes autour du debut de la fete.
const (
	festAvantOuverture = 662400 // annonce : 7 j 16 h avant le debut
	festVersMiParcours = 86400  // 24 h
	festVersFin        = 172800 // 48 h
	festVersCloture    = 180000 // 50 h — 2 h apres la fin
	festAvantPublie    = 25200  // la publication s'ouvre 7 h avant l'annonce
	festDureePubliee   = 3628800
)

// festRegions : les quatre codes de la capture, dans son ordre.
var festRegions = []string{"JP", "US", "EU", "AP"}

// festMaisonActif dit si l'on sert NOTRE fete plutot que la capture de Nintendo.
// « festmaison » seul suffit ; « festmaison=<horodatage unix> » fixe en plus l'heure de DEBUT.
func festMaisonActif() bool { return soirFlagValeur("festmaison") != "" }

// debutDeLaFeteMaison rend l'heure de debut choisie. Sans valeur explicite, on prend le prochain
// samedi a minuit UTC — c'est le jour ou Nintendo lance les siennes.
func debutDeLaFeteMaison(maintenant time.Time) time.Time {
	if v := soirFlagValeur("festmaison"); v != "" && v != "1" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return alignerSurCreneau(time.Unix(n, 0).UTC())
		}
	}
	j := maintenant.UTC().Truncate(24 * time.Hour)
	for j.Weekday() != time.Saturday || !j.After(maintenant.UTC()) {
		j = j.Add(24 * time.Hour)
	}
	return alignerSurCreneau(j)
}

// alignerSurCreneau ramene une date sur la frontiere de creneau inferieure.
//
// ⚠️ MESURE DU 2026-08-20, comparaison champ par champ de la capture Nintendo et de notre fete.
// Les deux reponses ont EXACTEMENT la meme structure — vingt champs de chaque cote, aucun
// manquant, aucun en trop. Seules les valeurs differaient, et une seule difference comptait :
//
//	Nintendo  open/start/mid/end/close  tous a 0 modulo 7200  (la fete demarre a minuit UTC)
//	nous      les cinq                  a 95 modulo 7200      (l'heure brute du drapeau)
//
// Splatoon 3 range ses horaires en creneaux de deux heures — le meme alignement que verifient
// deja nos diagnostics de rotation. Une fete a cheval sur un creneau n'entre dans aucune case :
// le jeu cessait alors de la traiter comme une fete ouverte et reclamait son VERDICT, ce que la
// vraie console ne fait jamais. Onze consoles l'ont recue ce soir, aucune n'a pu jouer.
//
// Les ecarts entre phases sont tous des multiples de 7200 (662400, 86400, 172800, 180000), donc
// aligner le DEBUT aligne toute la table d'un coup.
func alignerSurCreneau(t time.Time) time.Time {
	return t.UTC().Truncate(dureeCreneauFest)
}

// dureeCreneauFest : le pas des horaires de Splatoon 3.
const dureeCreneauFest = 2 * time.Hour

// identifiantDeFeteMaison fabrique un identifiant stable pour une date de debut donnee. Le prefixe
// « NXTD » nous distingue sans ambiguite des « JUEA » de Nintendo : personne ne confondra un
// enregistrement de notre fete avec l'une des leurs.
func identifiantDeFeteMaison(debut time.Time) string {
	// « festid=<ID> » impose l'identifiant. Il le faut des qu'on sert un paquet BCAT reel : le
	// summary.yml du cache nomme SA fete, et le jeu refuse (BcatInvalid) si l'horaire en annonce
	// une autre. Voir fest_align.go — trois choses doivent concorder, l'identifiant, la ressource
	// et l'empreinte.
	// ⚠️ Le fichier de drapeaux passe chaque ligne en minuscules, valeur comprise. Un identifiant
	// de fete est toujours en capitales (« JUEA-00201 »), donc on le retablit : sans cela le jeu
	// recevrait « juea-00201 » et ne reconnaitrait pas son propre paquet.
	if v := soirFlagValeur("festid"); v != "" && v != "1" {
		return strings.ToUpper(v)
	}
	return fmt.Sprintf("NXTD-%05d", debut.Unix()/86400%100000)
}

// empreinteDeFeteMaison rend la revision du paquet BCAT. Nintendo y met une empreinte de 40
// caracteres hexadecimaux ; nous derivons la notre du couple identifiant + ressource, ce qui la
// rend stable pour une fete donnee et differente d'une fete a l'autre.
func empreinteDeFeteMaison(id, ressource string) string {
	// « festrevision=<40 hexa> » impose l'empreinte du paquet reellement servi, celle que porte
	// son summary.yml. Sans elle on en derive une du couple identifiant + ressource : stable et
	// distincte d'une fete a l'autre, ce qui suffit tant qu'aucun paquet reel n'est en jeu, mais
	// ne peut par construction pas tomber sur celle d'un paquet existant.
	if v := soirFlagValeur("festrevision"); len(v) == 40 {
		if _, err := hex.DecodeString(v); err == nil {
			return strings.ToLower(v)
		}
	}
	h := sha1.Sum([]byte("nextendo/fest/" + id + "/" + ressource))
	return hex.EncodeToString(h[:])
}

// ressourceDeFeteMaison rend l'identifiant du paquet BCAT. C'est lui qui, cote console, derive l'IV
// de dechiffrement (nisasyst : CRC32 du nom de ressource, puis SeadRandom) — donc le changer change
// le paquet attendu. Reglable par « festressource=<slug> » pour pointer un paquet que l'on fabrique.
func ressourceDeFeteMaison(id string) string {
	if v := soirFlagValeur("festressource"); v != "" && v != "1" {
		// ⚠️ TOUJOURS EN MINUSCULES. La console derive l'IV de dechiffrement du NOM DE
		// RESSOURCE (nisasyst : CRC32 du nom, puis SeadRandom) : une capitale de travers et le
		// paquet ne s'ouvre plus. Jusqu'au 2026-08-24 le fichier de drapeaux passait chaque ligne
		// en minuscules et masquait la question ; il rend desormais la valeur telle qu'elle a ete
		// ecrite, donc c'est ici que la garantie doit vivre.
		return strings.ToLower(v)
	}
	return strings.ToLower(id)
}

// stagesTricoloresMaison choisit les deux stages du mode tricolore dans NOTRE table, de facon
// stable pour une fete donnee. La capture en portait deux (2 et 12) ; on en sert deux aussi.
func stagesTricoloresMaison(debut time.Time) []int64 {
	if len(stagesConnus) < 2 {
		return []int64{2, 12}
	}
	a := int(melange(debut.Unix(), 0x7E57) % uint64(len(stagesConnus)))
	b := int(melange(debut.Unix(), 0xF00D) % uint64(len(stagesConnus)-1))
	if b >= a {
		b++
	}
	return []int64{int64(stagesConnus[a]), int64(stagesConnus[b])}
}

// festMaison construit la reponse complete pour l'heure donnee.
func festMaison(maintenant time.Time) *toyohrpb.FestSchedule {
	debut := debutDeLaFeteMaison(maintenant)
	id := identifiantDeFeteMaison(debut)
	ressource := ressourceDeFeteMaison(id)

	ouverture := debut.Add(-festAvantOuverture * time.Second)
	publieDe := ouverture.Add(-festAvantPublie * time.Second)
	publieA := publieDe.Add(festDureePubliee * time.Second)

	publications := make(map[string]*toyohrpb.FestPublishment, len(festRegions))
	for _, r := range festRegions {
		publications[r] = &toyohrpb.FestPublishment{
			StartTime: timestamppb.New(publieDe),
			EndTime:   timestamppb.New(publieA),
		}
	}

	stages := stagesTricoloresMaison(debut)
	tricolores := make([]*commonpb.Value, 0, len(stages))
	for _, s := range stages {
		tricolores = append(tricolores, gsInt(s))
	}

	verifierCreneauxDeFete(debut, versMiParcours(), versFin(), versCloture())

	return &toyohrpb.FestSchedule{
		Name:                 npnTenant + "/festSchedules/" + id,
		Fest:                 npnTenant + "/fests/" + id,
		FestRegions:          append([]string(nil), festRegions...),
		Target:               "default",
		FestGameData:         ressource,
		FestGameDataRevision: empreinteDeFeteMaison(id, ressource),
		// Un decalage mal choisi peut faire tomber deux phases sur le meme creneau, en
		// silence : voir fest_creneaux.go.
		Timetable: &toyohrpb.FestTimetable{
			OpenTime:  timestamppb.New(ouverture),
			StartTime: timestamppb.New(debut),
			MidTime:   timestamppb.New(debut.Add(time.Duration(versMiParcours()) * time.Second)),
			EndTime:   timestamppb.New(debut.Add(time.Duration(versFin()) * time.Second)),
			CloseTime: timestamppb.New(debut.Add(time.Duration(versCloture()) * time.Second)),
		},
		FestPublishments: publications,
		Attributes: &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			"TricolorStages": gsTableau(tricolores...),
		}},
	}
}

// journaliserFeteMaison decrit la fete servie, pour qu'on sache d'un coup d'oeil ce que la console
// a recu — les dates sont la premiere chose qu'on veut verifier quand le jeu boude une fete.
func journaliserFeteMaison(f *toyohrpb.FestSchedule) {
	t := f.GetTimetable()
	log.Printf("[NPLN toyohr] SelectFestSchedule -> fete MAISON %s (paquet %q) : annonce %s, debut %s, mi %s, fin %s, resultats %s",
		lastSeg(f.GetName()), f.GetFestGameData(),
		t.GetOpenTime().AsTime().Format("02/01 15:04"),
		t.GetStartTime().AsTime().Format("02/01 15:04"),
		t.GetMidTime().AsTime().Format("02/01 15:04"),
		t.GetEndTime().AsTime().Format("02/01 15:04"),
		t.GetCloseTime().AsTime().Format("02/01 15:04"))
}
