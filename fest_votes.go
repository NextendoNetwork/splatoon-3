package main

// Le partage des votes d'un Splatfest — ce que le jeu appelle « yobisai », le vote preliminaire.
//
// CE QUE FAIT LE VRAI SERVEUR, MESURE. Capture du 2026-08-22, Splatfest officiel JUEA-00107 en
// cours, console reelle contre Nintendo. A chaque lancement du jeu, juste apres GetSaveRecord, la
// console appelle GetFestResult — et pendant une fete OUVERTE la reponse n'est ni vide ni absente :
//
//	name            tenants/t-dce9377b-lp1/festResults/JUEA-00107
//	yobisai         is_valid = true
//	                  Alpha    0.340110
//	                  Bravo    0.330110
//	                  Charlie  0.329810
//	honsai_midterm  absent
//	honsai_final    absent
//	overall         { weights: {} }
//	unk             3
//
// Les trois parts totalisent 1,000030 : c'est le partage des votes, en fractions et non en
// pourcentages. Les phases non encore jouees restent absentes — seul yobisai est rempli.
//
// CE QUE NOUS FAISIONS. Nous rendions FestResult{Name} et rien d'autre. Le jeu recevait donc un
// objet sans le moindre chiffre la ou le vrai serveur en donne trois, et la phase « fete
// commencee » n'allait pas plus loin. C'est le mur sur lequel on butait depuis le debut.
//
// D'OU VIENNENT NOS CHIFFRES. De nos propres inscriptions : CreateFestEntry porte {equipe, region}
// et nous les gardons deja, une par joueur et par fete (voir fest_entries.go). Compter les camps et
// diviser suffit — rien a inventer.
//
// LE CAS SANS AUCUN VOTE N'EXISTE PAS CHEZ NINTENDO — un Splatoon 3 en fete a toujours des
// joueurs, et la question ne s'y pose donc jamais. Elle se pose CHEZ NOUS : notre population tient
// dans une poignee de testeurs, et une fete peut demarrer avant que quiconque ait choisi son camp.
// Des parts nulles donneraient une somme de zero la ou le jeu attend l'unite, alors on partage a
// egalite. Ce n'est pas un modele du serveur de Nintendo, c'est un garde-fou pour le notre.

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// campsCanoniques : les noms que NPLN emploie pour les trois camps, releves sur la capture. Ce ne
// sont pas les noms affiches — ceux-la vivent dans le paquet BCAT — mais les cles du protocole,
// celles que CreateFestEntry nous renvoie deja telles quelles.
var campsCanoniques = []string{"Alpha", "Bravo", "Charlie"}

// unkDuFestResult : la capture porte 3, sur une fete a trois camps. Nous ne savons pas ce que ce
// champ signifie — le proto l'appelle « unk » faute de mieux. On rend le nombre de camps, lecture
// la plus naturelle du seul releve dont on dispose.
func unkDuFestResult(nbCamps int) int32 { return int32(nbCamps) }

// votesParCamp compte les inscriptions d'une fete, par camp.
func votesParCamp(festID string) map[string]int {
	compte := map[string]int{}
	entreesFest.Lock()
	chargerEntreesFestLocked()
	for cle, e := range entreesFest.m {
		if i := strings.Index(cle, "/"); i < 0 || !strings.EqualFold(cle[:i], festID) {
			continue
		}
		if e.Equipe == "" {
			continue
		}
		compte[e.Equipe]++
	}
	entreesFest.Unlock()
	return compte
}

// campsDeLaFete rend les camps a publier : ceux qui ont recu au moins un vote, completes par les
// camps canoniques pour qu'une fete a trois camps en presente toujours trois.
func campsDeLaFete(compte map[string]int) []string {
	vus := map[string]bool{}
	camps := make([]string, 0, 3)
	for _, c := range campsCanoniques {
		camps = append(camps, c)
		vus[c] = true
	}
	// Un camp nomme autrement par le jeu (fete a deux camps, nom exotique) ne doit pas disparaitre.
	autres := make([]string, 0, 2)
	for c := range compte {
		if !vus[c] {
			autres = append(autres, c)
		}
	}
	sort.Strings(autres)
	return append(camps, autres...)
}

// yobisaiDeLaFete rend le partage des votes, dans la forme exacte de la capture.
func yobisaiDeLaFete(festID string) *toyohrpb.YobisaiResult {
	compte := votesParCamp(festID)
	camps := campsDeLaFete(compte)

	total := 0
	for _, c := range camps {
		total += compte[c]
	}

	parts := make([]*toyohrpb.FestTeamRatio, 0, len(camps))
	for _, c := range camps {
		var r float64
		if total > 0 {
			r = float64(compte[c]) / float64(total)
		} else {
			// Aucun vote encore. Un tiers pile pour chacun n'existe PAS chez Nintendo : la capture
			// du festival JUEA-00107 donne 0,340110 / 0,330110 / 0,329810, trois valeurs distinctes
			// dont la somme fait 1,000030 — pas 1. Le jeu attend donc un partage inegal, et c'est ce
			// qui l'empechait de passer en mode festival quand personne n'avait encore vote.
			//
			// On en fabrique un de la meme forme, tire de l'identifiant de la fete : distinct par
			// camp, autour du tiers, et STABLE — deux appels successifs doivent rendre la meme chose,
			// sinon les pourcentages sautent d'un rafraichissement a l'autre.
			r = partDeDepart(festID, c, camps)
		}
		parts = append(parts, &toyohrpb.FestTeamRatio{FestTeam: c, Ratio: r})
	}

	if total > 0 {
		detail := make([]string, 0, len(camps))
		for _, c := range camps {
			detail = append(detail, c+"="+strconv.Itoa(compte[c]))
		}
		log.Printf("[NPLN fest] yobisai %s : %d vote(s) — %s", festID, total, strings.Join(detail, " "))
	} else {
		detail := make([]string, 0, len(camps))
		for _, c := range camps {
			detail = append(detail, fmt.Sprintf("%s=%.4f", c, partDeDepart(festID, c, camps)))
		}
		log.Printf("[NPLN fest] yobisai %s : aucun vote, parts de depart — %s", festID, strings.Join(detail, " "))
	}

	return &toyohrpb.YobisaiResult{IsValid: true, TeamRatios: parts}
}

// resultatDeFeteEnCours rend le FestResult d'une fete qui court : yobisai rempli, les phases
// suivantes absentes, et l'enveloppe « overall » vide que porte la capture.
func resultatDeFeteEnCours(nom, festID string) *toyohrpb.FestResult {
	y := yobisaiDeLaFete(festID)

	// L'INTERMEDE. Absent avant la mi-parcours — mesure du 2026-08-22, ou GetFestResult ne porte
	// que le vote preliminaire. Rempli ensuite, et FIGE : c'est lui qui designe le camp defenseur
	// du tricolore. Voir fest_intermede.go.
	var midterm *toyohrpb.HonsaiMidtermResult
	if t := festMaison(time.Now()).GetTimetable().GetMidTime(); t != nil {
		midterm = honsaiMidtermDeLaFete(festID, t.AsTime())
	}

	return &toyohrpb.FestResult{
		Name:          nom,
		Yobisai:       y,
		HonsaiMidterm: midterm,
		// La capture porte overall = { weights: {} } : le champ existe, sa carte est vide.
		Overall: &toyohrpb.OverallResult{Weights: &commonpb.MapValue{}},
		Unk:     unkDuFestResult(len(y.GetTeamRatios())),
	}
}

// feteEnCours dit si une fete maison court en ce moment — c'est-a-dire si l'heure presente est
// avant sa cloture. C'est le meme test que GetFestResult, sorti ici parce que la reponse de
// GetFestDecryptionKey en depend aussi : Nintendo retient la cle du vainqueur tant qu'il n'y en a
// pas (mesure du 2026-08-22, la reponse ne porte alors que le nom, notice_key et start_key).
func feteEnCours() bool {
	if !festMaisonActif() {
		return false
	}
	fin := festMaison(time.Now()).GetTimetable().GetCloseTime()
	return fin != nil && fin.AsTime().After(time.Now())
}

// partDeDepart rend la part d'un camp avant tout vote : proche du tiers, distincte des autres,
// stable pour une fete donnee, et dont la SOMME a la forme de celle que Nintendo sert.
//
// La forme vient de la capture officielle (JUEA-00107) : 0,340110 / 0,330110 / 0,329810, trois
// valeurs distinctes autour du tiers dont la somme fait 1,000030 — un poil au-dessus de l'unite,
// jamais en dessous.
//
// ⚠️ LES ECARTS DOIVENT SE COMPENSER. La premiere version tirait un ecart par camp sans se
// soucier des autres : rien ne garantissait qu'ils s'annulent, et pour la fete JUEA-00210 la somme
// tombait a 0,994010, soit six dixiemes de pour cent SOUS l'unite quand la capture est a trois
// millemes AU-DESSUS. Personne ne sait ce que la console fait d'un partage qui ne totalise pas, et
// ce chiffre part vers toutes les consoles pendant la fete. On retranche donc a chaque ecart la
// moyenne des ecarts du meme festival : leur somme devient nulle par construction, et le total
// retombe sur 1 + n x 0,00001 — exactement 1,000030 a trois camps.
func partDeDepart(festID, camp string, camps []string) float64 {
	n := len(camps)
	if n == 0 {
		return 0
	}

	var moyenne float64
	for _, c := range camps {
		moyenne += ecartDeDepart(festID, c)
	}
	moyenne /= float64(n)

	return 1.0/float64(n) + ecartDeDepart(festID, camp) - moyenne + 0.00001
}

// ecartDeDepart tire l'ecart au tiers d'un camp : au plus un demi-point, toujours le meme pour un
// couple festival/camp donne.
func ecartDeDepart(festID, camp string) float64 {
	h := sha1.Sum([]byte(festID + "/" + camp))

	// -5 000 a +5 000 dix-milliemes.
	return (float64(binary.BigEndian.Uint16(h[:2])%10001) - 5000) / 1e6
}
