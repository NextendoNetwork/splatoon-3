package main

// Le verdict d'un festival TERMINE, calcule avec le bareme de Nintendo.
//
// D'OU VIENT LE BAREME. Capture d'une vraie console apres l'ouverture des resultats du festival
// officiel JUEA-00107, le relais de session
// Le champ 5 de GetFestResult porte les totaux par camp ET la carte des poids :
//
//	critere                 1er   2e   3e
//	Yobisai (vote)           90    45    0
//	PlayerCount (popularite) 70    35    0
//	Regular (matchs ouverts) 120   60    0
//	Challenge (matchs defi)  120   60    0
//	Tricolor                 180   90    0
//
// VERIFIE ARITHMETIQUEMENT. Les classements captures donnent Alpha 90+0+0+60+0 = 150,
// Bravo 45+70+60+0+90 = 265, Charlie 0+35+120+120+180 = 455 — exactement les totaux que Nintendo
// renvoie. C'est aussi ce qui identifie l'ordre des sous-champs du champ 4 : PlayerCount, Regular,
// Challenge, Tricolor.
//
// ⚠️ NOTRE PROTO N'A QUE TROIS LISTES. HonsaiFinalResult expose TeamRatios1..3, soit les champs 2,
// 3 et 4 ; la capture en montre QUATRE, le champ 5 portant le tricolore. Plutot que de regenerer
// tout le proto pour un champ, on l'ecrit en champ inconnu — protobuf le transporte tel quel et le
// jeu le lit a sa place.
//
// CE QU'ON SAIT DE NOTRE PROPRE FETE. Aucune bataille n'est encore comptee : le classement de
// l'intermede, qui est FIGE, est la seule information disponible et sert donc les quatre criteres.
// Des que des batailles seront comptees, il suffira de nourrir classementParCritere.

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// poidsDuBareme : les quinze valeurs relevees dans la capture.
var poidsDuBareme = map[string]int64{
	"YobisaiWinPoint": 90, "YobisaiSecondPoint": 45, "YobisaiThirdPoint": 0,
	"PlayerCountWinPoint": 70, "PlayerCountSecondPoint": 35, "PlayerCountThirdPoint": 0,
	"RegularWinPoint": 120, "RegularSecondPoint": 60, "RegularThirdPoint": 0,
	"ChallengeWinPoint": 120, "ChallengeSecondPoint": 60, "ChallengeThirdPoint": 0,
	"TricolorWinPoint": 180, "TricolorSecondPoint": 90, "TricolorThirdPoint": 0,
}

// pointsDuCritere rend les points d'une place pour un critere donne.
func pointsDuCritere(critere string, place int) int64 {
	switch place {
	case 0:
		return poidsDuBareme[critere+"WinPoint"]
	case 1:
		return poidsDuBareme[critere+"SecondPoint"]
	default:
		return poidsDuBareme[critere+"ThirdPoint"]
	}
}

// carteDesPoids rend la carte que Nintendo place en champ 3 de l'overall.
func carteDesPoids() *commonpb.MapValue {
	champs := make(map[string]*commonpb.Value, len(poidsDuBareme))
	for nom, v := range poidsDuBareme {
		champs[nom] = &commonpb.Value{ValueType: &commonpb.Value_IntegerValue{IntegerValue: v}}
	}
	return &commonpb.MapValue{Fields: champs}
}

// rangs fabrique les entrees d'un classement.
//
// ⚠️ CHAQUE ENTREE PORTE UN RATIO, et pas seulement un nom. Mesure du 2026-08-24 sur la capture
// des resultats de JUEA-00107 : les entrees de Nintendo font seize a dix-huit octets, les notres
// en faisaient sept. Le jeu n'avait donc aucun pourcentage a afficher pour les quatre criteres et
// ne jouait pas l'ecran de resultats. Les valeurs captures ne sont pas symboliques — la popularite
// de Bravo y vaut 0,5685, tres loin d'un tiers.
func rangs(ordre []string, parts map[string]float64) []*toyohrpb.FestTeamRatio {
	out := make([]*toyohrpb.FestTeamRatio, 0, len(ordre))
	for _, c := range ordre {
		out = append(out, &toyohrpb.FestTeamRatio{FestTeam: c, Ratio: parts[c]})
	}
	return out
}

// champCinqTricolore encode a la main la quatrieme liste, absente de notre proto.
func champCinqTricolore(ordre []string, parts map[string]float64) []byte {
	var liste []byte
	for _, c := range ordre {
		entree := appendProtoString(nil, 1, c)
		// protobuf3 OMET les valeurs par defaut : un ratio nul ne s'ecrit pas. Sans cela notre
		// champ ecrit a la main est plus long que la liste equivalente produite par le proto, et
		// n'a plus la meme forme que les trois autres criteres.
		if parts[c] != 0 {
			entree = append(entree, pbFlottant(2, parts[c])...)
		}
		liste = append(liste, pbField(5, entree)...)
	}
	return liste
}

// pbFlottant encode le ratio d'un FestTeamRatio.
//
// ⚠️ C'EST UN float64, DONC fixed64 — huit octets, type de fil 1. Je l'avais ecrit en fixed32 le
// 2026-08-24, et le jeu lisait n'importe quoi sur la seule ligne qui passe par ce champ ecrit a la
// main, le tricolore : 180 points attribues au mauvais camp. La verification etait dans les
// chiffres — le serveur calculait 580 pour le vainqueur, l'ecran affichait 400, soit exactement
// les 180 points egares.
//
// La taille confirme la forme : nom (7 octets) plus ratio (1 + 8) font les seize octets par entree
// qu'on mesure dans la capture de Nintendo.
func pbFlottant(champ int, v float64) []byte {
	var b [9]byte
	b[0] = byte(champ<<3 | 1)
	binary.LittleEndian.PutUint64(b[1:], math.Float64bits(v))
	return b[:]
}

// classementsDeLaFete rend, dans l'ordre, le classement du prestival, celui des quatre autres
// criteres, et le total de points par camp.
//
// ⚠️ DEUX FAUTES CORRIGEES LE 2026-08-24, toutes deux visibles a l'ecran.
//
// La premiere : je lisais l'ordre du prestival dans la liste rendue par yobisaiDeLaFete, qui est
// l'ordre CANONIQUE des camps — Alpha, Bravo, Charlie — et pas un classement. Alpha encaissait
// donc les 90 points de la premiere place avec zero vote, pendant que le camp qui avait 100 % des
// conques finissait troisieme a zero.
//
// La seconde : les quatre autres criteres recopiaient le classement FIGE de l'intermede. Ce gel a
// une raison d'etre — il designe le defenseur du tricolore et ne doit plus bouger — mais il datait
// d'avant le premier vote. Un camp sans un seul inscrit raflait 490 points.
//
// Tant qu'aucune bataille n'est comptee, la seule mesure honnete est le vote lui-meme : les quatre
// criteres le suivent donc, et suivront les batailles des qu'il y en aura.
func classementsDeLaFete(festID string) (ordre []string, parts map[string]float64, total map[string]int64) {
	// yobisaiDeLaFete rend deja la bonne mesure : les votes reels s'il y en a, les parts de depart
	// sinon. On trie DESSUS — avant, sans vote, on retombait sur l'ordre canonique des camps et un
	// camp encaissait 580 points pendant que l'ecran en donnait un autre en tete.
	parts = map[string]float64{}
	for _, r := range yobisaiDeLaFete(festID).GetTeamRatios() {
		parts[r.GetFestTeam()] = r.GetRatio()
	}
	ordre = classementParParts(parts)
	places := placesAvecEgalites(ordre, parts)

	total = map[string]int64{}
	for i, camp := range ordre {
		total[camp] += pointsDuCritere("Yobisai", places[i])
	}
	for _, critere := range []string{"PlayerCount", "Regular", "Challenge", "Tricolor"} {
		parCamp, _ := placesDuCritere(festID, critere, ordre, parts)
		for _, camp := range ordre {
			total[camp] += pointsDuCritere(critere, parCamp[camp])
		}
	}
	return ordre, parts, total
}

// placesAvecEgalites rend, pour chaque position du classement, la place a retenir pour les points.
//
// ⚠️ POURQUOI CE N'EST PAS SIMPLEMENT L'INDICE. Le tri departage les ex aequo par ORDRE
// ALPHABETIQUE, ce qui est bien pour obtenir une liste stable mais faux pour distribuer des points.
// Mesure du 2026-08-24 sur JUEA-00208 : un seul joueur avait vote, pour Charlie. Alpha et Bravo
// etaient donc a 0 % tous les deux — et Alpha, pour la seule raison que son nom vient avant celui
// de Bravo, encaissait les points de deuxieme place sur les cinq criteres, soit 290 points. Un camp
// que personne n'a choisi affichait presque autant qu'un camp reellement soutenu.
//
// NOTRE REGLE : les camps que la mesure ne separe pas partagent la DERNIERE place de leur groupe.
// Deux camps a egalite sur les places 2 et 3 prennent donc la troisieme, et zero point : ils n'ont
// devance personne, aucun des deux ne peut revendiquer la deuxieme place. Nintendo n'a jamais
// d'egalite exacte — ses parts ont six decimales et des millions de joueurs — donc aucune capture
// ne dit ce qu'il ferait ; la regle est a nous, et elle est ecrite ici pour qu'on sache qu'elle
// l'est.
// modeDuCritere : quel game_mode alimente ce critere de notation.
//
// Les numeros de mode des festimatchs ne sont pas connus tant qu'aucune fete n'a tourne — les
// deviner aurait fausse le classement en silence. Ils se posent donc a chaud, des qu'on les voit
// passer : « festmoderegular=<n> » pour le festimatch ouvert, « festmodechallenge=<n> » pour le
// defi. Tant qu'ils sont vides, le critere recopie l'ordre du vote comme avant, et l'API le declare
// en mettant « reel » a faux.
func modeDuCritere(critere string) string {
	switch critere {
	case "Regular":
		return soirFlagValeur("festmoderegular")
	case "Challenge":
		return soirFlagValeur("festmodechallenge")
	}

	return ""
}

// placesDuCritere rend la place de chaque camp pour un critere donne, et dit si ce classement est
// MESURE sur de vraies batailles ou recopie de l'ordre du vote.
func placesDuCritere(festID, critere string, ordre []string, parts map[string]float64) (map[string]int, bool) {
	if mode := modeDuCritere(critere); mode != "" {
		if v := victoiresDuMode(festID, mode); len(v) > 0 {
			total := 0
			for _, n := range v {
				total += n
			}
			// Un camp sans victoire garde une part nulle : l'omettre le ferait disparaitre du
			// classement au lieu de le placer dernier.
			p := make(map[string]float64, len(ordre))
			for _, camp := range ordre {
				if total > 0 {
					p[camp] = float64(v[camp]) / float64(total)
				}
			}
			o := classementParParts(p)
			pl := placesAvecEgalites(o, p)
			out := make(map[string]int, len(o))
			for i, camp := range o {
				out[camp] = pl[i]
			}

			return out, true
		}
	}

	pl := placesAvecEgalites(ordre, parts)
	out := make(map[string]int, len(ordre))
	for i, camp := range ordre {
		out[camp] = pl[i]
	}

	return out, false
}

func placesAvecEgalites(ordre []string, parts map[string]float64) []int {
	places := make([]int, len(ordre))
	for i := 0; i < len(ordre); {
		j := i
		for j+1 < len(ordre) && parts[ordre[j+1]] == parts[ordre[i]] {
			j++
		}
		for k := i; k <= j; k++ {
			places[k] = j
		}
		i = j + 1
	}

	return places
}

// classementParParts trie du plus grand au plus petit, a egalite par nom pour rester stable.
func classementParParts(parts map[string]float64) []string {
	camps := make([]string, 0, len(parts))
	for c := range parts {
		camps = append(camps, c)
	}
	sort.Slice(camps, func(i, j int) bool {
		if parts[camps[i]] != parts[camps[j]] {
			return parts[camps[i]] > parts[camps[j]]
		}
		return camps[i] < camps[j]
	})
	return camps
}

// entiers convertit un comptage de votes en carte de points, pour reutiliser le meme tri.
func entiers(compte map[string]int) map[string]int64 {
	out := make(map[string]int64, len(compte))
	for k, v := range compte {
		out[k] = int64(v)
	}
	return out
}

// resultatDeFeteTerminee rend le verdict complet d'une fete close.
func resultatDeFeteTerminee(nom, festID string) *toyohrpb.FestResult {
	y := yobisaiDeLaFete(festID)
	ordre, parts, total := classementsDeLaFete(festID)

	points := make([]*toyohrpb.FestTeamPoint, 0, len(ordre))
	for _, camp := range classementParPoints(total) {
		points = append(points, &toyohrpb.FestTeamPoint{FestTeam: camp, Points: total[camp]})
	}

	final := &toyohrpb.HonsaiFinalResult{
		IsValid:     true,
		TeamRatios1: rangs(ordre, parts), // PlayerCount
		TeamRatios2: rangs(ordre, parts), // Regular
		TeamRatios3: rangs(ordre, parts), // Challenge
	}
	final.ProtoReflect().SetUnknown(champCinqTricolore(ordre, parts))

	log.Printf("[NPLN fest] verdict de %s : %s", festID, resumeDesPoints(points))

	return &toyohrpb.FestResult{
		Name:          nom,
		Yobisai:       y,
		HonsaiMidterm: intermedeDuVerdict(festID),
		HonsaiFinal:   final,
		Overall: &toyohrpb.OverallResult{
			IsValid: true,
			Points:  points,
			Weights: carteDesPoids(),
		},
		Unk: int32(len(ordre)),
	}
}

func resumeDesPoints(points []*toyohrpb.FestTeamPoint) string {
	out := ""
	for i, p := range points {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s=%d", p.GetFestTeam(), p.GetPoints())
	}
	return out
}

// classementParPoints trie les camps du plus grand total au plus petit, a egalite par nom pour
// que deux appels rendent toujours la meme chose.
func classementParPoints(total map[string]int64) []string {
	camps := make([]string, 0, len(total))
	for c := range total {
		camps = append(camps, c)
	}
	sort.Slice(camps, func(i, j int) bool {
		if total[camps[i]] != total[camps[j]] {
			return total[camps[i]] > total[camps[j]]
		}
		return camps[i] < camps[j]
	})
	return camps
}

// intermedeDuVerdict rend l'intermede, en le FIGEANT s'il ne l'a jamais ete.
//
// ⚠️ UN VERDICT SANS INTERMEDE EST INCOMPLET, et le jeu le dit mot pour mot : « je n'ai pas encore
// recu tous les resultats ». Mesure du 2026-08-24 sur JUEA-00207 — une fete passee directement de
// « en cours » a « close » n'avait jamais fige son intermede, parce que seule la reponse d'une
// fete EN COURS declenche le gel. Le champ 3 partait donc absent, et l'ecran de resultats restait
// bloque sur la scene d'attente.
//
// On le gele donc ici si besoin, avec la meme chaine de reperes que pendant la fete : la ferveur
// d'abord, les victoires ensuite, le vote a defaut.
func intermedeDuVerdict(festID string) *toyohrpb.HonsaiMidtermResult {
	if m := midtermFigeOuRien(festID); m != nil {
		return m
	}
	if t := festMaison(time.Now()).GetTimetable().GetMidTime(); t != nil {
		if m := honsaiMidtermDeLaFete(festID, t.AsTime()); m != nil {
			return m
		}
	}
	return midtermFigeOuRien(festID)
}

// midtermFigeOuRien rend l'intermede tel qu'il a ete fige, sans le recalculer.
func midtermFigeOuRien(festID string) *toyohrpb.HonsaiMidtermResult {
	parts := intermedeFige(festID)
	if len(parts) == 0 {
		return nil
	}
	out := make([]*toyohrpb.FestTeamRatio, 0, len(parts))
	for _, camp := range classement(parts) {
		out = append(out, &toyohrpb.FestTeamRatio{FestTeam: camp, Ratio: parts[camp]})
	}
	return &toyohrpb.HonsaiMidtermResult{IsValid: true, TeamRatios: out}
}

var _ = proto.Marshal
