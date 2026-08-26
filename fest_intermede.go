package main

// L'INTERMEDE d'un Splatfest : qui mene a mi-parcours, et donc qui defendra en tricolore.
//
// CE QUE FAIT NINTENDO, MESURE. Capture du 2026-08-22 (festival ouvert, avant la mi-parcours) :
// GetFestResult ne porte QUE le vote preliminaire, trois parts qui totalisent 1,000030 —
// Alpha 0,340110, Bravo 0,330110, Charlie 0,329810 — et honsai_midterm reste absent.
//
// Capture du 2026-08-23, tricolore ouvert : la meme reponse porte DEUX blocs de parts, somme
// 2,000060. Le premier est le vote preliminaire, inchange. Le second est l'intermede :
//
//	Bravo 0,334710   Charlie 0,334110   Alpha 0,331210
//
// Deux choses s'en deduisent. Les camps y sont ranges par part DECROISSANTE, vainqueur en tete,
// alors que le vote preliminaire suit l'ordre canonique. Et Bravo, en tete de l'intermede, est
// exactement le camp qui defend dans les CINQ batailles tricolores de la capture — quatre joueurs
// Bravo en personal_data_team3 a chaque fois, face a deux Alpha et deux Charlie.
//
// L'intermede designe donc le defenseur, et il est FIGE pour toute la seconde moitie du festival.
//
// CE QUE NINTENDO NE NOUS DIT PAS : COMMENT il calcule ces parts. Chez eux cela mele le vote
// preliminaire, les victoires en bataille et la ferveur, avec une ponderation que la capture ne
// revele pas. On ne peut pas la reproduire — alors on ne fait pas semblant.
//
// NOTRE REGLE, ASSUMEE COMME TELLE : la part d'un camp est sa part des BATAILLES GAGNEES depuis
// l'ouverture du festival. C'est ce que nous savons compter honnetement, et ce que les joueurs
// peuvent verifier en jouant. Ce n'est PAS le modele de Nintendo, et ce commentaire est la pour que
// personne ne le prenne un jour pour tel.
//
// Sans aucune bataille jouee, on retombe sur le vote preliminaire : c'est la seule information
// disponible a cet instant, et elle vaut mieux qu'un partage arbitraire.

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// cheminVictoiresFest : le comptage survit aux redemarrages, sinon un redeploiement en pleine fete
// remettrait l'intermede a zero.
func cheminVictoiresFest() string {
	return filepath.Join(filepath.Dir(saveDir()), "fest_victoires.json")
}

type comptageFest struct {
	// Victoires par camp, depuis l'ouverture.
	Victoires map[string]int `json:"victoires"`
	// Ferveur cumulee par camp : la somme des points d'encre de ses joueurs.
	Ferveur map[string]int64 `json:"ferveur,omitempty"`
	// Victoires VENTILEES PAR MODE : game_mode -> camp -> nombre.
	//
	// Le bareme note separement « matchs ouverts » et « matchs defi », et ces deux criteres valent
	// 120 points chacun a la premiere place. Le compte global ci-dessus les melange, donc il ne peut
	// pas les departager. On les separe ici des la premiere bataille de la fete, meme si la
	// correspondance mode -> critere n'est branchee qu'ensuite : les donnees ne se rattrapent pas
	// apres coup, le classement si.
	ParMode map[string]map[string]int `json:"par_mode,omitempty"`
	// Intermede FIGE : une fois la mi-parcours passee, la photo ne bouge plus.
	Intermede map[string]float64 `json:"intermede,omitempty"`
}

var victoiresFest = struct {
	sync.Mutex
	m      map[string]*comptageFest
	chargé bool
}{m: map[string]*comptageFest{}}

func chargerVictoiresLocked() {
	if victoiresFest.chargé {
		return
	}
	victoiresFest.chargé = true
	b, err := os.ReadFile(cheminVictoiresFest())
	if err != nil {
		return
	}
	_ = json.Unmarshal(b, &victoiresFest.m)
}

func enregistrerVictoiresLocked() {
	b, err := json.MarshalIndent(victoiresFest.m, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(cheminVictoiresFest(), b, 0o644); err != nil {
		log.Printf("[NPLN fest] ecriture du comptage des victoires : %v", err)
	}
}

// enregistrerVictoireDeFete ajoute une bataille gagnee au camp indique.
func enregistrerVictoireDeFete(festID, camp, mode string) {
	if festID == "" || camp == "" {
		return
	}
	victoiresFest.Lock()
	defer victoiresFest.Unlock()
	chargerVictoiresLocked()

	c := victoiresFest.m[festID]
	if c == nil {
		c = &comptageFest{Victoires: map[string]int{}}
		victoiresFest.m[festID] = c
	}
	if c.Victoires == nil {
		c.Victoires = map[string]int{}
	}
	c.Victoires[camp]++
	if mode != "" {
		if c.ParMode == nil {
			c.ParMode = map[string]map[string]int{}
		}
		if c.ParMode[mode] == nil {
			c.ParMode[mode] = map[string]int{}
		}
		c.ParMode[mode][camp]++
	}
	total := 0
	for _, n := range c.Victoires {
		total += n
	}
	enregistrerVictoiresLocked()
	log.Printf("[NPLN fest] victoire pour %s (%s) — %d bataille(s) comptee(s) sur cette fete", camp, festID, total)
}

// modeDuVerdict rend le game_mode de la bataille, sous forme de chaine, ou "" s'il est absent.
func modeDuVerdict(verdict *commonpb.MapValue) string {
	f := verdict.GetFields()["game_mode"]
	if f == nil {
		return ""
	}
	if v := f.GetIntegerValue(); v != 0 {
		return strconv.FormatInt(v, 10)
	}
	if s := f.GetStringValue(); s != "" {
		return s
	}

	return ""
}

// victoiresDuMode rend le nombre de batailles gagnees par camp pour un game_mode donne, ou nil si
// ce mode n'a encore rien produit. C'est ce qui permet de noter « matchs ouverts » et « matchs
// defi » separement, au lieu de recopier l'ordre du vote pour les deux.
func victoiresDuMode(festID, mode string) map[string]int {
	if festID == "" || mode == "" {
		return nil
	}

	victoiresFest.Lock()
	defer victoiresFest.Unlock()
	chargerVictoiresLocked()

	c := victoiresFest.m[festID]
	if c == nil || len(c.ParMode[mode]) == 0 {
		return nil
	}

	out := make(map[string]int, len(c.ParMode[mode]))
	for camp, n := range c.ParMode[mode] {
		out[camp] = n
	}

	return out
}

// partsDesVictoires rend la part de chaque camp dans les batailles gagnees, ou nil s'il n'y en a
// aucune. Les camps sans victoire figurent avec une part nulle : un camp absent du resultat
// disparaitrait de l'affichage.
func partsDesVictoires(festID string) map[string]float64 {
	victoiresFest.Lock()
	defer victoiresFest.Unlock()
	chargerVictoiresLocked()

	c := victoiresFest.m[festID]
	if c == nil {
		return nil
	}
	total := 0
	for _, n := range c.Victoires {
		total += n
	}
	if total == 0 {
		return nil
	}
	parts := map[string]float64{}
	for _, camp := range campsCanoniques {
		parts[camp] = float64(c.Victoires[camp]) / float64(total)
	}
	for camp, n := range c.Victoires {
		if _, vu := parts[camp]; !vu {
			parts[camp] = float64(n) / float64(total)
		}
	}
	return parts
}

// figerIntermede prend la photo, une fois pour toutes.
func figerIntermede(festID string, parts map[string]float64) {
	victoiresFest.Lock()
	defer victoiresFest.Unlock()
	chargerVictoiresLocked()

	c := victoiresFest.m[festID]
	if c == nil {
		c = &comptageFest{Victoires: map[string]int{}}
		victoiresFest.m[festID] = c
	}
	if c.Intermede != nil {
		return // deja fige : l'intermede ne rebouge plus
	}
	c.Intermede = parts
	enregistrerVictoiresLocked()
	log.Printf("[NPLN fest] INTERMEDE fige pour %s : %s defend", festID, classement(parts)[0])
}

// intermedeFige rend la photo si elle existe.
func intermedeFige(festID string) map[string]float64 {
	victoiresFest.Lock()
	defer victoiresFest.Unlock()
	chargerVictoiresLocked()
	if c := victoiresFest.m[festID]; c != nil {
		return c.Intermede
	}
	return nil
}

// classement rend les camps du mieux place au moins bien. C'est l'ordre dans lequel Nintendo publie
// l'intermede — mesure du 2026-08-23, ou Bravo (0,334710) precede Charlie (0,334110) puis Alpha
// (0,331210), alors que le vote preliminaire suit l'ordre canonique.
func classement(parts map[string]float64) []string {
	camps := make([]string, 0, len(parts))
	for c := range parts {
		camps = append(camps, c)
	}
	sort.Slice(camps, func(i, j int) bool {
		if parts[camps[i]] != parts[camps[j]] {
			return parts[camps[i]] > parts[camps[j]]
		}
		return camps[i] < camps[j] // depart au nom, pour que l'ordre soit stable
	})
	return camps
}

// campDefenseur rend le camp qui defend en tricolore : le vainqueur de l'intermede. Vide tant que
// l'intermede n'est pas fige.
func campDefenseur(festID string) string {
	parts := intermedeFige(festID)
	if len(parts) == 0 {
		return ""
	}
	return classement(parts)[0]
}

// honsaiMidtermDeLaFete rend le bloc honsai_midterm, ou nil si la mi-parcours n'est pas passee.
//
// Avant mid_time le champ doit rester ABSENT : c'est ce que montre la capture du 2026-08-22, ou
// GetFestResult ne porte que le vote preliminaire.
func honsaiMidtermDeLaFete(festID string, midTime time.Time) *toyohrpb.HonsaiMidtermResult {
	if midTime.IsZero() || time.Now().Before(midTime) {
		return nil
	}

	parts := intermedeFige(festID)
	if parts == nil {
		// LA FERVEUR D'ABORD : c'est la mesure la plus fine dont on dispose, et celle qui
		// recompense de jouer plutot que de gagner. Les victoires ne servent que si personne
		// n'a encore encre quoi que ce soit.
		parts = partsDeFerveur(festID)
		if parts == nil {
			parts = partsDesVictoires(festID)
		}
		if parts == nil {
			// Aucune bataille jouee avant la mi-parcours : le vote preliminaire est la seule
			// information dont on dispose, et elle vaut mieux qu'un partage arbitraire.
			parts = map[string]float64{}
			for _, r := range yobisaiDeLaFete(festID).GetTeamRatios() {
				parts[r.GetFestTeam()] = r.GetRatio()
			}
			log.Printf("[NPLN fest] intermede %s : aucune bataille comptee, on reprend le vote preliminaire", festID)
		}
		figerIntermede(festID, parts)
		parts = intermedeFige(festID)
	}

	rangs := make([]*toyohrpb.FestTeamRatio, 0, len(parts))
	for _, camp := range classement(parts) {
		rangs = append(rangs, &toyohrpb.FestTeamRatio{FestTeam: camp, Ratio: parts[camp]})
	}
	return &toyohrpb.HonsaiMidtermResult{IsValid: true, TeamRatios: rangs}
}

// campDuJoueur rend le camp choisi par ce joueur pour cette fete, ou vide s'il n'a pas vote.
func campDuJoueur(festID, uid string) string {
	entreesFest.Lock()
	defer entreesFest.Unlock()
	chargerEntreesFestLocked()
	if e, ok := entreesFest.m[festID+"/"+uid]; ok {
		return e.Equipe
	}
	return ""
}

// campVainqueur rend le camp du slot gagnant d'une bataille de fete.
//
// Le verdict dit quel SLOT l'emporte (team_result.winner vaut 1, 2 ou 3) ; les fiches de
// personal_result disent quel slot occupe chaque joueur. Le camp se lit ensuite dans nos
// inscriptions. On prend la MAJORITE des camps du slot : une equipe de fete est homogene — mesure
// du 2026-08-23, les quatre defenseurs sont tous Bravo dans les cinq batailles — mais un joueur
// sans inscription connue ne doit pas faire basculer le compte.
func campVainqueur(festID string, verdict *commonpb.MapValue, slot int64) string {
	if festID == "" || slot == 0 || verdict == nil {
		return ""
	}
	personal := verdict.GetFields()["personal_result"].GetMapValue()
	if personal == nil {
		return ""
	}

	compte := map[string]int{}
	for _, fiche := range personal.GetFields() {
		m := fiche.GetMapValue()
		if m == nil || m.GetFields()["team"].GetIntegerValue() != slot {
			continue
		}
		uid := m.GetFields()["npln_user_id"].GetStringValue()
		if camp := campDuJoueur(festID, uid); camp != "" {
			compte[camp]++
		}
	}

	meilleur, combien := "", 0
	for camp, n := range compte {
		if n > combien || (n == combien && camp < meilleur) {
			meilleur, combien = camp, n
		}
	}
	return meilleur
}

// compterLaBatailleDeFete enregistre le vainqueur d'une bataille, si c'en est une de festival et si
// l'intermede n'est pas deja fige — apres la mi-parcours, la photo ne bouge plus.
func compterLaBatailleDeFete(festID string, verdict *commonpb.MapValue) {
	if festID == "" || verdict == nil {
		return
	}
	if intermedeFige(festID) != nil {
		return
	}
	slot := verdict.GetFields()["team_result"].GetMapValue().GetFields()["winner"].GetIntegerValue()
	if camp := campVainqueur(festID, verdict, slot); camp != "" {
		enregistrerVictoireDeFete(festID, camp, modeDuVerdict(verdict))
	}
}

// ---- LA FERVEUR ------------------------------------------------------------------------------
//
// CE QUE FAIT NINTENDO, MESURE SUR SEPT BATAILLES (captures des 2026-08-22 et 23). Le champ
// fest_info.contribution du VsResults est la ferveur d'un joueur pour une bataille. Il est calcule
// PAR NINTENDO — la console ne nous l'envoie jamais — et sa structure est nette :
//
//	bataille de fete ordinaire   [1410 1410 1410 1410 | 0 0 0 0]
//	                             les quatre vainqueurs touchent la MEME somme, les perdants zero
//	tricolore                    [7950 7950 | 16500 16500 | 1223 969 818 702]
//	                             chaque paire d'attaquants touche une somme PLATE — 16500 pour celle
//	                             qui a plante les deux signaux, 7950 pour celle qui n'en a plante
//	                             aucun — pendant que les QUATRE defenseurs touchent des montants
//	                             INDIVIDUELS, proches de leurs points d'encre (1223, 969, 818, 702).
//
// Les constantes 16500, 8850, 7950, 1410 reviennent a l'identique d'une bataille a l'autre : ce sont
// bien des paliers, pas des calculs. Mais sept echantillons ne suffisent pas a en tirer la formule —
// les multiplicateurs 10x et 100x (dragon_cert) s'y melent, et on ne les a pas isoles.
//
// NOTRE REGLE, ASSUMEE. La ferveur d'un camp est la somme des POINTS D'ENCRE de ses joueurs. C'est
// la grandeur la plus fine que la console nous rapporte reellement, elle recompense de jouer plutot
// que de gagner, et un joueur peut la verifier a l'ecran de score. Ce n'est pas le bareme de
// Nintendo, et les paliers ci-dessus sont notes ici pour le jour ou l'on aura de quoi le reverser.

// enregistrerFerveur ajoute l'encre d'un camp au compteur de la fete.
func enregistrerFerveur(festID, camp string, encre int64) {
	if festID == "" || camp == "" || encre <= 0 {
		return
	}
	victoiresFest.Lock()
	defer victoiresFest.Unlock()
	chargerVictoiresLocked()

	c := victoiresFest.m[festID]
	if c == nil {
		c = &comptageFest{Victoires: map[string]int{}}
		victoiresFest.m[festID] = c
	}
	if c.Ferveur == nil {
		c.Ferveur = map[string]int64{}
	}
	c.Ferveur[camp] += encre
	enregistrerVictoiresLocked()
}

// partsDeFerveur rend la part de chaque camp dans la ferveur totale, ou nil s'il n'y en a aucune.
func partsDeFerveur(festID string) map[string]float64 {
	victoiresFest.Lock()
	defer victoiresFest.Unlock()
	chargerVictoiresLocked()

	c := victoiresFest.m[festID]
	if c == nil || len(c.Ferveur) == 0 {
		return nil
	}
	var total int64
	for _, n := range c.Ferveur {
		total += n
	}
	if total == 0 {
		return nil
	}
	parts := map[string]float64{}
	for _, camp := range campsCanoniques {
		parts[camp] = float64(c.Ferveur[camp]) / float64(total)
	}
	for camp, n := range c.Ferveur {
		if _, vu := parts[camp]; !vu {
			parts[camp] = float64(n) / float64(total)
		}
	}
	return parts
}

// compterLaFerveurDeLaBataille additionne l'encre de chaque joueur au compteur de son camp.
func compterLaFerveurDeLaBataille(festID string, verdict *commonpb.MapValue) {
	if festID == "" || verdict == nil || intermedeFige(festID) != nil {
		return
	}
	personal := verdict.GetFields()["personal_result"].GetMapValue()
	if personal == nil {
		return
	}
	parCamp := map[string]int64{}
	for _, fiche := range personal.GetFields() {
		m := fiche.GetMapValue()
		if m == nil {
			continue
		}
		encre := m.GetFields()["paint_point"].GetIntegerValue()
		uid := m.GetFields()["npln_user_id"].GetStringValue()
		if camp := campDuJoueur(festID, uid); camp != "" && encre > 0 {
			parCamp[camp] += encre
		}
	}
	for camp, encre := range parCamp {
		enregistrerFerveur(festID, camp, encre)
	}
	if len(parCamp) > 0 {
		log.Printf("[NPLN fest] ferveur de la bataille (%s) : %v", festID, parCamp)
	}
}
