package main

// /api/splatfest — tout ce que le serveur sait du festival en cours, pour le tableau de bord.
//
// CE QUE CETTE ROUTE APPORTE. Jusqu'ici, suivre une fete voulait dire lire le journal du serveur et
// recouper trois fichiers a la main : les drapeaux pour le calendrier, fest_entries.json pour les
// votes, et le code du bareme pour comprendre les points. Cette route rassemble les trois et rend
// exactement ce que le jeu, lui, calcule — pas une approximation refaite ailleurs. Les parts, les
// classements et les points sortent de classementsDeLaFete, la MEME fonction qui alimente le
// verdict envoye a la console. Si l'ecran de resultats et cette page divergeaient un jour, ce
// serait un bug du serveur, pas un ecart d'affichage.
//
// ⚠️ ELLE DIT AUSSI CE QU'ELLE NE MESURE PAS. Sur cinq criteres de notation, un seul est
// reellement mesure aujourd'hui — le vote preliminaire. Les quatre autres (popularite, matchs
// ouverts, matchs defi, tricolore) recopient l'ordre du vote faute de batailles comptees. Chaque
// ligne de detail le declare (« reel »), pour qu'on ne lise jamais 180 points de tricolore comme la
// preuve qu'un match tricolore a eu lieu.
//
// Lecture seule, et derriere le meme jeton que /api/stats.

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

// libelleDesCriteres nomme en clair les cinq criteres du bareme, dans l'ordre ou Nintendo les
// empile dans le champ 4 du verdict.
var libelleDesCriteres = []struct {
	Cle, Libelle string
	Reel         bool
}{
	{"Yobisai", "Vote préliminaire", true},
	{"PlayerCount", "Popularité (nombre de joueurs)", false},
	{"Regular", "Matchs ouverts", false},
	{"Challenge", "Matchs défi", false},
	{"Tricolor", "Match tricolore", false},
}

type apiCritereFest struct {
	Critere string `json:"critere"`
	Libelle string `json:"libelle"`
	Place   int    `json:"place"` // 1, 2 ou 3
	Points  int64  `json:"points"`
	Reel    bool   `json:"reel"` // mesure, ou recopie de l'ordre du vote ?
}

type apiCampFest struct {
	Cle       string           `json:"cle"` // Alpha / Bravo / Charlie
	Nom       string           `json:"nom"` // « Perle & Coralie », si le drapeau festcamps le dit
	Votes     int              `json:"votes"`
	Part      float64          `json:"part"` // la part que le jeu affiche, entre 0 et 1
	Rang      int              `json:"rang"`
	Points    int64            `json:"points"`
	Vainqueur bool             `json:"vainqueur"`
	Detail    []apiCritereFest `json:"detail"`
}

type apiVotantFest struct {
	UID     string `json:"uid"`
	PID     uint64 `json:"pid"` // 0 pour un vote pose avant que le PID soit retenu
	Camp    string `json:"camp"`
	Region  string `json:"region"`
	Vote    int64  `json:"vote,omitempty"` // horodatage unix du choix
	EnLigne bool   `json:"enLigne"`
}

type apiFest struct {
	Actif      bool  `json:"actif"`
	Maintenant int64 `json:"maintenant"`

	FestID    string `json:"festId"`
	Ressource string `json:"ressource"`
	Revision  string `json:"revision"`

	Phase        string `json:"phase"`
	PhaseLibelle string `json:"phaseLibelle"`

	Horaires map[string]int64 `json:"horaires"`
	Regions  []string         `json:"regions"`
	Logo     string           `json:"logo"`

	Camps   []apiCampFest   `json:"camps"`
	Votants []apiVotantFest `json:"votants"`

	Bareme map[string]int64 `json:"bareme"`

	// Projection dit que les points affiches ne reposent sur AUCUN vote : tant que personne n'a
	// choisi son camp, le classement sort des parts de depart, qui sont tirees de l'identifiant de
	// la fete et non d'une mesure. Le tableau de bord doit le dire, sans quoi on lirait un
	// vainqueur la ou il n'y a qu'une graine.
	Projection  bool   `json:"projection"`
	Votes       int    `json:"votes"`       // total des inscriptions, tous camps confondus
	ClesServies int    `json:"clesServies"` // 2 pendant la fete, 3 aux resultats
	Vainqueur   string `json:"vainqueur"`   // "" tant que les resultats ne sont pas ouverts
}

// nomsDesCamps lit le drapeau « festcamps », trois noms separes par des barres verticales, dans
// l'ordre Alpha, Bravo, Charlie. Absent, chaque camp garde sa cle : le tableau de bord reste
// lisible, il est seulement moins parlant.
func nomsDesCamps() map[string]string {
	out := map[string]string{}
	morceaux := strings.Split(soirFlagValeur("festcamps"), "|")
	for i, camp := range campsCanoniques {
		if i < len(morceaux) {
			if nom := strings.TrimSpace(morceaux[i]); nom != "" {
				out[camp] = nom
			}
		}
	}

	return out
}

// etatDeLaFete assemble la reponse. Tout vient des fonctions qui servent deja la console.
func etatDeLaFete() apiFest {
	maintenant := time.Now()
	out := apiFest{Maintenant: maintenant.Unix(), Horaires: map[string]int64{}}

	p, ok := phaseDeLaFete(maintenant)
	if !ok {
		return out
	}

	f := festMaison(maintenant)
	out.Actif = true
	out.FestID = lastSeg(f.GetName())
	out.Ressource = f.GetFestGameData()
	out.Revision = f.GetFestGameDataRevision()
	out.Regions = f.GetFestRegions()
	out.Phase, out.PhaseLibelle = p.Cle, p.Libelle
	out.Logo = logoDeFete()
	out.Horaires["annonce"] = p.Annonce.Unix()
	out.Horaires["debut"] = p.Debut.Unix()
	out.Horaires["intermede"] = p.Intermede.Unix()
	out.Horaires["fin"] = p.Fin.Unix()
	out.Horaires["resultats"] = p.Resultats.Unix()

	// Le classement, les parts et les points : exactement ceux du verdict.
	ordre, parts, total := classementsDeLaFete(out.FestID)
	votes := votesParCamp(out.FestID)
	noms := nomsDesCamps()

	// Les MEMES places que celles qui distribuent les points : a egalite, les camps partagent la
	// derniere place de leur groupe (voir placesAvecEgalites). Recalculer l'indice ici aurait fait
	// dire au tableau de bord « rang 2 » pour un camp qui, en points, est traite comme troisieme.
	places := placesAvecEgalites(ordre, parts)
	rangDe := map[string]int{}
	for i, camp := range ordre {
		rangDe[camp] = places[i]
	}

	out.Vainqueur = ""
	if resultatsOuverts() {
		out.Vainqueur = campVainqueurDeLaFete(out.FestID)
		out.ClesServies = 3
	} else {
		out.ClesServies = 2
	}

	// Chaque critere a son propre classement des que de vraies batailles l'alimentent.
	parCritere := map[string]map[string]int{}
	mesure := map[string]bool{}
	for _, cr := range libelleDesCriteres {
		if cr.Cle == "Yobisai" {
			continue
		}
		parCritere[cr.Cle], mesure[cr.Cle] = placesDuCritere(out.FestID, cr.Cle, ordre, parts)
	}

	for _, camp := range ordre {
		place := rangDe[camp]
		c := apiCampFest{
			Cle:       camp,
			Nom:       camp,
			Votes:     votes[camp],
			Part:      parts[camp],
			Rang:      place + 1,
			Points:    total[camp],
			Vainqueur: camp == out.Vainqueur,
		}
		if n, ok := noms[camp]; ok {
			c.Nom = n
		}
		for _, cr := range libelleDesCriteres {
			p, reel := place, cr.Reel
			if m, ok := parCritere[cr.Cle]; ok {
				p, reel = m[camp], mesure[cr.Cle]
			}
			c.Detail = append(c.Detail, apiCritereFest{
				Critere: cr.Cle, Libelle: cr.Libelle,
				Place: p + 1, Points: pointsDuCritere(cr.Cle, p), Reel: reel,
			})
		}
		out.Camps = append(out.Camps, c)
	}

	for _, n := range votes {
		out.Votes += n
	}
	out.Projection = out.Votes == 0

	out.Bareme = poidsDuBareme
	out.Votants = votantsDeLaFete(out.FestID)

	return out
}

// votantsDeLaFete rend la liste nominative des inscrits, du vote le plus recent au plus ancien.
func votantsDeLaFete(festID string) []apiVotantFest {
	prefixe := festID + "/"

	entreesFest.Lock()
	chargerEntreesFestLocked()
	out := make([]apiVotantFest, 0, len(entreesFest.m))
	for cle, e := range entreesFest.m {
		if !strings.HasPrefix(cle, prefixe) {
			continue
		}
		out = append(out, apiVotantFest{
			UID:    strings.TrimPrefix(cle, prefixe),
			PID:    e.Pid,
			Camp:   e.Equipe,
			Region: e.Region,
			Vote:   e.Vote,
		})
	}
	entreesFest.Unlock()

	// La liveness se lit hors du verrou des inscriptions : deux verrous differents, jamais pris
	// ensemble, donc aucun ordre a respecter entre eux.
	for i := range out {
		out[i].EnLigne = nplnUserOnline(out[i].UID)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Vote != out[j].Vote {
			return out[i].Vote > out[j].Vote
		}

		return out[i].UID < out[j].UID
	})

	return out
}

func festAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(etatDeLaFete())
}
