package main

// rotation_api — la rotation des stages, exposee pour le site.
//
// La rotation est une fonction PURE du numero de creneau (voir rotation_generee.go) : on peut donc
// rendre n'importe quelle plage de dates, passee comme future, sans base de donnees ni etat. C'est
// ce qui permet au site d'annoncer les stages a l'avance et d'afficher l'historique, en etant sur
// que ce sera exactement ce que les joueurs ont eu ou auront.

import (
	"encoding/json"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// plafondCreneaux borne la reponse : un mois de rotation suffit largement a un site, et evite
// qu'une plage absurde fasse produire des megaoctets.
const plafondCreneaux = 372 // 31 jours

type modeRotation struct {
	Mode   string  `json:"mode"`
	Regle  int32   `json:"rule,omitempty"`
	Stages []int32 `json:"stages"`
}

type creneauRotation struct {
	Debut string         `json:"start"`
	Fin   string         `json:"end"`
	Modes []modeRotation `json:"modes"`
}

// rotationHandler : GET /api/rotation?from=<RFC3339>&to=<RFC3339>
// Sans parametre, rend les prochaines 24 h.
func rotationHandler(w http.ResponseWriter, r *http.Request) {
	lire := func(nom string, defaut time.Time) time.Time {
		v := r.URL.Query().Get(nom)
		if v == "" {
			return defaut
		}
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return defaut
		}
		return t.UTC()
	}

	debut := lire("from", time.Now().UTC())
	fin := lire("to", debut.Add(24*time.Hour))

	if !fin.After(debut) {
		http.Error(w, "to doit etre apres from", http.StatusBadRequest)
		return
	}

	modele := &toyohrpb.SelectVsSchedulesResponse{}
	if proto.Unmarshal(rawVsSchedules, modele) != nil {
		http.Error(w, "gabarit illisible", http.StatusInternalServerError)
		return
	}

	var out []creneauRotation
	for c := creneauDe(debut); c <= creneauDe(fin) && len(out) < plafondCreneaux; c++ {
		t := time.Unix(c*int64(rotationPeriod/time.Second), 0).UTC()

		// On demande la rotation POUR cet instant et on lit la phase qui le couvre : meme chemin
		// que celui servi au jeu, donc aucune divergence possible entre le site et la partie.
		var phase *toyohrpb.VsSchedule
		for _, p := range rotationEngendree(t.Add(time.Minute), modele).GetSchedules() {
			if !p.GetStartTime().AsTime().After(t) && p.GetEndTime().AsTime().After(t) {
				phase = p
				break
			}
		}
		if phase == nil {
			continue
		}

		cr := creneauRotation{
			Debut: phase.GetStartTime().AsTime().Format(time.RFC3339),
			Fin:   phase.GetEndTime().AsTime().Format(time.RFC3339),
			Modes: []modeRotation{
				{Mode: "regular", Stages: phase.GetRegularSettings().GetStages()},
			},
		}
		for i, b := range phase.GetBankaraSettings() {
			nom := "bankara_challenge"
			if i == 1 {
				nom = "bankara_open"
			}
			cr.Modes = append(cr.Modes, modeRotation{Mode: nom, Regle: b.GetRule(), Stages: b.GetStages()})
		}
		cr.Modes = append(cr.Modes,
			modeRotation{Mode: "x", Regle: phase.GetXSettings().GetRule(), Stages: phase.GetXSettings().GetStages()},
			modeRotation{Mode: "league", Regle: phase.GetLeagueSettings().GetRule(), Stages: phase.GetLeagueSettings().GetStages()},
		)

		out = append(out, cr)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"rotationPeriodMinutes": int(rotationPeriod / time.Minute),
		"slots":                 out,
	})
}
