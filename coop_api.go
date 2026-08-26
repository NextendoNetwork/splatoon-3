package main

// coop_api — les quarts de travail du Salmon Run, exposes pour le site.
//
// Contrairement au versus, la rotation coop n'est pas engendree : elle rejoue la capture Nintendo
// recalee vers le present, exactement comme SelectCoopSchedules la sert au jeu. On passe donc par
// le MEME chemin et le MEME decalage — le site ne peut pas annoncer autre chose que ce que les
// joueurs auront.
//
// Un quart porte plus qu'un stage : le boss, les quatre armes tirees, l'arme Grizzco, et la
// recompense. C'est ce qui rend la page interessante, donc on rend tout.

import (
	"encoding/json"
	"net/http"
	"time"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

type quartCoop struct {
	Debut          string  `json:"start"`
	Fin            string  `json:"end"`
	Type           string  `json:"kind"`
	Stage          int32   `json:"stage"`
	Boss           string  `json:"boss,omitempty"`
	Armes          []int64 `json:"weapons,omitempty"`
	ArmeGrizzco    int32   `json:"grizzcoWeapon,omitempty"`
	Recompense     string  `json:"rewardType,omitempty"`
	GearRecompense int32   `json:"rewardGearId,omitempty"`
	ShiftID        string  `json:"shiftId,omitempty"`
}

// coopHandler : GET /api/coop
// Rend tous les quarts connus, du plus ancien au plus recent, apres recalage.
func coopHandler(w http.ResponseWriter, r *http.Request) {
	rep := &toyohrpb.SelectCoopSchedulesResponse{}
	serveShifted("api/coop", rawCoopSchedules, rep, scheduleDelta())

	maintenant := time.Now().UTC()
	var out []quartCoop
	var courant *quartCoop

	for _, s := range rep.GetSchedules() {
		q := quartCoop{
			Debut:   s.GetStartTime().AsTime().Format(time.RFC3339),
			Fin:     s.GetEndTime().AsTime().Format(time.RFC3339),
			Type:    "normal",
			ShiftID: s.GetShiftId(),
		}

		// Les trois formes d'un quart. On lit celle qui est renseignee : un Big Run et un
		// concours d'equipe ne decrivent pas la meme chose qu'un quart ordinaire.
		switch {
		case s.GetBigRun() != nil:
			b := s.GetBigRun()
			q.Type = "bigrun"
			q.Stage = b.GetStage()
			q.Boss = b.GetBoss()
			q.Armes = b.GetMainWeapons()
			q.ArmeGrizzco = b.GetKumaWeapon()
			q.Recompense = b.GetRewardType()
			q.GearRecompense = b.GetRewardGearId()
		case s.GetTeamContest() != nil:
			c := s.GetTeamContest()
			// Un concours d'equipe n'annonce PAS de boss : il n'a pas ce champ. On ne
			// remplit donc que ce qu'il porte reellement.
			q.Type = "teamcontest"
			q.Stage = c.GetStage()
			q.Armes = c.GetMainWeapons()
			q.ArmeGrizzco = c.GetKumaWeapon()
		case s.GetNormal() != nil:
			n := s.GetNormal()
			q.Stage = n.GetStage()
			q.Boss = n.GetBoss()
			q.Armes = n.GetMainWeapons()
			q.ArmeGrizzco = n.GetKumaWeapon()
			q.Recompense = n.GetRewardType()
			q.GearRecompense = n.GetRewardGearId()
		}

		out = append(out, q)
		if courant == nil {
			deb := s.GetStartTime().AsTime()
			fin := s.GetEndTime().AsTime()
			if !deb.After(maintenant) && fin.After(maintenant) {
				c := q
				courant = &c
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "public, max-age=300")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"now":     maintenant.Format(time.RFC3339),
		"current": courant,
		"shifts":  out,
	})
}
