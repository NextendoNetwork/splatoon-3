package main

import "testing"

// TestPlacesAvecEgalites verrouille la regle des ex aequo.
//
// Elle vient d'un defaut CONSTATE A L'ECRAN le 2026-08-24 : un seul joueur avait vote, pour
// Charlie, et le tableau de bord donnait 290 points a Alpha — c'est-a-dire les points de deuxieme
// place — alors qu'Alpha etait a 0 %, exactement comme Bravo. Le tri departageait les deux par
// ordre alphabetique, ce qui convient pour une liste stable mais pas pour distribuer des points.
func TestPlacesAvecEgalites(t *testing.T) {
	cas := []struct {
		nom    string
		ordre  []string
		parts  map[string]float64
		places []int
	}{
		{
			nom:   "un seul camp soutenu : les deux autres partagent la troisieme place",
			ordre: []string{"Charlie", "Alpha", "Bravo"},
			parts: map[string]float64{"Charlie": 1, "Alpha": 0, "Bravo": 0},
			// 0 = 1re, 1 = 2e, 2 = 3e. Alpha et Bravo prennent la 3e : zero point pour les deux.
			places: []int{0, 2, 2},
		},
		{
			nom:    "trois parts distinctes : chacun sa place, rien ne change",
			ordre:  []string{"Alpha", "Bravo", "Charlie"},
			parts:  map[string]float64{"Alpha": 0.5, "Bravo": 0.3, "Charlie": 0.2},
			places: []int{0, 1, 2},
		},
		{
			nom:    "egalite en tete : personne ne peut revendiquer la premiere place",
			ordre:  []string{"Alpha", "Bravo", "Charlie"},
			parts:  map[string]float64{"Alpha": 0.4, "Bravo": 0.4, "Charlie": 0.2},
			places: []int{1, 1, 2},
		},
		{
			nom:    "les trois a egalite : tous troisiemes, donc zero partout",
			ordre:  []string{"Alpha", "Bravo", "Charlie"},
			parts:  map[string]float64{"Alpha": 0.33, "Bravo": 0.33, "Charlie": 0.33},
			places: []int{2, 2, 2},
		},
	}

	for _, c := range cas {
		got := placesAvecEgalites(c.ordre, c.parts)
		if len(got) != len(c.places) {
			t.Fatalf("%s : %d places rendues, %d attendues", c.nom, len(got), len(c.places))
		}
		for i := range got {
			if got[i] != c.places[i] {
				t.Errorf("%s : %s place %d, attendu %d", c.nom, c.ordre[i], got[i], c.places[i])
			}
		}
	}
}

// TestZeroVoixZeroPoint enonce la consequence qui compte pour un joueur : un camp que personne n'a
// choisi ne rapporte aucun point, sur aucun des cinq criteres.
func TestZeroVoixZeroPoint(t *testing.T) {
	ordre := []string{"Charlie", "Alpha", "Bravo"}
	parts := map[string]float64{"Charlie": 1, "Alpha": 0, "Bravo": 0}
	places := placesAvecEgalites(ordre, parts)

	for _, critere := range []string{"Yobisai", "PlayerCount", "Regular", "Challenge", "Tricolor"} {
		for i, camp := range ordre {
			pts := pointsDuCritere(critere, places[i])
			if camp != "Charlie" && pts != 0 {
				t.Errorf("%s n'a aucune voix mais gagne %d point(s) en %s", camp, pts, critere)
			}
			if camp == "Charlie" && pts == 0 {
				t.Errorf("Charlie a toutes les voix mais ne gagne rien en %s", critere)
			}
		}
	}
}
