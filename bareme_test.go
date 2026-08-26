package main

import "testing"

// Le bareme doit reproduire les totaux du festival officiel JUEA-00107, releves dans la capture
// du 2026-08-24 : Alpha 150, Bravo 265, Charlie 455.
func TestBaremeReproduitLesTotauxDeNintendo(t *testing.T) {
	// Classements captures, dans l'ordre 1er, 2e, 3e.
	yobisai := []string{"Alpha", "Bravo", "Charlie"}
	criteres := map[string][]string{
		"PlayerCount": {"Bravo", "Charlie", "Alpha"},
		"Regular":     {"Charlie", "Bravo", "Alpha"},
		"Challenge":   {"Charlie", "Alpha", "Bravo"},
		"Tricolor":    {"Charlie", "Bravo", "Alpha"},
	}

	total := map[string]int64{}
	for place, camp := range yobisai {
		total[camp] += pointsDuCritere("Yobisai", place)
	}
	for critere, ordre := range criteres {
		for place, camp := range ordre {
			total[camp] += pointsDuCritere(critere, place)
		}
	}

	for camp, attendu := range map[string]int64{"Alpha": 150, "Bravo": 265, "Charlie": 455} {
		if total[camp] != attendu {
			t.Errorf("%s : %d points calcules, %d attendus par la capture", camp, total[camp], attendu)
		}
	}
	if classementParPoints(total)[0] != "Charlie" {
		t.Errorf("vainqueur attendu Charlie, obtenu %s", classementParPoints(total)[0])
	}
}
