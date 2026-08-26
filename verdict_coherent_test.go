package main

import "testing"

// Le vainqueur annonce, la cle servie et les points affiches doivent sortir du MEME calcul.
// Le 2026-08-24 ils divergeaient : l'ecran donnait la victoire a un camp, les idoles en
// annoncaient un autre.
func TestVainqueurEtPointsConcordent(t *testing.T) {
	const fest = "JUEA-TEST1"
	_, _, total := classementsDeLaFete(fest)
	if len(total) == 0 {
		t.Skip("aucun camp connu pour cette fete de test")
	}
	attendu := classementParPoints(total)[0]
	if got := campVainqueurDeLaFete(fest); got != attendu {
		t.Errorf("vainqueur annonce %q, mais les points designent %q", got, attendu)
	}
}

// Le prestival doit suivre les VOTES, pas l'ordre canonique des camps.
func TestPrestivalSuitLesVotes(t *testing.T) {
	// Un seul votant, pour Charlie : il doit prendre les 90 points de la premiere place.
	total := map[string]int64{}
	ordre := classementParPoints(map[string]int64{"Alpha": 0, "Bravo": 0, "Charlie": 1})
	for place, camp := range ordre {
		total[camp] += pointsDuCritere("Yobisai", place)
	}
	if total["Charlie"] != 90 {
		t.Errorf("Charlie a 100%% des conques mais %d point(s) au lieu de 90", total["Charlie"])
	}
	if total["Alpha"] == 90 {
		t.Error("Alpha encaisse la premiere place avec zero vote")
	}
}
