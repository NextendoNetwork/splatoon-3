package main

// Verrou sur les TROIS files de festival, relevees sur la capture du Splatfest officiel
// JUEA-00107 (2026-08-22, console reelle contre Nintendo).
//
// Elles etaient toutes reduites a « fest ». Consequence visible : huit joueurs qui rempilent
// ensemble redemandaient une file ordinaire au lieu d'un appariement direct, et le suivi ne les
// voyait plus en recherche.

import "testing"

func TestTroisFilesDeFestival(t *testing.T) {
	cas := []struct{ cfg, cle, nom string }{
		{"fest_match_regular_normal_config", "fest", "Festival (ouvert)"},
		{"fest_match_regular_direct_pair_config", "fest_equipe", "Festival (meme equipe)"},
		{"fest_match_challenge_oneshot_config", "fest_defi", "Festival (defi)"},
	}
	for _, c := range cas {
		if got := cleDuMode(c.cfg); got != c.cle {
			t.Errorf("cleDuMode(%q) = %q, attendu %q", c.cfg, got, c.cle)
		}
		if got := nomLisibleDuMode(c.cfg); got != c.nom {
			t.Errorf("nomLisibleDuMode(%q) = %q, attendu %q", c.cfg, got, c.nom)
		}
	}
	// Les trois doivent rester DISTINCTES : c'est tout l'interet.
	if cleDuMode(cas[0].cfg) == cleDuMode(cas[1].cfg) || cleDuMode(cas[1].cfg) == cleDuMode(cas[2].cfg) {
		t.Error("deux files de festival partagent la meme cle : les equipes se melangeront")
	}
	// Et le chemin complet, tel que la capture le nomme.
	if cleDuMode("tenants/t-dce9377b-lp1/matchmakingConfigs/fest_match_challenge_oneshot_config") != "fest_defi" {
		t.Error("le chemin complet n'est pas reconnu — c'est sous cette forme que la capture l'envoie")
	}
}
