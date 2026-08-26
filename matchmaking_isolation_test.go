package main

// Verrou sur l'ISOLATION DES MODES dans l'appariement.
//
// Un mode ne doit jamais recevoir les joueurs d'un autre. Le filtrage se fait sur une egalite
// stricte du dernier segment de la configuration — mais une demande SANS configuration etait
// versee d'office dans « regular_match_config », la file la plus frequentee du serveur.

import "testing"

func TestChaqueModeSaPropreFile(t *testing.T) {
	// Deux configurations differentes ne doivent JAMAIS produire la meme cle de file.
	configs := []string{
		"regular_match_config",
		"bankara_match_challenge_ar_team_config",
		"coop_regular_config",
		"coop_private_config",
		"private_match_config",
		"x_match_config",
		"fest_match_regular_normal_config",
		"fest_match_regular_direct_pair_config",
		"fest_match_challenge_oneshot_config",
	}
	vues := map[string]string{}
	for _, c := range configs {
		k := baseConfigName("tenants/t-dce9377b-lp1/matchmakingConfigs/" + c)
		if autre, deja := vues[k]; deja {
			t.Errorf("%q et %q partagent la file %q : leurs joueurs se melangeraient", c, autre, k)
		}
		vues[k] = c
		if k != c {
			t.Errorf("baseConfigName(%q) = %q : le chemin complet doit rendre le dernier segment", c, k)
		}
	}
}

func TestDemandeSansConfigurationNeContaminePasLaGuerreDeTerritoire(t *testing.T) {
	vide := baseConfigName("")
	if vide == "regular_match_config" {
		t.Fatal("une demande sans configuration retombe dans la file de Guerre de territoire : " +
			"un salon prive ou un quart de Salmon Run s'y ferait apparier avec des inconnus")
	}
	if vide != configSansNom {
		t.Errorf("file des demandes sans configuration = %q, attendu %q", vide, configSansNom)
	}
	// Et elle reste distincte de tous les modes connus.
	for _, c := range []string{"regular_match_config", "coop_regular_config", "private_match_config"} {
		if baseConfigName(c) == vide {
			t.Errorf("la file sans configuration se confond avec %q", c)
		}
	}
}
