package main

// Verrou sur le decoupage d'un chemin de fete.
//
// MESURE DU 2026-08-23. Fete maison en cours : 144 appels a GetFestResult, et 144 fois
// « nom non decoupe -> objet vide ». Le decoupage ne connaissait que le segment « fests », celui
// des inscriptions, alors que le verdict vit sous « festResults ». Le jeu recevait donc un verdict
// VIDE pour une fete qui court — exactement le mur du 2026-08-20, ou onze consoles avaient recu
// notre fete sans pouvoir jouer pendant que la seule qui ne l'avait pas reçue continuait.
//
// La comparaison champ par champ avec la capture Nintendo, faite le meme jour, avait innocente le
// calendrier : vingt champs de chaque cote, aucun manquant. Le defaut etait ici.

import "testing"

func TestIdentifiantLuDansLesTroisCollections(t *testing.T) {
	cas := []struct {
		chemin  string
		attendu string
		quoi    string
	}{
		{"tenants/current/fests/JUEA-00015/entries/u-abc", "JUEA-00015", "inscription"},
		{"tenants/current/festResults/JUEA-00015", "JUEA-00015", "verdict — celui qui echouait"},
		{"tenants/current/festSchedules/JUEA-00015", "JUEA-00015", "calendrier"},
		{"tenants/t-dce9377b-lp1/festResults/JUEA-00201", "JUEA-00201", "verdict, locataire en clair"},
	}
	for _, c := range cas {
		if got := festIDDuChemin(c.chemin); got != c.attendu {
			t.Errorf("%s : %q -> %q, attendu %q", c.quoi, c.chemin, got, c.attendu)
		}
	}
}

func TestCheminSansIdentifiantRendVide(t *testing.T) {
	for _, chemin := range []string{
		"",
		"tenants/current",
		"tenants/current/festResults", // la collection sans son membre
		"tenants/current/users/u-abc",
	} {
		if got := festIDDuChemin(chemin); got != "" {
			t.Errorf("%q -> %q, attendu vide", chemin, got)
		}
	}
}
