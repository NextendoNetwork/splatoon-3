package main

import (
	"testing"
	"time"
)

// TestLogoSAllumeEtSEteintToutSeul verrouille la bascule de l'ecran-titre.
//
// POURQUOI UN TEST ET PAS UN ESSAI. Le logo doit apparaitre a l'heure de debut du festival et
// disparaitre a sa fin. On n'a qu'une seule occasion de le constater le jour dit, et un logo qui ne
// s'affiche pas ne laisse aucune trace : il se voit a l'ecran, jamais dans un journal. Le test
// interroge donc chaque borne a l'avance.
//
// Les valeurs sont celles du LoveFest reellement servi : debut le 25/08/2026 a 12h00 de Paris
// (10h00 UTC), intermede trois jours plus tard, fin le 01/09 a la meme heure, resultats deux heures
// apres. Elles sont toutes multiples de 7200, la taille du creneau sur lequel le jeu tronque.
func TestLogoSAllumeEtSEteintToutSeul(t *testing.T) {
	const debutUnix = 1787652000 // 25/08/2026 10:00 UTC = 12:00 Paris

	avecDrapeaux(t, map[string]string{
		"festmaison":  "1787652000",
		"festmi":      "259200", // +3 j  -> 28/08 12:00 Paris
		"festfin":     "604800", // +7 j  -> 01/09 12:00 Paris
		"festcloture": "612000", // +7 j 2 h -> 01/09 14:00 Paris
		"festid":      "JUEA-00210",
		"festlogo":    "lovefest",
	})

	debut := time.Unix(debutUnix, 0).UTC()

	cas := []struct {
		quand  time.Time
		veut   string
		raison string
	}{
		{debut.Add(-24 * time.Hour), "", "la veille : la fete est annoncee mais n'a pas commence"},
		{debut.Add(-time.Second), "", "une seconde avant le debut"},
		{debut, "lovefest", "a l'heure pile du debut"},
		{debut.Add(12 * time.Hour), "lovefest", "en premiere mi-temps"},
		{debut.Add(3 * 24 * time.Hour), "lovefest", "a l'intermede"},
		{debut.Add(5 * 24 * time.Hour), "lovefest", "en seconde mi-temps"},
		{debut.Add(7*24*time.Hour - time.Second), "lovefest", "une seconde avant la fin"},
		{debut.Add(7 * 24 * time.Hour), "", "a l'heure pile de la fin : l'ecran-titre redevient normal"},
		{debut.Add(7*24*time.Hour + 2*time.Hour), "", "a l'ouverture des resultats"},
		{debut.Add(30 * 24 * time.Hour), "", "un mois plus tard, la fete est oubliee"},
	}

	for _, c := range cas {
		got := logoDeFeteA(c.quand)
		if got != c.veut {
			t.Errorf("%s (%s) : logo %q, attendu %q",
				c.raison, c.quand.Format("02/01 15:04 MST"), got, c.veut)
		}
	}
}

// TestLogoInconnuNeTouchePasALEcran : un nom que l'emulateur ne sait pas traiter ne doit RIEN
// changer. Le defaut est de laisser l'ecran-titre du joueur tranquille.
func TestLogoInconnuNeTouchePasALEcran(t *testing.T) {
	avecDrapeaux(t, map[string]string{
		"festmaison":  "1787652000",
		"festmi":      "259200",
		"festfin":     "604800",
		"festcloture": "612000",
		"festid":      "JUEA-00210",
		"festlogo":    "une-valeur-qui-n-existe-pas",
	})

	pendant := time.Unix(1787652000, 0).UTC().Add(12 * time.Hour)
	if got := logoDeFeteA(pendant); got != "" {
		t.Errorf("un nom de logo inconnu rend %q au lieu de ne rien changer", got)
	}
}
