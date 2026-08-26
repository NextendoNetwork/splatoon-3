package main

// Verrou sur l'intermede d'un Splatfest.
//
// Mesure du 2026-08-23, capture d'un festival officiel, tricolore ouvert : GetFestResult porte DEUX
// blocs de parts. Le vote preliminaire, inchange depuis la veille, et l'intermede :
//
//	Bravo 0,334710   Charlie 0,334110   Alpha 0,331210
//
// Bravo en tete — et Bravo est exactement le camp qui defend dans les CINQ batailles tricolores de
// la capture. Les camps de l'intermede sont ranges par part DECROISSANTE, contrairement au vote
// preliminaire qui suit l'ordre canonique.

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

func intermedeIsole(t *testing.T) {
	t.Helper()
	t.Setenv("NPLN_SAVE_DIR", filepath.Join(t.TempDir(), "saves"))
	victoiresFest.Lock()
	victoiresFest.m = map[string]*comptageFest{}
	victoiresFest.chargé = true // on ne lit pas le disque de production
	victoiresFest.Unlock()
}

func TestClassementRangeParPartDecroissante(t *testing.T) {
	// Les valeurs exactes de la capture.
	parts := map[string]float64{"Alpha": 0.331210, "Bravo": 0.334710, "Charlie": 0.334110}
	ordre := classement(parts)
	attendu := []string{"Bravo", "Charlie", "Alpha"}
	for i := range attendu {
		if ordre[i] != attendu[i] {
			t.Errorf("rang %d : %s, attendu %s — Nintendo publie le vainqueur en tete", i+1, ordre[i], attendu[i])
		}
	}
	if d := campDefenseurDe(parts); d != "Bravo" {
		t.Errorf("defenseur = %s, attendu Bravo", d)
	}
}

// campDefenseurDe : le vainqueur d'un partage donne, sans passer par le disque.
func campDefenseurDe(parts map[string]float64) string { return classement(parts)[0] }

func TestIntermedeAbsentAvantLaMiParcours(t *testing.T) {
	intermedeIsole(t)
	demain := time.Now().Add(2 * time.Hour)
	if honsaiMidtermDeLaFete("JUEA-00201", demain) != nil {
		t.Error("intermede publie avant la mi-parcours : la capture du 2026-08-22 le montre ABSENT")
	}
}

func TestIntermedeSuitLesVictoires(t *testing.T) {
	intermedeIsole(t)
	for i := 0; i < 5; i++ {
		enregistrerVictoireDeFete("JUEA-00201", "Bravo", "")
	}
	for i := 0; i < 3; i++ {
		enregistrerVictoireDeFete("JUEA-00201", "Charlie", "")
	}
	enregistrerVictoireDeFete("JUEA-00201", "Alpha", "")

	m := honsaiMidtermDeLaFete("JUEA-00201", time.Now().Add(-time.Hour))
	if m == nil {
		t.Fatal("intermede absent apres la mi-parcours")
	}
	if !m.GetIsValid() {
		t.Error("is_valid doit etre vrai")
	}
	attendu := map[string]float64{"Bravo": 5.0 / 9, "Charlie": 3.0 / 9, "Alpha": 1.0 / 9}
	var somme float64
	for _, r := range m.GetTeamRatios() {
		somme += r.GetRatio()
		if math.Abs(r.GetRatio()-attendu[r.GetFestTeam()]) > 1e-9 {
			t.Errorf("%s : part %.6f, attendu %.6f", r.GetFestTeam(), r.GetRatio(), attendu[r.GetFestTeam()])
		}
	}
	if math.Abs(somme-1.0) > 1e-9 {
		t.Errorf("somme = %.6f, attendu 1", somme)
	}
	if r := m.GetTeamRatios()[0]; r.GetFestTeam() != "Bravo" {
		t.Errorf("tete de classement = %s, attendu Bravo", r.GetFestTeam())
	}
	if d := campDefenseur("JUEA-00201"); d != "Bravo" {
		t.Errorf("defenseur = %q, attendu Bravo", d)
	}
}

func TestIntermedeEstFigeUneFoisPris(t *testing.T) {
	intermedeIsole(t)
	enregistrerVictoireDeFete("JUEA-00201", "Alpha", "")

	passe := time.Now().Add(-time.Hour)
	premier := honsaiMidtermDeLaFete("JUEA-00201", passe)
	if premier.GetTeamRatios()[0].GetFestTeam() != "Alpha" {
		t.Fatal("Alpha devait mener")
	}

	// Bravo gagne ensuite dix batailles : l'intermede NE DOIT PAS bouger, il designe le defenseur
	// pour toute la seconde moitie du festival.
	for i := 0; i < 10; i++ {
		enregistrerVictoireDeFete("JUEA-00201", "Bravo", "")
	}
	apres := honsaiMidtermDeLaFete("JUEA-00201", passe)
	if apres.GetTeamRatios()[0].GetFestTeam() != "Alpha" {
		t.Errorf("l intermede a bouge : %s en tete alors qu il etait fige sur Alpha",
			apres.GetTeamRatios()[0].GetFestTeam())
	}
	if campDefenseur("JUEA-00201") != "Alpha" {
		t.Error("le defenseur a change en cours de seconde mi-temps")
	}
}

func TestLaFerveurPrimeSurLesVictoires(t *testing.T) {
	intermedeIsole(t)

	// Alpha gagne toutes les batailles, mais Bravo encre bien davantage. C'est la ferveur qui
	// doit trancher : notre regle recompense de JOUER, pas seulement de gagner.
	for i := 0; i < 4; i++ {
		enregistrerVictoireDeFete("JUEA-00201", "Alpha", "")
	}
	enregistrerFerveur("JUEA-00201", "Alpha", 1000)
	enregistrerFerveur("JUEA-00201", "Bravo", 3000)

	m := honsaiMidtermDeLaFete("JUEA-00201", time.Now().Add(-time.Hour))
	if m == nil {
		t.Fatal("intermede absent")
	}
	if tete := m.GetTeamRatios()[0].GetFestTeam(); tete != "Bravo" {
		t.Errorf("tete = %s, attendu Bravo — la ferveur doit primer sur les victoires", tete)
	}
	for _, r := range m.GetTeamRatios() {
		if r.GetFestTeam() == "Bravo" && math.Abs(r.GetRatio()-0.75) > 1e-9 {
			t.Errorf("Bravo : part %.6f, attendu 0,75 (3000 sur 4000)", r.GetRatio())
		}
	}
}

func TestFerveurIgnoreLesJoueursSansCamp(t *testing.T) {
	intermedeIsole(t)
	enregistrerFerveur("JUEA-00201", "", 5000) // joueur sans inscription connue
	enregistrerFerveur("JUEA-00201", "Alpha", 1000)

	parts := partsDeFerveur("JUEA-00201")
	if parts == nil {
		t.Fatal("aucune ferveur comptee")
	}
	if math.Abs(parts["Alpha"]-1.0) > 1e-9 {
		t.Errorf("Alpha : %.6f, attendu 1 — l encre d un joueur sans camp ne doit compter pour personne", parts["Alpha"])
	}
}
