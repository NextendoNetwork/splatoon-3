package main

// Verrou sur l'ALIGNEMENT DE LA FETE SUR LES CRENEAUX DE DEUX HEURES.
//
// Mesure du 2026-08-20. Notre fete servie en cours a bloque ONZE consoles : aucune de celles qui
// l'ont recue n'a pu jouer, tandis que la seule qui ne l'avait pas recue continuait normalement.
//
// La comparaison champ par champ avec la capture Nintendo a montre que la structure etait
// exacte — vingt champs de chaque cote, aucun manquant, aucun en trop. La seule difference qui
// comptait tenait a l'alignement :
//
//	Nintendo  les cinq dates de la table  a 0 modulo 7200
//	nous      les cinq                    a 95 modulo 7200
//
// Splatoon 3 range ses horaires en creneaux de deux heures. Une fete a cheval n'entre dans aucune
// case, et le jeu cessait de la traiter comme ouverte : il reclamait son verdict par GetFestResult,
// ce que la vraie console ne fait jamais.

import (
	"testing"
	"time"
)

func TestLesCinqDatesTombentSurUnCreneau(t *testing.T) {
	// Une heure volontairement batarde : 95 secondes apres une frontiere, comme le drapeau que
	// nous avions pose ce soir-la.
	avecDrapeaux(t, map[string]string{
		"festmaison":    "1787252495",
		"festressource": "juea-ability",
	})

	f := festMaison(time.Now())
	tt := f.GetTimetable()
	dates := map[string]time.Time{
		"open":  tt.GetOpenTime().AsTime(),
		"start": tt.GetStartTime().AsTime(),
		"mid":   tt.GetMidTime().AsTime(),
		"end":   tt.GetEndTime().AsTime(),
		"close": tt.GetCloseTime().AsTime(),
	}
	for nom, d := range dates {
		if r := d.Unix() % int64((2 * time.Hour).Seconds()); r != 0 {
			t.Fatalf("%s = %s : %d s hors creneau (Nintendo les met toutes a 0)", nom, d.Format(time.RFC3339), r)
		}
	}
}

func TestLOrdreDesPhasesTient(t *testing.T) {
	avecDrapeaux(t, map[string]string{"festmaison": "1787252495"})
	tt := festMaison(time.Now()).GetTimetable()

	suite := []struct {
		nom string
		t   time.Time
	}{
		{"open", tt.GetOpenTime().AsTime()},
		{"start", tt.GetStartTime().AsTime()},
		{"mid", tt.GetMidTime().AsTime()},
		{"end", tt.GetEndTime().AsTime()},
		{"close", tt.GetCloseTime().AsTime()},
	}
	for i := 1; i < len(suite); i++ {
		if !suite[i].t.After(suite[i-1].t) {
			t.Fatalf("%s (%s) ne suit pas %s (%s)",
				suite[i].nom, suite[i].t.Format(time.RFC3339),
				suite[i-1].nom, suite[i-1].t.Format(time.RFC3339))
		}
	}
}

func TestLaFeteDuSamediTombeAussiSurUnCreneau(t *testing.T) {
	// Sans horodatage explicite, la fete vise le samedi suivant a minuit — deja aligne, mais on
	// le verrouille pour que l'alignement ne depende pas du chemin emprunte.
	avecDrapeaux(t, map[string]string{"festmaison": "1"})
	d := debutDeLaFeteMaison(time.Unix(1787252495, 0).UTC())
	if d.Weekday() != time.Saturday {
		t.Fatalf("la fete par defaut doit tomber un samedi, pas un %s", d.Weekday())
	}
	if r := d.Unix() % int64((2 * time.Hour).Seconds()); r != 0 {
		t.Fatalf("le samedi choisi est hors creneau de %d s", r)
	}
}
