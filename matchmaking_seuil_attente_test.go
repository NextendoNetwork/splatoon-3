package main

// Verrou sur l'assouplissement du seuil de formation.
//
// MESURE DU 2026-08-22, 18:15 -> 20:05 : 67 billets de recherche, 20 annulations, ZERO partie.
// La file de regular_match_config a plafonne a 5 joueurs distincts sur les 8 exiges — jamais 6,
// jamais 7, jamais 8. Le seuil fixe a huit rendait la formation IMPOSSIBLE pendant deux heures.

import (
	"testing"
	"time"

	mmpb "npln.nintendo.net/npln-practice/proto/matchmaking/v1"
)

func TestSeuilResteNominalPendantTouteLAttente(t *testing.T) {
	for _, age := range []time.Duration{0, 45 * time.Second, 2 * time.Minute, 10 * time.Minute} {
		m := fileDeTest("regular_match_config", 3, age)
		if got, _ := m.seuilCourantLocked("regular_match_config", 8); got != 8 {
			t.Errorf("after %s, regular threshold = %d, want 8", age, got)
		}
	}
}

func TestSeuilConserveCapaciteNominaleParMode(t *testing.T) {
	for _, tc := range []struct {
		config  string
		nominal int32
	}{
		{"regular_match_config", 8},
		{"coop_regular_config", 4},
		{"two_player_config", 2},
	} {
		m := fileDeTest(tc.config, 1, 10*time.Minute)
		if got, _ := m.seuilCourantLocked(tc.config, tc.nominal); got != tc.nominal {
			t.Errorf("%s threshold = %d, want nominal %d", tc.config, got, tc.nominal)
		}
	}
}

// fileDeTest garnit la file d'un mode avec n joueurs distincts, tous en attente depuis `depuis`.
func fileDeTest(cfg string, n int, depuis time.Duration) *matchmakerServer {
	m := newMatchmaker()
	t0 := time.Now().Add(-depuis)
	for i := 0; i < n; i++ {
		m.waiting = append(m.waiting, &mmWaiter{
			ticket: &mmpb.MatchmakingTicket{MatchmakingConfig: cfg},
			uid:    "u-test" + string(rune(int(97)+i)),
			depuis: t0,
		})
	}
	return m
}

func TestAttenteLaPlusLongueEstCelleDuPlusAncien(t *testing.T) {
	m := fileDeTest("regular_match_config", 2, 30*time.Second)
	// Un troisieme joueur arrive a l instant : il ne doit PAS raccourcir l attente mesuree.
	m.waiting = append(m.waiting, &mmWaiter{
		ticket: &mmpb.MatchmakingTicket{MatchmakingConfig: "regular_match_config"},
		uid:    "u-testz",
		depuis: time.Now(),
	})
	a := m.attenteLaPlusLongueLocked("regular_match_config")
	if a < 29*time.Second {
		t.Errorf("attente mesuree %s : on doit compter celle du joueur qui patiente DEPUIS LE PLUS LONGTEMPS", a)
	}
}

func TestLeSeuilNeDescendJamaisSousCeQueLeJeuExige(t *testing.T) {
	// ⚠️ REGLE DURE. Une Guerre de territoire se joue a HUIT. Le 2026-08-25, avoir laisse le seuil
	// se detendre a produit des salons de quatre et de six, incapables de lancer la partie. Peu
	// importe depuis combien de temps on patiente, et peu importe les drapeaux : le seuil rendu est
	// le seuil NOMINAL.
	m := fileDeTest("regular_match_config", 3, 2*time.Minute)

	sansAucunFichierDeDrapeaux(t)
	if s, _ := m.seuilCourantLocked("regular_match_config", 8); s != 8 {
		t.Errorf("sans drapeau, apres 2 min d attente : seuil %d, attendu 8", s)
	}

	cheminFlags = ecrireDrapeaux(t, "mmstrict"+string(rune(10)))
	invaliderCacheFlags()
	if s, _ := m.seuilCourantLocked("regular_match_config", 8); s != 8 {
		t.Errorf("avec mmstrict : seuil %d, attendu 8", s)
	}
}

func TestUneLongueAttenteNeRetrecitAucuneFile(t *testing.T) {
	// Meme un Salmon Run qui patiente depuis trois minutes garde sa capacite pleine, et l attente
	// d une file ne deteint pas sur une autre.
	m := fileDeTest("coop_regular_config", 2, 3*time.Minute)
	m.waiting = append(m.waiting, &mmWaiter{
		ticket: &mmpb.MatchmakingTicket{MatchmakingConfig: "regular_match_config"},
		uid:    "u-testfrais",
		depuis: time.Now(),
	})
	sansAucunFichierDeDrapeaux(t)

	if s, _ := m.seuilCourantLocked("coop_regular_config", 4); s != 4 {
		t.Errorf("coop apres 3 min : seuil %d, attendu 4", s)
	}
	if s, _ := m.seuilCourantLocked("regular_match_config", 8); s != 8 {
		t.Errorf("turf : seuil %d, attendu 8", s)
	}
}
