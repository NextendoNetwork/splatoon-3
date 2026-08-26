package main

// Verrou sur la FERMETURE DES SALONS MORTS.
//
// Mesure du 2026-08-20, serveur de test ouvert : un salon de Guerre de territoire forme a huit
// joueurs est tombe a un pendant que les sept autres repartaient en file. Il figurait toujours
// comme « actif », sur le monitoring comme sur la page publique, et le joueur restant y etait
// epingle — donc compte « en lobby » alors qu'il ne jouait pas.
//
// Ce n'etait pas un defaut d'affichage. formMatchLocked cree TOUJOURS une session neuve et
// n'ajoute jamais personne a une session existante : un salon apparie ne peut donc que se vider,
// et passe sous son effectif de formation il est mort de facon prouvable.
//
// Le risque du remede est l'inverse : fermer une partie encore en cours. Les deux tests ci-dessous
// tiennent les deux bords.

import (
	"testing"
	"time"
)

// salonForme installe un salon apparie tel que le matchmaker l'aurait cree, vieilli de `age`.
func salonForme(t *testing.T, nom string, requis int, membres []string, age time.Duration) {
	t.Helper()
	for i, uid := range membres {
		poserJoueur(uid, uint64(1800000000+i), "regular", nom)
	}
	dashNoteRoomMode(nom, "Guerre de territoire", "regular", requis, 8, membres[0], 1800000000, membres)

	dash.mu.Lock()
	defer dash.mu.Unlock()
	dash.rooms[nom].created = time.Now().Add(-age)
}

// quitte detache un joueur du salon, comme le fait dashQuitterSalons quand il repart en file.
func quitte(uid string) {
	dash.mu.Lock()
	defer dash.mu.Unlock()
	if p := dash.players[uid]; p != nil {
		p.room = ""
	}
}

func TestSalonTombeSousSonEffectifFinitParFermer(t *testing.T) {
	avecMonitoringVide(t)

	membres := []string{"u-a", "u-b", "u-c", "u-d", "u-e", "u-f", "u-g", "u-h"}
	salonForme(t, "sessions/turf", 8, membres, 5*time.Minute)

	// Sept joueurs repartent en file : il n'en reste qu'un sur un salon forme a huit.
	for _, uid := range membres[1:] {
		quitte(uid)
	}

	// Premier passage : le salon vient seulement de passer sous l'effectif, on ne le ferme pas.
	if s := dashSnapshot(); s.ActiveLobbies != 1 {
		t.Fatalf("le salon ne doit pas fermer des le premier passage (%d salon(s))", s.ActiveLobbies)
	}

	// On le vieillit au-dela de la fenetre de tolerance.
	dash.mu.Lock()
	dash.rooms["sessions/turf"].faible = time.Now().Add(-dashSousEffectifTTL - time.Second)
	dash.mu.Unlock()

	s := dashSnapshot()
	if s.ActiveLobbies != 0 {
		t.Fatalf("un salon a 1/8 depuis plus de %s doit fermer, %d encore actif(s)",
			dashSousEffectifTTL, s.ActiveLobbies)
	}
	if s.InLobby != 0 {
		t.Fatalf("le joueur restant doit etre rendu a la file, inLobby=%d", s.InLobby)
	}

	// Et il ne doit pas ressusciter au passage suivant.
	if s := dashSnapshot(); s.ActiveLobbies != 0 {
		t.Fatalf("le salon est revenu apres fermeture (%d)", s.ActiveLobbies)
	}
}

func TestPartieAuCompletNeFermeJamais(t *testing.T) {
	avecMonitoringVide(t)

	membres := []string{"u-a", "u-b", "u-c", "u-d", "u-e", "u-f", "u-g", "u-h"}
	salonForme(t, "sessions/turf", 8, membres, 10*time.Minute)

	// Une partie longue : le salon reste au complet, quel que soit le nombre de passages.
	for i := 0; i < 4; i++ {
		if s := dashSnapshot(); s.ActiveLobbies != 1 || s.InLobby != 8 {
			t.Fatalf("passage %d : une partie a 8/8 doit rester ouverte (lobbies=%d inLobby=%d)",
				i, s.ActiveLobbies, s.InLobby)
		}
	}
}

func TestSalonDHoteResteOuvertMemeSeul(t *testing.T) {
	avecMonitoringVide(t)

	// Un salon prive n'a pas d'effectif de formation : il attend ses invites, parfois longtemps,
	// et personne ne doit le fermer sous pretexte qu'il est a un contre huit.
	poserJoueur("u-hote", 1800000001, "private", "sessions/prive")
	dashNoteRoom("sessions/prive", "Salon", 8, "u-hote", 1800000001, []string{"u-hote"})

	dash.mu.Lock()
	dash.rooms["sessions/prive"].created = time.Now().Add(-30 * time.Minute)
	dash.rooms["sessions/prive"].faible = time.Now().Add(-30 * time.Minute)
	dash.mu.Unlock()

	if s := dashSnapshot(); s.ActiveLobbies != 1 {
		t.Fatalf("un salon d'hote seul doit rester ouvert, %d actif(s)", s.ActiveLobbies)
	}
}

func TestEffectifRetrouveRemetLeCompteurAZero(t *testing.T) {
	avecMonitoringVide(t)

	membres := []string{"u-a", "u-b", "u-c", "u-d"}
	salonForme(t, "sessions/coop", 4, membres, 5*time.Minute)

	quitte("u-d")
	dashSnapshot() // le salon passe sous l'effectif : le compteur demarre

	dash.mu.Lock()
	demarre := dash.rooms["sessions/coop"].faible
	dash.mu.Unlock()
	if demarre.IsZero() {
		t.Fatalf("le compteur de sous-effectif aurait du demarrer")
	}

	// Le joueur revient : le salon est de nouveau complet, le compteur doit repartir de zero.
	dash.mu.Lock()
	dash.players["u-d"].room = "sessions/coop"
	dash.players["u-d"].lastSeen = time.Now()
	dash.mu.Unlock()
	dashSnapshot()

	dash.mu.Lock()
	remis := dash.rooms["sessions/coop"] != nil && dash.rooms["sessions/coop"].faible.IsZero()
	dash.mu.Unlock()
	if !remis {
		t.Fatalf("l'effectif retrouve doit remettre le compteur a zero")
	}
}
