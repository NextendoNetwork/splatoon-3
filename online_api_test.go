package main

// Verrou sur la VUE PUBLIQUE des salons.
//
// /api/stats porte des adresses IP, des villes, des fournisseurs d'acces et des identifiants de
// compte, et c'est pour cela qu'un jeton le garde. /api/online, lui, n'en a pas : il est ouvert.
// Ces deux tests fixent donc ce qui a le droit d'en sortir, et ce qui n'en sort jamais.
//
// Deux exigences se croisent ici :
//   1. un salon prive est compte, jamais detaille — sinon la page publique annulerait la seule
//      chose qu'un salon prive promet ;
//   2. aucun champ nominatif ni reseau ne traverse, meme si le monitoring en connait.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// avecMonitoringVide remet l'etat global a neuf : les tests du paquet partagent `dash`.
func avecMonitoringVide(t *testing.T) {
	t.Helper()
	dash.mu.Lock()
	defer dash.mu.Unlock()
	dash.rooms = map[string]*dashRoom{}
	dash.players = map[string]*dashSeen{}
	dash.events = nil
}

// poserJoueur inscrit un joueur vu a l'instant, avec le mode et le salon demandes.
func poserJoueur(uid string, pid uint64, modeKey, salon string) {
	dash.mu.Lock()
	defer dash.mu.Unlock()
	dash.players[uid] = &dashSeen{
		pid: pid, uid: uid, first: time.Now(), lastSeen: time.Now(),
		mode: "quelque chose", modeKey: modeKey, room: salon,
		ip: "203.0.113.7",
	}
}

func lireOnline(t *testing.T) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	onlineHandler(rec, httptest.NewRequest(http.MethodGet, "/api/online", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("code %d", rec.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("json illisible : %v", err)
	}
	return out
}

func TestOnlineNeDetaillePasLesSalonsPrives(t *testing.T) {
	avecMonitoringVide(t)

	poserJoueur("u-public", 1001, "regular", "sessions/pub")
	poserJoueur("u-prive", 1002, "private", "sessions/priv")
	dashNoteRoomMode("sessions/pub", "Guerre de territoire", "regular", 1, 8, "u-public", 1001, []string{"u-public"})
	dashNoteRoomMode("sessions/priv", "Match prive", "private", 1, 8, "u-prive", 1002, []string{"u-prive"})

	out := lireOnline(t)

	salons, _ := out["lobbies"].([]any)
	if len(salons) != 1 {
		t.Fatalf("1 salon public attendu, %d rendu(s)", len(salons))
	}
	if m := salons[0].(map[string]any)["mode"]; m != "regular" {
		t.Fatalf("le salon rendu devrait etre le public, mode=%v", m)
	}
	if p, _ := out["privateLobbies"].(float64); p != 1 {
		t.Fatalf("le salon prive doit etre COMPTE (1 attendu, %v rendu)", p)
	}
	// Le PID du salon prive ne doit apparaitre nulle part.
	if brut, _ := json.Marshal(out); strings.Contains(string(brut), "1002") {
		t.Fatalf("le PID du salon prive a fuite : %s", brut)
	}
}

func TestOnlineNePublieNiAdresseNiCompte(t *testing.T) {
	avecMonitoringVide(t)

	poserJoueur("u-en-recherche", 2001, "bankara_open", "")
	poserJoueur("u-en-salon", 2002, "coop", "sessions/coop")
	dashNoteRoomMode("sessions/coop", "Salmon Run", "coop", 1, 4, "u-en-salon", 2002, []string{"u-en-salon"})

	out := lireOnline(t)
	brut, _ := json.Marshal(out)

	for _, interdit := range []string{"203.0.113.7", "u-en-recherche", "u-en-salon", "\"ip\"", "\"uid\"", "\"isp\"", "\"city\""} {
		if strings.Contains(string(brut), interdit) {
			t.Fatalf("%q ne doit pas sortir de /api/online : %s", interdit, brut)
		}
	}

	// La recherche en cours doit etre visible : c'est la moitie utile de la page.
	modes, _ := out["modes"].([]any)
	var vuRecherche bool
	for _, m := range modes {
		e := m.(map[string]any)
		if e["mode"] == "bankara_open" {
			if s, _ := e["searching"].(float64); s == 1 {
				vuRecherche = true
			}
		}
	}
	if !vuRecherche {
		t.Fatalf("la recherche en anarchie ouverte devrait etre comptee : %s", brut)
	}
}
