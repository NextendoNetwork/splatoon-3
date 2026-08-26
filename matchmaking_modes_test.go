package main

// Verrou sur la SEPARATION DES FILES PAR MODE.
//
// Mesure du 2026-08-19, serveur de test ouvert : en soixante minutes, 255 tickets
// « regular_match_config » (capacite 8) et 4 tickets « coop_regular_config » (capacite 4) etaient
// verses dans UNE SEULE file sans aucun filtre. formMatchLocked prenait alors la configuration du
// PREMIER joueur de la liste et en etiquetait toute la session : les sept autres recevaient une
// partie dans un mode qu'ils n'avaient pas demande.
//
// Le mecanisme est cumulatif. Un joueur parti en Salmon Run ne trouve pas ses quatre coequipiers
// quand il est seul, donc son ticket ne quitte jamais la file — et il est happe dans CHAQUE salon
// forme pendant ce temps. Un seul chercheur bloque suffit a empoisonner une serie entiere de salons,
// ce qui donne exactement le symptome observe : parfois la partie se lance, parfois elle se delite.

import (
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	mmpb "npln.nintendo.net/npln-practice/proto/matchmaking/v1"
)

// waiterEnMode fabrique un joueur en attente d'une partie de la configuration demandee.
func waiterEnMode(uid, cfg string) *mmWaiter {
	return &mmWaiter{
		uid: uid,
		out: make(chan *mmpb.MatchmakingTicket, 1),
		ticket: &mmpb.MatchmakingTicket{
			Name:              npnTenant + "/matchmakingTickets/" + uuid4(),
			MatchmakingConfig: npnTenant + "/matchmakingConfigs/" + cfg,
			State:             mmpb.MatchmakingTicket_SEARCHING,
			UserDefinitions: []*mmpb.UserDefinition{{
				User:        npnTenant + "/users/" + uid,
				Attributes:  &commonpb.MapValue{Fields: map[string]*commonpb.Value{"uid": vStr(uid)}},
				LatencyData: &mmpb.LatencyData{},
			}},
		},
	}
}

// servi dit si le joueur a recu son ticket SUCCEEDED.
func servi(w *mmWaiter) bool {
	select {
	case <-w.out:
		return true
	default:
		return false
	}
}

func TestSalmonRunNeJamaisVerseDansUnSalonTurf(t *testing.T) {
	t.Setenv("NPLN_JWT_KEY", t.TempDir()+"/test_es256.key")

	turf := make([]*mmWaiter, 0, 8)
	for _, uid := range []string{
		"u-aaaaaaaaaaaaaaaaaaaa", "u-bbbbbbbbbbbbbbbbbbbb", "u-cccccccccccccccccccc",
		"u-dddddddddddddddddddd", "u-eeeeeeeeeeeeeeeeeeee", "u-ffffffffffffffffffff",
		"u-gggggggggggggggggggg", "u-hhhhhhhhhhhhhhhhhhhh",
	} {
		turf = append(turf, waiterEnMode(uid, "regular_match_config"))
	}
	salmon := waiterEnMode("u-ssssssssssssssssssss", "coop_regular_config")

	m := newMatchmaker()
	m.mu.Lock()
	// Le chercheur Salmon Run est arrive EN PREMIER : c'est lui qui, sans filtre, imposait sa
	// configuration a toute la session.
	m.waiting = append([]*mmWaiter{salmon}, turf...)
	m.formMatchLocked("regular_match_config", "203.0.113.7", 7575)
	m.mu.Unlock()

	if servi(salmon) {
		t.Fatal("le chercheur Salmon Run a ete verse dans un salon de Guerre de territoire : " +
			"sa console recevrait une partie dans un mode qu'elle n'a pas demande")
	}
	for _, w := range turf {
		if !servi(w) {
			t.Fatalf("%s : joueur Turf non servi alors que les huit etaient reunis", w.uid)
		}
	}

	// Il doit RESTER en file pour trouver ses propres coequipiers.
	m.mu.Lock()
	reste := m.waitersForConfigLocked("coop_regular_config")
	m.mu.Unlock()
	if len(reste) != 1 {
		t.Fatalf("%d chercheur(s) Salmon Run en file, attendu 1 : son ticket a ete consomme "+
			"par un salon qui n'etait pas le sien", len(reste))
	}
}

func TestSeuilSuitLaCapaciteDuMode(t *testing.T) {
	// 8 en Guerre de territoire et en Anarchie, 4 en Salmon Run. Un seuil unique pour tout le
	// serveur ne peut pas servir les deux : a 8, aucune partie de Salmon Run ne se forme jamais ;
	// a 4, la Guerre de territoire se lance a moitie vide et le jeu la refuse.
	for _, cas := range []struct {
		cfg  string
		veut int32
	}{
		{"regular_match_config", 8},
		{"bankara_match_config", 8},
		{"coop_regular_config", 4},
	} {
		if got := seuilPourConfig(cas.cfg, 8); got != cas.veut {
			t.Errorf("seuilPourConfig(%q) = %d, attendu %d", cas.cfg, got, cas.veut)
		}
	}
}

func TestSalonNeDepasseJamaisSaCapacite(t *testing.T) {
	t.Setenv("NPLN_JWT_KEY", t.TempDir()+"/test_es256.key")

	// Douze joueurs Turf en attente : une session annoncee pour huit qui en recoit douze decrit une
	// partie que le jeu ne peut pas lancer. Le surplus doit rester en file.
	tous := make([]*mmWaiter, 0, 12)
	for _, c := range "abcdefghijkl" {
		uid := "u-" + string(c) + "0000000000000000000"
		tous = append(tous, waiterEnMode(uid, "regular_match_config"))
	}

	m := newMatchmaker()
	m.mu.Lock()
	m.waiting = append([]*mmWaiter{}, tous...)
	m.formMatchLocked("regular_match_config", "203.0.113.7", 7575)
	restants := len(m.waitersForConfigLocked("regular_match_config"))
	m.mu.Unlock()

	n := 0
	for _, w := range tous {
		if servi(w) {
			n++
		}
	}
	if n != 8 {
		t.Fatalf("%d joueurs verses dans le salon, attendu 8 (capacite de regular_match_config)", n)
	}
	if restants != 4 {
		t.Fatalf("%d joueurs restes en file, attendu 4 : le surplus doit former le salon suivant", restants)
	}
}

func TestChaqueModeAsaPropreFile(t *testing.T) {
	// La cle de regroupement est le nom EXACT de la configuration : tout mode portant un nom
	// distinct obtient donc sa file sans qu'on ait a l'enumerer. Ce test verifie qu'aucun couple de
	// modes reellement observes ne se retrouve dans la meme file.
	observes := []string{
		"regular_match_config",                   // journal du 2026-08-19
		"coop_regular_config",                    // journal du 2026-08-19
		"bankara_match_challenge_ar_team_config", // ticket capture le 2026-08-14 a 14:17:56
		"coop_private_config",
		"private_match_config",
	}

	m := newMatchmaker()
	m.mu.Lock()
	for i, cfg := range observes {
		m.waiting = append(m.waiting, waiterEnMode("u-"+string(rune('a'+i))+"1111111111111111111", cfg))
	}
	files := m.configsEnAttenteLocked()
	m.mu.Unlock()

	if len(files) != len(observes) {
		t.Fatalf("%d file(s) pour %d modes distincts : deux modes partagent une file", len(files), len(observes))
	}
	for _, cfg := range observes {
		m.mu.Lock()
		n := len(m.waitersForConfigLocked(cfg))
		m.mu.Unlock()
		if n != 1 {
			t.Errorf("%s : %d joueur(s) dans sa file, attendu 1", cfg, n)
		}
	}
}

func TestModeInconnuResteVisible(t *testing.T) {
	// Un mode jamais vu ne doit PAS etre range de force dans une categorie : il se signale tel quel
	// dans le monitoring. Deguiser un inconnu en Guerre de territoire, c'est masquer exactement le
	// genre de melange qu'on cherche a rendre visible.
	if got := nomLisibleDuMode("tenants/t/matchmakingConfigs/mode_jamais_vu_config"); got != "mode_jamais_vu_config" {
		t.Errorf("mode inconnu rendu %q, attendu le nom brut", got)
	}
	for cfg, veut := range map[string]string{
		"regular_match_config":                   "Guerre de territoire",
		"coop_regular_config":                    "Salmon Run",
		"coop_private_config":                    "Salmon Run (prive)",
		"bankara_match_challenge_ar_team_config": "Anarchie (serie)",
		"private_match_config":                   "Match prive",
	} {
		if got := nomLisibleDuMode(cfg); got != veut {
			t.Errorf("nomLisibleDuMode(%q) = %q, attendu %q", cfg, got, veut)
		}
	}
}
