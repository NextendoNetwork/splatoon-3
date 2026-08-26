package main

// Verrou sur l'inventaire du verdict SALMON RUN.
//
// Releve sur la capture Nintendo du 2026-08-20 00:34 (relais-session, hote de session
// 34.44.179.9:7287) : le document @@RefereeResult d'une partie coop fait 2007 octets et porte ONZE
// champs de haut niveau. La console y ecrit d'abord @@Profile, puis @@RefereeRequest, puis
// @@RefereeReport — exactement la meme sequence qu'en versus — et Nintendo pousse @@RefereeResult
// TROIS fois sur KeepUserSession, distingue par `status` (-1 observe dans la premiere poussee).
//
// L'enjeu du test : l'inventaire versus, applique a une partie coop, supprimerait quatre champs
// (challenge_grade_point, scenario, shift_id, shift_time) et rendrait un verdict illisible.

import (
	"sort"
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

func TestInventaireDuVerdictCoop(t *testing.T) {
	releves := []string{
		"challenge_grade_point", "parent_npln_user_id", "personal_result", "request_id",
		"scenario", "shift_id", "shift_time", "status", "team_result", "trace_id", "users",
	}

	if len(champsDuVerdictCoop) != len(releves) {
		t.Fatalf("inventaire coop de %d champs, %d releves sur la capture", len(champsDuVerdictCoop), len(releves))
	}
	for _, k := range releves {
		if !champsDuVerdictCoop[k] {
			t.Errorf("%q absent de l'inventaire coop alors qu'il est dans le verdict de Nintendo", k)
		}
	}

	// Les quatre champs propres au coop que l'inventaire VERSUS aurait supprimes.
	for _, k := range []string{"challenge_grade_point", "scenario", "shift_id", "shift_time"} {
		if champsDuVerdict[k] {
			t.Errorf("%q figure dans l'inventaire versus : le releve dit le contraire", k)
		}
		if !champsDuVerdictCoop[k] {
			t.Errorf("%q doit survivre en coop — c'est precisement ce que l'ancien filtre detruisait", k)
		}
	}

	// Le coop n'a RIEN a faire des champs de terrain du versus.
	for _, k := range []string{"stage", "game_rule", "teams", "mode", "est_mmr_team1", "fest_id"} {
		if champsDuVerdictCoop[k] {
			t.Errorf("%q ne figure pas dans le verdict coop de Nintendo", k)
		}
	}
}

func TestModeDuMatchLitLeGsid(t *testing.T) {
	// Le suffixe d'un match porte « <horodatage>_<gsid> » : c'est par lui qu'on retrouve le mode.
	// Forme exacte relevee sur la capture : 20260820T003453_65f23ae4-432b-400c-871a-9cb53ba31b77
	gsid := "65f23ae4-432b-400c-871a-9cb53ba31b77"
	rememberRoomSettings(gsid, "", true, "coop_regular_config", nil)

	if got := modeDuMatch("20260820T003453_" + gsid); got != "coop_regular_config" {
		t.Fatalf("modeDuMatch = %q, attendu coop_regular_config — sans lui, un verdict coop repart "+
			"sur l'inventaire versus", got)
	}
	if got := modeDuMatch("sans-souligne"); got != "" {
		t.Errorf("suffixe sans souligne : %q, attendu vide", got)
	}
}

func TestPersonalResultCoopRecopieTelQuel(t *testing.T) {
	// Releve sur la capture Nintendo du 2026-08-20 : le personal_result d'un joueur en Salmon Run
	// porte SEPT champs, et `report_score` y est un MESSAGE — c'est lui qui porte l'anti-triche
	// (cheating, cheat_reason_flags, survival, disconnect...). Le chemin versus y ajouterait vingt-
	// trois champs et convertirait report_score en nombre, detruisant ce bloc.
	cle := "20260820T003453_65f23ae4-432b-400c-871a-9cb53ba31b77"
	joueur := "c6895a57-c729-4aa4-8784-0cb3e89c1eb2"

	entree := gsMap(map[string]*commonpb.Value{
		"npln_user_id":  gsStr(joueur),
		"paint_point":   gsInt(1234),
		"catalog_point": gsInt(56),
		"group_leader":  gsInt(1),
		"disconnected":  gsInt(0),
		"personal_wave_result": gsMap(map[string]*commonpb.Value{
			"golden_ikura_num": gsInt(15),
		}),
		"report_score": gsMap(map[string]*commonpb.Value{
			"cheating":           gsInt(0),
			"cheat_reason_flags": gsInt(0),
			"survival":           gsInt(1),
		}),
	})

	if n := retenirRapport(cle, gsMap(map[string]*commonpb.Value{joueur: entree})); n != 1 {
		t.Fatalf("rapport retenu pour %d joueur(s), attendu 1", n)
	}

	carte, n := resultatsIndividuels(cle, 0, true)
	if carte == nil || n != 1 {
		t.Fatalf("personal_result coop absent (n=%d)", n)
	}
	got := carte.GetMapValue().GetFields()[joueur].GetMapValue().GetFields()
	if len(got) != 7 {
		noms := make([]string, 0, len(got))
		for k := range got {
			noms = append(noms, k)
		}
		sort.Strings(noms)
		t.Fatalf("%d champs pour le joueur, attendu 7 (les champs versus ont ete injectes) : %v", len(got), noms)
	}
	if got["report_score"].GetMapValue() == nil {
		t.Fatal("report_score n'est plus un message : enDouble l'a converti et l'anti-triche est perdu")
	}
	if got["report_score"].GetMapValue().GetFields()["cheat_reason_flags"] == nil {
		t.Fatal("cheat_reason_flags disparu du report_score")
	}
	if _, ajoute := got["deemed_result"]; ajoute {
		t.Error("deemed_result ajoute : ce champ versus n'existe pas dans le verdict coop de Nintendo")
	}
}

func TestVerdictCoopSeReconnaitASonContenu(t *testing.T) {
	// POURQUOI CE TEST EXISTE — mesure du 2026-08-21, premiere Salmon Run jouee sur Nextendo.
	//
	// L'apparieur et l'arbitre sont deux PROCESSUS distincts (conteneur npln d'un cote,
	// service gamesync7575 de l'autre). roomSettingsByGsid vit en memoire : cote arbitre elle est
	// toujours vide, donc modeDuMatch rend "" et le match repart sur l'inventaire versus.
	//
	// Ce jour-la, le verdict livre au jeu s'est vu retirer [challenge_grade_point job_id scenario
	// shift_id shift_time]. Les quatre joueurs ont fini leur partie avec zero point.
	//
	// La reconnaissance par le CONTENU ne depend d'aucune memoire partagee : elle doit suffire.
	versus := map[string]*commonpb.Value{
		"stage": gsStr("Vss_Yagara"), "game_rule": gsStr("Vss_Pnt"), "teams": gsInt(2),
		"mode": gsStr("regular"), "status": gsInt(1),
	}
	if verdictCoop(versus) {
		t.Error("un verdict de Guerre de territoire ne doit jamais passer pour du coop")
	}

	// Chacun des cinq champs, seul, doit suffire a trancher.
	for _, k := range champsPropresAuCoop {
		if !verdictCoop(map[string]*commonpb.Value{"status": gsInt(1), k: gsStr("x")}) {
			t.Errorf("%q seul ne suffit pas a reconnaitre une partie coop", k)
		}
		if champsDuVerdict[k] {
			t.Errorf("%q figure dans l'inventaire versus : ce n'est donc pas une signature coop", k)
		}
	}

	// Le cas reel : salon inconnu de l'arbitre, verdict porteur de shift_id.
	if modeDuMatch("20260821T195214_inconnu-de-larbitre") != "" {
		t.Fatal("ce gsid doit etre inconnu, c'est tout l'interet du cas")
	}
	reel := map[string]*commonpb.Value{
		"shift_id": gsStr("s3-2026-08-21"), "challenge_grade_point": gsInt(120), "status": gsInt(1),
	}
	if !verdictCoop(reel) {
		t.Error("salon inconnu + champs coop : c'est exactement la partie du 21/08, elle doit etre reconnue")
	}
}
