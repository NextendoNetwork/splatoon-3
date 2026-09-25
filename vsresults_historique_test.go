package main

// Verrou sur l'historique en jeu.
//
// Mesure du 2026-08-23, capture d'un Splatfest officiel contre Nintendo : apres chaque bataille le
// SERVEUR cree
//
//	tenants/t-dce9377b-lp1/documents/services/GameRecord/users/<uid>/VsResults/20260823T000736_35ddcc57-...
//
// et la console le LIT par RunQuery sur « GameRecord/users/<uid> ». Elle n'ecrit rien : zero
// WriteDocuments sur toute la capture. Chez nous, 103 RunQuery sur ce parent recevaient 103 fois
// « aucun document » — l'historique ne pouvait qu'etre vide.

import (
	"path/filepath"
	"strings"
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

// magasinIsole met le magasin de documents dans un dossier temporaire.
//
// Sans cela les tests ecrivent dans le vrai dossier de production (documentsDir derive de
// NPLN_SAVE_DIR, dont le defaut est /data/saves) et se RELISENT entre executions : un test a vu
// trois batailles la ou il en avait publie deux, la troisieme venant d'un run precedent.
func magasinIsole(t *testing.T) {
	t.Helper()
	t.Setenv("NPLN_SAVE_DIR", filepath.Join(t.TempDir(), "saves"))
	documentStore.mu.Lock()
	documentStore.docs = map[string]*ugcpb.Document{}
	documentStore.mu.Unlock()
}

const battleDeTest = "20260823T000736_35ddcc57-431a-481d-b04a-8de6e580219b"

func TestNomDuDocumentSuitLaCapture(t *testing.T) {
	got := nomDuDocumentVsResults("u-exemple7000000000000", battleDeTest)
	attendu := "tenants/t-dce9377b-lp1/documents/services/GameRecord/users/u-exemple7000000000000/VsResults/" + battleDeTest
	if got != attendu {
		t.Errorf("nom du document :\n  obtenu : %s\n  attendu: %s", got, attendu)
	}
}

func ficheJoueur(uid string, equipe int64, encre int64) *commonpb.Value {
	return gsMap(map[string]*commonpb.Value{
		"npln_user_id": gsStr(uid),
		"team":         gsInt(equipe),
		"paint_point":  gsInt(encre),
	})
}

func TestRepartitionTricolore(t *testing.T) {
	// Structure relevee sur les cinq batailles de la capture : team3 porte QUATRE joueurs, tous du
	// meme camp (Bravo dans ce festival), et defend ; team1 et team2 en portent deux chacune, Alpha
	// et Charlie. Le camp defenseur est celui qui a gagne l'intermede, fixe pour tout le festival.
	personal := &commonpb.MapValue{Fields: map[string]*commonpb.Value{
		"a": ficheJoueur("u-a", 1, 881),
		"b": ficheJoueur("u-b", 1, 993),
		"c": ficheJoueur("u-c", 2, 1150),
		"d": ficheJoueur("u-d", 2, 940),
		"e": ficheJoueur("u-e", 3, 914),
		"f": ficheJoueur("u-f", 3, 943),
		"g": ficheJoueur("u-g", 3, 972),
		"h": ficheJoueur("u-h", 3, 758),
	}}

	par := fichesParEquipe(personal)
	if n := len(par["personal_data_team1"]); n != 2 {
		t.Errorf("team1 : %d joueur(s), attendu 2", n)
	}
	if n := len(par["personal_data_team2"]); n != 2 {
		t.Errorf("team2 : %d joueur(s), attendu 2", n)
	}
	if n := len(par["personal_data_team3"]); n != 4 {
		t.Errorf("team3 : %d joueur(s), attendu 4 — c est l equipe qui defend", n)
	}
}

func TestMatchOrdinaireNeRemplitPasLaTroisiemeEquipe(t *testing.T) {
	personal := &commonpb.MapValue{Fields: map[string]*commonpb.Value{
		"a": ficheJoueur("u-a", 1, 500),
		"b": ficheJoueur("u-b", 2, 600),
	}}
	par := fichesParEquipe(personal)
	if n := len(par["personal_data_team3"]); n != 0 {
		t.Errorf("team3 : %d joueur(s) sur un match a deux camps, attendu 0", n)
	}
}

func TestUnDocumentParJoueur(t *testing.T) {
	magasinIsole(t)

	rapport := &commonpb.MapValue{Fields: map[string]*commonpb.Value{
		"team_result": gsMap(map[string]*commonpb.Value{"winner": gsInt(1)}),
		"personal_result": gsMap(map[string]*commonpb.Value{
			"a": ficheJoueur("u-a", 1, 881),
			"b": ficheJoueur("u-b", 2, 940),
		}),
	}}

	uids := []string{"u-a", "u-b"}
	if n := publierHistoriqueDeBataille(battleDeTest, "35ddcc57-431a-481d-b04a-8de6e580219b", uids, rapport); n != 2 {
		t.Fatalf("%d document(s) publie(s), attendu 2 — un par joueur, chacun sous SON chemin", n)
	}

	for _, uid := range uids {
		d, ok := storeGetDocument(nomDuDocumentVsResults(uid, battleDeTest))
		if !ok {
			t.Errorf("aucun document pour %s : sa console ne verra pas la bataille", uid)
			continue
		}
		f := d.GetFields().GetFields()
		if f["battle_id"].GetStringValue() != battleDeTest {
			t.Errorf("%s : battle_id absent ou faux", uid)
		}
		if f["npln_user_id"].GetStringValue() != uid {
			t.Errorf("%s : le document doit porter SON identifiant", uid)
		}
		if f["team_result"] == nil {
			t.Errorf("%s : team_result perdu", uid)
		}
	}
}

func TestRunQueryTrouveLesBataillesMalgreLeLocataireCurrent(t *testing.T) {
	magasinIsole(t)

	publierHistoriqueDeBataille("20260823T000736_aaa", "g1", []string{"u-a"}, nil)
	publierHistoriqueDeBataille("20260823T001816_bbb", "g2", []string{"u-a"}, nil)
	publierHistoriqueDeBataille("20260823T002241_ccc", "g3", []string{"u-autre"}, nil)

	// La console demande « tenants/current/... » alors que nos documents portent le locataire en
	// clair : sans normalisation, on ne trouverait rien.
	docs := documentsSousParent("tenants/current/documents/services/GameRecord/users/u-a")
	if len(docs) != 2 {
		t.Fatalf("%d document(s) trouve(s), attendu 2 — et surtout pas ceux d un autre joueur", len(docs))
	}
	// Le plus recent d'abord : l'identifiant commence par un horodatage compact.
	if !strings.Contains(docs[0].GetName(), "20260823T001816") {
		t.Errorf("mauvais ordre : %s en tete, attendu la bataille la plus recente", lastSeg(docs[0].GetName()))
	}
	for _, d := range docs {
		if strings.Contains(d.GetName(), "u-autre") {
			t.Error("la bataille d un autre joueur a fuite dans l historique")
		}
	}
}
