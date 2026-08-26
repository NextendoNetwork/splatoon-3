package main

// Verrou sur les casiers de la place.
//
// Mesure du 2026-08-23 : sur 905 sauvegardes Splatoon 3, 905 portent un LockerInfo — mais seuls 37
// joueurs ont repondu OUI a la question que le jeu leur pose, « veux-tu publier ton casier »
// (IsUploadLockerInfo). C'est cette reponse-la qui decide, pas nous.

import (
	"strings"
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

func sauvegardeAvecCasier(consent bool, avecInfo bool) *commonpb.MapValue {
	f := map[string]*commonpb.Value{
		"IsUploadLockerInfo": {ValueType: &commonpb.Value_BooleanValue{BooleanValue: consent}},
		"AppVersion":         gsStr("9.9.9"),
		"FestRegion":         gsStr("EU"),
	}
	if avecInfo {
		f["LockerInfo"] = gsMap(map[string]*commonpb.Value{
			"ContentInfo": gsMap(map[string]*commonpb.Value{"ObjectInfo": gsTableau()}),
		})
	}
	return &commonpb.MapValue{Fields: f}
}

func TestCasierPublieUniquementAvecConsentement(t *testing.T) {
	if d := casierDuJoueur("u-refus", sauvegardeAvecCasier(false, true)); d != nil {
		t.Error("casier publie alors que le joueur a repondu NON dans le jeu : 868 joueurs sur 905 sont dans ce cas")
	}
	if d := casierDuJoueur("u-accord", sauvegardeAvecCasier(true, true)); d == nil {
		t.Error("casier non publie alors que le joueur a accepte")
	}
}

func TestCasierSansContenuNEstPasPublie(t *testing.T) {
	if d := casierDuJoueur("u-vide", sauvegardeAvecCasier(true, false)); d != nil {
		t.Error("document publie sans LockerInfo : il n y a rien a montrer")
	}
}

func TestFormeDuDocumentSuitLaCapture(t *testing.T) {
	d := casierDuJoueur("u-accord", sauvegardeAvecCasier(true, true))
	if d == nil {
		t.Fatal("aucun document")
	}
	if !strings.Contains(d.GetName(), "/documents/services/toyohr/interfaces/locker/lockerPosts/") {
		t.Errorf("chemin %q : la capture range les casiers sous locker/lockerPosts", d.GetName())
	}
	casier := d.GetFields().GetFields()["Locker"].GetMapValue()
	if casier == nil {
		t.Fatal("le document doit porter un bloc Locker, comme la capture")
	}
	for _, k := range []string{"LockerInfo", "AppVersion", "FestRegion"} {
		if casier.GetFields()[k] == nil {
			t.Errorf("champ %s absent du bloc Locker", k)
		}
	}
	// Aucune identite dans un casier : la capture n en porte pas.
	for _, interdit := range []string{"UserName", "Byname", "NamePlate", "npln_user_id"} {
		if casier.GetFields()[interdit] != nil || d.GetFields().GetFields()[interdit] != nil {
			t.Errorf("%s present : le document de casier capture ne porte aucune identite", interdit)
		}
	}
}

func TestIdentifiantDeCasierEstStable(t *testing.T) {
	a := identifiantDeCasier("u-exemple7000000000000")
	b := identifiantDeCasier("u-exemple7000000000000")
	if a != b {
		t.Error("identifiant instable : la console verrait un casier different a chaque demarrage")
	}
	if a == identifiantDeCasier("u-autre") {
		t.Error("deux joueurs partagent le meme identifiant de casier")
	}
	if len(a) != 36 || strings.Count(a, "-") != 4 {
		t.Errorf("identifiant %q : la capture utilise la forme uuid", a)
	}
}

func TestLesDeuxCollectionsSontDistinctes(t *testing.T) {
	// Un jeu de cartes ne doit pas se retrouver parmi les casiers, ni l'inverse : les deux
	// collections vivent sous des chemins differents et le hall les affiche a des endroits
	// differents.
	if collectionDesCasiers == collectionDesJeux {
		t.Fatal("les deux collections pointent au meme endroit")
	}
	if !strings.HasSuffix(collectionDesCasiers, "locker/lockerPosts") {
		t.Errorf("casiers : %q, la capture les range sous locker/lockerPosts", collectionDesCasiers)
	}
	if !strings.HasSuffix(collectionDesJeux, "canola/deckPosts") {
		t.Errorf("jeux de cartes : %q, la capture les range sous canola/deckPosts", collectionDesJeux)
	}
}

func TestPlafondsAlignesSurNintendo(t *testing.T) {
	// Mesure du demarrage, 2026-08-23 : Nintendo pousse 31 casiers et 6 jeux de cartes.
	if casiersPousses != 31 {
		t.Errorf("plafond des casiers = %d, la capture en montre 31", casiersPousses)
	}
	if jeuxPousses != 6 {
		t.Errorf("plafond des jeux = %d, la capture en montre 6", jeuxPousses)
	}
}

func TestChaqueCollectionASonPropreCache(t *testing.T) {
	magasinIsole(t)
	cachesDeLaPlace.Lock()
	cachesDeLaPlace.m = map[string]*cacheCollection{}
	cachesDeLaPlace.Unlock()

	// Publier un casier ne doit pas faire apparaitre un jeu de cartes.
	if n := len(jeuxDeCartesPublics()); n != 0 {
		t.Errorf("%d jeu(x) de cartes sortis d un magasin vide", n)
	}
	if n := len(casiersPublics()); n != 0 {
		t.Errorf("%d casier(s) sortis d un magasin vide", n)
	}
}
