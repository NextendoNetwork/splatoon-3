package main

// Verrou sur l'inscription a un Splatfest, ecrit sur la capture Nintendo du 2026-08-20 16:09
// (relais-session, fete JUEA-00107, equipe Bravo, region EU) :
//
//	CreateFestEntry  parent="tenants/current/fests/JUEA-00107"
//	                 fest_entry={fest_team:"Bravo", fest_region:"EU"}
//	     -> name="tenants/t-dce9377b-lp1/fests/JUEA-00107/entries/u-exemple7000000000000"
//	        fest_team="Bravo" fest_region="EU"
//
//	GetFestEntry     name="tenants/current/fests/JUEA-00107/entries/current"
//	     -> AUCUNE reponse tant que le joueur n'a pas choisi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

const (
	festCapture = "JUEA-00107"
	uidCapture  = "u-exemple7000000000000"
)

// ctxJoueur reproduit l'identite authentifiee : un UID seul dans les metadonnees est
// fourni par le client et ne peut plus etre utilise comme preuve d'identite.
func ctxJoueur(uid string) context.Context {
	token := mintNplnAccessToken(1800004321, nplnTenant+"/users/"+uid, nplnTenant)
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer "+token,
		"uid", "spoofed-client-metadata",
		"npln-tenant-id", "t-dce9377b-lp1",
	))
}

// festIsole repointe le magasin sur un fichier temporaire et le vide.
func festIsole(t *testing.T) {
	t.Helper()
	ancien := cheminEntreesFest
	cheminEntreesFest = filepath.Join(t.TempDir(), "fest_entries.json")
	entreesFest.Lock()
	entreesFest.m = map[string]entreeFest{}
	entreesFest.chargé = false
	entreesFest.Unlock()
	t.Cleanup(func() {
		cheminEntreesFest = ancien
		entreesFest.Lock()
		entreesFest.m = map[string]entreeFest{}
		entreesFest.chargé = false
		entreesFest.Unlock()
	})
}

// cheminFlagsFest allume le drapeau « festequipes » sur un fichier temporaire, et remet le cache
// de drapeaux a zero pour qu'il soit relu tout de suite.
func cheminFlagsFest(t *testing.T) {
	t.Helper()
	ancien := cheminFlags
	f := filepath.Join(t.TempDir(), "soir.flags")
	if err := os.WriteFile(f, []byte("festequipes\n"), 0o644); err != nil {
		t.Fatalf("ecriture des drapeaux : %v", err)
	}
	cheminFlags = f
	flagsCache.Lock()
	flagsCache.lu = time.Time{}
	flagsCache.Unlock()
	t.Cleanup(func() {
		cheminFlags = ancien
		flagsCache.Lock()
		flagsCache.lu = time.Time{}
		flagsCache.Unlock()
	})
	if !festEntriesActives() {
		t.Fatal("le drapeau festequipes n'a pas ete pris en compte")
	}
}

func TestFestIDLuDansLesCheminsDeLaCapture(t *testing.T) {
	for _, cas := range []struct{ chemin, veut string }{
		{"tenants/current/fests/JUEA-00107", festCapture},
		{"tenants/current/fests/JUEA-00107/entries/current", festCapture},
		{"tenants/t-dce9377b-lp1/fests/JUEA-00107/entries/" + uidCapture, festCapture},
		{"tenants/current/fests/JUEA-00107/decryptionKey", festCapture},
		{"tenants/current/documents/quelquechose", ""},
	} {
		if got := festIDDuChemin(cas.chemin); got != cas.veut {
			t.Errorf("festIDDuChemin(%q) = %q, attendu %q", cas.chemin, got, cas.veut)
		}
	}
}

func TestFestEteinteParDefaut(t *testing.T) {
	festIsole(t)
	// Le drapeau « festequipes » n'est pas pose : les deux methodes doivent rendre exactement ce
	// qu'elles rendaient AVANT d'exister. Allumer la fete est une decision, pas un effet de bord.
	ancien := cheminFlags
	cheminFlags = filepath.Join(t.TempDir(), "vide.flags")
	flagsCache.Lock()
	flagsCache.lu = time.Time{}
	flagsCache.Unlock()
	t.Cleanup(func() { cheminFlags = ancien })
	if festEntriesActives() {
		t.Fatal("l'inscription aux fetes est active sans drapeau")
	}
	s := &festServer{}
	_, err := s.CreateFestEntry(ctxJoueur(uidCapture), &toyohrpb.CreateFestEntryRequest{
		Parent:    "tenants/current/fests/" + festCapture,
		FestEntry: &toyohrpb.FestEntry{FestTeam: "Bravo", FestRegion: "EU"},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("CreateFestEntry eteinte : code %v, attendu Unimplemented", status.Code(err))
	}
	_, err = s.GetFestEntry(ctxJoueur(uidCapture), &toyohrpb.GetFestEntryRequest{
		Name: "tenants/current/fests/" + festCapture + "/entries/current",
	})
	if status.Code(err) != codes.Unimplemented {
		t.Errorf("GetFestEntry eteinte : code %v, attendu Unimplemented", status.Code(err))
	}
}

func TestInscriptionSuitLaCapture(t *testing.T) {
	festIsole(t)
	cheminFlagsFest(t)

	s := &festServer{}
	ctx := ctxJoueur(uidCapture)

	// AVANT le choix : rien, c'est ce qui autorise le jeu a proposer l'ecran d'equipe.
	if _, err := s.GetFestEntry(ctx, &toyohrpb.GetFestEntryRequest{
		Name: "tenants/current/fests/" + festCapture + "/entries/current",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("avant le choix : code %v, attendu NotFound", status.Code(err))
	}

	// LE CHOIX.
	got, err := s.CreateFestEntry(ctx, &toyohrpb.CreateFestEntryRequest{
		Parent:    "tenants/current/fests/" + festCapture,
		FestEntry: &toyohrpb.FestEntry{FestTeam: "Bravo", FestRegion: "EU"},
	})
	if err != nil {
		t.Fatalf("CreateFestEntry : %v", err)
	}
	veutNom := "tenants/t-dce9377b-lp1/fests/" + festCapture + "/entries/" + uidCapture
	if got.GetName() != veutNom {
		t.Errorf("name = %q, attendu %q (locataire RESOLU, entree nommee par le uid)", got.GetName(), veutNom)
	}
	if got.GetFestTeam() != "Bravo" || got.GetFestRegion() != "EU" {
		t.Errorf("equipe/region = %q/%q, attendu Bravo/EU", got.GetFestTeam(), got.GetFestRegion())
	}

	// APRES le choix : l'inscription se relit.
	relu, err := s.GetFestEntry(ctx, &toyohrpb.GetFestEntryRequest{
		Name: "tenants/current/fests/" + festCapture + "/entries/current",
	})
	if err != nil {
		t.Fatalf("GetFestEntry apres le choix : %v", err)
	}
	if relu.GetFestTeam() != "Bravo" || relu.GetName() != veutNom {
		t.Errorf("relu = %q / %q", relu.GetName(), relu.GetFestTeam())
	}
}

func TestChoixDEquipeIrreversible(t *testing.T) {
	festIsole(t)
	cheminFlagsFest(t)

	s := &festServer{}
	ctx := ctxJoueur(uidCapture)
	parent := "tenants/current/fests/" + festCapture

	if _, err := s.CreateFestEntry(ctx, &toyohrpb.CreateFestEntryRequest{
		Parent:    parent,
		FestEntry: &toyohrpb.FestEntry{FestTeam: "Bravo", FestRegion: "EU"},
	}); err != nil {
		t.Fatalf("premier choix : %v", err)
	}

	// En jeu, le choix ne se fait qu'UNE fois. Une seconde demande ne doit pas changer de camp.
	got, err := s.CreateFestEntry(ctx, &toyohrpb.CreateFestEntryRequest{
		Parent:    parent,
		FestEntry: &toyohrpb.FestEntry{FestTeam: "Alpha", FestRegion: "US"},
	})
	if err != nil {
		t.Fatalf("second choix : %v", err)
	}
	if got.GetFestTeam() != "Bravo" || got.GetFestRegion() != "EU" {
		t.Fatalf("le second choix a pris : %q/%q — un joueur pourrait changer de camp en pleine fete",
			got.GetFestTeam(), got.GetFestRegion())
	}
}

func TestDeuxJoueursDeuxEquipes(t *testing.T) {
	festIsole(t)
	cheminFlagsFest(t)

	s := &festServer{}
	parent := "tenants/current/fests/" + festCapture
	for uid, equipe := range map[string]string{
		uidCapture:               "Bravo",
		"u-exemple1000000000000": "Alpha",
	} {
		if _, err := s.CreateFestEntry(ctxJoueur(uid), &toyohrpb.CreateFestEntryRequest{
			Parent:    parent,
			FestEntry: &toyohrpb.FestEntry{FestTeam: equipe, FestRegion: "EU"},
		}); err != nil {
			t.Fatalf("%s : %v", uid, err)
		}
		got, err := s.GetFestEntry(ctxJoueur(uid), &toyohrpb.GetFestEntryRequest{
			Name: parent + "/entries/current",
		})
		if err != nil || got.GetFestTeam() != equipe {
			t.Errorf("%s : relu %q, attendu %q (err=%v)", uid, got.GetFestTeam(), equipe, err)
		}
	}
}
