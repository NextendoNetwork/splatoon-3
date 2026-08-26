package main

// Verrou sur l'ALIGNEMENT DE LA FETE MAISON SUR SON PAQUET.
//
// Le jeu recoupe ce que l'horaire annonce avec ce que le cache BCAT contient. Trois choses
// doivent concorder — l'identifiant, la ressource et l'empreinte — et si l'une diverge il leve
// BcatInvalid sans autre explication (voir fest_align.go).
//
// L'empreinte est le piege : elle etait DERIVEE du couple identifiant + ressource, donc elle ne
// pouvait par construction jamais tomber sur celle d'un paquet existant. Servir un vrai paquet
// etait impossible tant qu'on ne pouvait pas l'imposer.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// avecDrapeaux ecrit un fichier de drapeaux temporaire et l'installe le temps d'un test.
// Un drapeau par ligne : « nom=valeur » sur la meme ligne casse les DEUX drapeaux voisins.
func avecDrapeaux(t *testing.T, paires map[string]string) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "soir.flags")
	var b strings.Builder
	for k, v := range paires {
		if v == "" {
			continue
		}
		b.WriteString(k + "=" + v)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(f, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	ancien := cheminFlags
	cheminFlags = f
	invaliderCacheFlags()
	t.Cleanup(func() {
		cheminFlags = ancien
		invaliderCacheFlags()
	})
}

// invaliderCacheFlags force la relecture : le cache tient une seconde, ce qui suffirait a faire
// lire a un test le fichier du test precedent.
func invaliderCacheFlags() {
	flagsCache.Lock()
	flagsCache.lu = time.Time{}
	flagsCache.Unlock()
}

func TestEmpreinteSuitLePaquetServi(t *testing.T) {
	// Une empreinte arbitraire : le test verifie une NON-collision, donc seule compte le fait
	// qu'elle soit distincte de ce que la derivation produit.
	const reelle = "0000000000000000000000000000000000000001"

	sans := empreinteDeFeteMaison("NXTD-00042", "juea-ability")
	if sans == reelle {
		t.Fatalf("l'empreinte derivee ne peut pas tomber sur celle d'un vrai paquet par hasard")
	}
	if len(sans) != 40 {
		t.Fatalf("une empreinte fait 40 caracteres, celle-ci en fait %d", len(sans))
	}

	avecDrapeaux(t, map[string]string{"festrevision": reelle})
	if got := empreinteDeFeteMaison("NXTD-00042", "juea-ability"); got != reelle {
		t.Fatalf("festrevision doit imposer l'empreinte du paquet : %q", got)
	}
}

func TestEmpreinteRefuseUneValeurQuiNEnEstPasUne(t *testing.T) {
	// Une valeur mal formee ne doit pas etre servie telle quelle : mieux vaut l'empreinte
	// derivee, qui est au moins bien formee, qu'un champ que le jeu ne saura pas lire.
	for _, mauvaise := range []string{"1", "oui", strings.Repeat("z", 40), strings.Repeat("a", 39)} {
		avecDrapeaux(t, map[string]string{"festrevision": mauvaise})
		got := empreinteDeFeteMaison("NXTD-1", "juea-ability")
		if got == mauvaise {
			t.Fatalf("%q ne doit pas etre servie comme empreinte", mauvaise)
		}
		if len(got) != 40 {
			t.Fatalf("repli attendu sur une empreinte de 40 caracteres, %d obtenus", len(got))
		}
	}
}

func TestIdentifiantSuitLePaquetServi(t *testing.T) {
	avecDrapeaux(t, map[string]string{"festid": "JUEA-00201"})
	if got := identifiantDeFeteMaison(time.Unix(1787000000, 0).UTC()); got != "JUEA-00201" {
		t.Fatalf("festid doit imposer l'identifiant : %q", got)
	}
}

func TestSansDrapeauLIdentifiantResteLeNotre(t *testing.T) {
	avecDrapeaux(t, map[string]string{"festid": ""})
	got := identifiantDeFeteMaison(time.Unix(1787000000, 0).UTC())
	if !strings.HasPrefix(got, "NXTD-") {
		t.Fatalf("sans drapeau, la fete doit rester la notre (prefixe NXTD) : %q", got)
	}
}
