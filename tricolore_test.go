package main

import (
	"testing"

	"google.golang.org/protobuf/proto"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// Le champ 5 est ecrit a la main : il doit produire EXACTEMENT la meme forme que les listes
// generees par le proto, sinon le jeu le lit de travers.
func TestTricoloreMemeFormeQueLesAutresListes(t *testing.T) {
	parts := map[string]float64{"Charlie": 1, "Bravo": 0, "Alpha": 0}
	ordre := []string{"Charlie", "Bravo", "Alpha"}

	// Reference : la meme liste, encodee par le proto.
	ref, err := proto.Marshal(&toyohrpb.HonsaiFinalResult{TeamRatios3: rangs(ordre, parts)})
	if err != nil {
		t.Fatal(err)
	}
	// Le champ 4 (TeamRatios3) et le champ 5 ne different que par le numero de champ : meme taille.
	mien := champCinqTricolore(ordre, parts)
	if len(mien) != len(ref) {
		t.Fatalf("champ 5 fait %d octets, la liste equivalente du proto en fait %d", len(mien), len(ref))
	}
	// Chaque entree doit faire seize octets pour un nom de sept caracteres, comme chez Nintendo.
	if n := len(champCinqTricolore([]string{"Charlie"}, parts)); n != 20 {
		t.Errorf("une entree Charlie fait %d octets, 20 attendus (tag+len+7 nom, tag+8 ratio)", n)
	}
}
