package main

import (
	"testing"

	"google.golang.org/protobuf/proto"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// Le classement final porte QUATRE listes, la quatrieme ecrite a la main en champ inconnu. On
// verifie qu'elle se serialise et se relit — c'est la partie la plus susceptible de produire un
// protobuf invalide, et un protobuf invalide fait abandonner le jeu.
func TestVerdictSeSerialiseEtSeRelit(t *testing.T) {
	r := resultatDeFeteTerminee("tenants/t/festResults/JUEA-00206", "JUEA-00206")
	b, err := proto.Marshal(r)
	if err != nil {
		t.Fatalf("serialisation impossible : %v", err)
	}
	relu := &toyohrpb.FestResult{}
	if err := proto.Unmarshal(b, relu); err != nil {
		t.Fatalf("protobuf invalide : %v", err)
	}
	if relu.GetHonsaiFinal() == nil {
		t.Fatal("le classement final a disparu")
	}
	inconnu := relu.GetHonsaiFinal().ProtoReflect().GetUnknown()
	if len(inconnu) == 0 {
		t.Error("le champ 5, le tricolore, n'a pas survecu a l'aller-retour")
	}
	t.Logf("verdict %d o, dont %d o de champ inconnu", len(b), len(inconnu))
	t.Logf("points : %s", resumeDesPoints(relu.GetOverall().GetPoints()))
	t.Logf("poids : %d entrees", len(relu.GetOverall().GetWeights().GetFields()))
}
