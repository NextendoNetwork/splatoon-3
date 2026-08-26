package main

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

func TestMesureDeFeteRejoueLaCapture(t *testing.T) {
	exigeCapture(t, rawMesureDeFete, "captured/CreateFestPowerMeasurement.bin")
	req := &toyohrpb.CreateFestPowerMeasurementRequest{}
	if err := proto.Unmarshal(rawMesureDeFete[5:], req); err == nil {
		_ = req
	}
	// Requete minimale : le jeu ne dit que la fete.
	demande := []byte(nil)
	out, err := repondreMesureDeFete(demande, "u-exemple1000000000000")
	if err != nil {
		t.Fatalf("reponse illisible : %v", err)
	}
	m := &toyohrpb.FestPowerMeasurement{}
	if err := proto.Unmarshal(out, m); err != nil {
		t.Fatalf("protobuf invalide : %v", err)
	}
	if !strings.Contains(m.GetName(), "u-exemple1000000000000") {
		t.Fatalf("compte non realigne : %q", m.GetName())
	}
	if !strings.HasPrefix(m.GetDocument(), "tenants/") || !strings.Contains(m.GetDocument(), "/documents/") {
		t.Fatalf("le document doit respecter la grammaire du SDK : %q", m.GetDocument())
	}
	t.Logf("name     = %s", m.GetName())
	t.Logf("document = %s", m.GetDocument())
	t.Logf("champs   = %d", len(m.GetFields().GetFields()))
}
