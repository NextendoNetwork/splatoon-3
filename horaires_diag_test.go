package main

// Diagnostic : que valent VRAIMENT les horaires que nous servons, une fois decales ?
// Le jeu affiche « informations non disponibles hors ligne » quand aucune rotation ne couvre
// l'instant present — ce test le rend visible au lieu de le supposer.

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

func TestDiagHoraires(t *testing.T) {
	exigeCapture(t, rawVsSchedules, "captured/SelectVsSchedules.bin")
	d := scheduleDelta()
	t.Logf("delta applique : %s", d)

	r := &toyohrpb.SelectVsSchedulesResponse{}
	if err := proto.Unmarshal(rawVsSchedules, r); err != nil {
		t.Fatalf("unmarshal : %v", err)
	}

	var brut []time.Time
	walkTimestamps(r.ProtoReflect(), func(ts *timestamppb.Timestamp) {
		if ts.Seconds > 0 {
			brut = append(brut, ts.AsTime())
		}
	})
	t.Logf("%d horodatages dans la capture", len(brut))

	maintenant := time.Now().UTC()
	var min, max time.Time
	couvre := 0
	for _, b := range brut {
		s := b.Add(d)
		if min.IsZero() || s.Before(min) {
			min = s
		}
		if max.IsZero() || s.After(max) {
			max = s
		}
	}
	t.Logf("apres decalage : du %s au %s", min.Format(time.RFC3339), max.Format(time.RFC3339))
	t.Logf("maintenant     : %s", maintenant.Format(time.RFC3339))
	if maintenant.Before(min) {
		t.Errorf("TOUT est dans le futur de %s -> aucune rotation ne couvre maintenant", min.Sub(maintenant))
	}
	if maintenant.After(max) {
		t.Errorf("TOUT est dans le passe de %s", maintenant.Sub(max))
	}

	// Les 6 premiers horodatages, tries, pour voir la forme des fenetres.
	for i, b := range brut {
		if i >= 6 {
			break
		}
		t.Logf("  capture %s -> servi %s", b.Format("2006-01-02 15:04"), b.Add(d).Format("2006-01-02 15:04"))
	}
	_ = couvre
}
