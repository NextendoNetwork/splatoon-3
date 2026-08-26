package main

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// TestAllSchedulesAreCurrent asserts every schedule type we serve lands on the present
// after the shift. A STALE schedule is what S3's NPLN layer rejects (2321-4992), so the
// captured 2026-06-28 windows must not survive into the response.
func TestAllSchedulesAreCurrent(t *testing.T) {
	exigeCapture(t, rawCoopSchedules, "captured/SelectCoopSchedules.bin")
	now := time.Now().UTC()
	delta := scheduleDelta()
	t.Logf("delta global = %s", delta)

	cases := []struct {
		name string
		raw  []byte
		msg  proto.Message
	}{
		{"SelectVsSchedules", rawVsSchedules, &toyohrpb.SelectVsSchedulesResponse{}},
		{"SelectCoopSchedules", rawCoopSchedules, &toyohrpb.SelectCoopSchedulesResponse{}},
		{"SelectLeagueSchedules", rawLeagueSchedules, &toyohrpb.SelectLeagueSchedulesResponse{}},
		{"SelectSeasonSchedules", rawSeasonSchedules, &toyohrpb.SelectSeasonSchedulesResponse{}},
	}

	for _, c := range cases {
		// Before: what the captured bytes say untouched.
		before := c.msg.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(c.raw, before); err != nil {
			t.Fatalf("%s: unmarshal: %v", c.name, err)
		}
		var bMin, bMax time.Time
		walkTimestamps(before.ProtoReflect(), func(ts *timestamppb.Timestamp) {
			if ts.Seconds <= 0 {
				return
			}
			tm := ts.AsTime()
			if bMin.IsZero() || tm.Before(bMin) {
				bMin = tm
			}
			if bMax.IsZero() || tm.After(bMax) {
				bMax = tm
			}
		})

		// After: what serveShifted actually puts on the wire.
		serveShifted(c.name, c.raw, c.msg, delta)
		var aMin, aMax time.Time
		sentinels := 0
		walkTimestamps(c.msg.ProtoReflect(), func(ts *timestamppb.Timestamp) {
			if ts.Seconds <= 0 {
				sentinels++
				return
			}
			tm := ts.AsTime()
			if aMin.IsZero() || tm.Before(aMin) {
				aMin = tm
			}
			if aMax.IsZero() || tm.After(aMax) {
				aMax = tm
			}
		})

		t.Logf("%-22s AVANT %s -> %s | APRES %s -> %s (sentinels intacts: %d)",
			c.name,
			bMin.Format("2006-01-02 15:04"), bMax.Format("2006-01-02 15:04"),
			aMin.Format("2006-01-02 15:04"), aMax.Format("2006-01-02 15:04"), sentinels)

		if aMin.IsZero() {
			t.Errorf("%s: aucun timestamp reel", c.name)
			continue
		}
		// The window must straddle the present: something already started, something
		// still to come. That is what "there is a rotation right now" means to S3.
		if !aMin.Before(now) || !aMax.After(now) {
			t.Errorf("%s: la fenetre %s->%s n'encadre PAS le present (%s)",
				c.name, aMin.Format(time.RFC3339), aMax.Format(time.RFC3339), now.Format(time.RFC3339))
		}
	}
}
