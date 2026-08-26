package main

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// TestCapturedRoundTripIsByteExact checks whether unmarshal+marshal of each captured
// response reproduces the captured bytes exactly. Our handlers all do this round-trip
// (replay and serveShifted both go through proto), so any drift here is what actually
// reaches S3 — and npl1 rejects Select*Schedules with 2321-4992 even when the dates are
// current, which points at the bytes rather than the content.
func TestCapturedRoundTripIsByteExact(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		msg  proto.Message
	}{
		{"SelectVsSchedules", rawVsSchedules, &toyohrpb.SelectVsSchedulesResponse{}},
		{"SelectVsParams", rawVsParams, &toyohrpb.SelectVsParamsResponse{}},
		{"SelectCoopSchedules", rawCoopSchedules, &toyohrpb.SelectCoopSchedulesResponse{}},
		{"SelectLeagueSchedules", rawLeagueSchedules, &toyohrpb.SelectLeagueSchedulesResponse{}},
		{"SelectSeasonSchedules", rawSeasonSchedules, &toyohrpb.SelectSeasonSchedulesResponse{}},
		{"SelectFestSchedule", rawFestSchedule, &toyohrpb.SelectFestScheduleResponse{}},
	}

	for _, c := range cases {
		if err := proto.Unmarshal(c.raw, c.msg); err != nil {
			t.Errorf("%-22s UNMARSHAL ERROR: %v", c.name, err)
			continue
		}

		unknown := len(c.msg.ProtoReflect().GetUnknown())

		// Deterministic marshal mirrors what gRPC emits for the same message.
		out, err := proto.MarshalOptions{Deterministic: true}.Marshal(c.msg)
		if err != nil {
			t.Errorf("%-22s MARSHAL ERROR: %v", c.name, err)
			continue
		}

		if bytes.Equal(out, c.raw) {
			t.Logf("%-22s OK byte-exact (%d o, unknown=%d)", c.name, len(c.raw), unknown)
			continue
		}

		// Find the first divergence to show where the round-trip drifts.
		diffAt := -1
		for i := 0; i < len(out) && i < len(c.raw); i++ {
			if out[i] != c.raw[i] {
				diffAt = i
				break
			}
		}
		if diffAt == -1 {
			diffAt = min(len(out), len(c.raw))
		}
		t.Errorf("%-22s DIFFERE: capture=%d o, re-serialise=%d o, 1er ecart @%d (unknown=%d)",
			c.name, len(c.raw), len(out), diffAt, unknown)
	}
}
