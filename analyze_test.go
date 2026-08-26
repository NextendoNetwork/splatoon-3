package main

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// walkTS with a path so we can SEE which field each timestamp belongs to.
func walkTSPath(m protoreflect.Message, path string, out *[]string) {
	if m.Descriptor().FullName() == "google.protobuf.Timestamp" {
		if ts, ok := m.Interface().(*timestamppb.Timestamp); ok {
			*out = append(*out, fmt.Sprintf("%-48s %s (aligned2h=%v)", path, ts.AsTime().UTC().Format("2006-01-02 15:04:05"), ts.AsTime().UTC().Truncate(2*time.Hour).Equal(ts.AsTime().UTC())))
		}
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(mk protoreflect.MapKey, mv protoreflect.Value) bool {
					walkTSPath(mv.Message(), fmt.Sprintf("%s.%s[%s]", path, name, mk.String()), out)
					return true
				})
			}
		case fd.IsList() && (fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind):
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				walkTSPath(l.Get(i).Message(), fmt.Sprintf("%s.%s[%d]", path, name, i), out)
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			walkTSPath(v.Message(), path+"."+name, out)
		}
		return true
	})
}

func dump(t *testing.T, label string, raw []byte, msg proto.Message) {
	if err := proto.Unmarshal(raw, msg); err != nil {
		t.Logf("%s: unmarshal err %v", label, err)
		return
	}
	var out []string
	walkTSPath(msg.ProtoReflect(), label, &out)
	sort.Strings(out)
	fmt.Printf("\n===== %s : %d timestamps =====\n", label, len(out))
	for _, s := range out {
		fmt.Println(s)
	}
}

func TestVerifyShiftKeepsSentinel(t *testing.T) {
	delta := scheduleDelta()
	r := &toyohrpb.SelectLeagueSchedulesResponse{}
	serveShifted("LEAGUE", rawLeagueSchedules, r, delta)
	var out []string
	walkTSPath(r.ProtoReflect(), "LEAGUE", &out)
	sort.Strings(out)
	fmt.Printf("\n=== LEAGUE APRES SHIFT (delta=%s) ===\n", delta)
	badSentinel := false
	for _, s := range out {
		fmt.Println(s)
	}
	// Concrete assertion: the two league.timestamp sentinels must remain exactly 0001-01-01.
	r2 := &toyohrpb.SelectLeagueSchedulesResponse{}
	serveShifted("LEAGUE2", rawLeagueSchedules, r2, delta)
	for i, sch := range r2.GetSchedules() {
		ts := sch.GetTimestamp()
		if ts != nil && ts.AsTime().UTC().Year() != 1 {
			t.Errorf("league[%d].timestamp got SHIFTED to %s (should stay 0001)", i, ts.AsTime().UTC())
			badSentinel = true
		}
		// start/end MUST be shifted to 2026
		if st := sch.GetStartTime(); st != nil && st.AsTime().UTC().Year() != 2026 {
			t.Errorf("league[%d].start_time = %s (expected 2026 after shift)", i, st.AsTime().UTC())
		}
	}
	if !badSentinel {
		fmt.Println(">>> OK: sentinels preserves, start/end shiftes en 2026")
	}
}

func TestAnalyzeSchedules(t *testing.T) {
	fmt.Printf("NOW (fixed ref for reading) = compare against 2026-07-15 ~xx UTC\n")
	dump(t, "VS", rawVsSchedules, &toyohrpb.SelectVsSchedulesResponse{})
	dump(t, "COOP", rawCoopSchedules, &toyohrpb.SelectCoopSchedulesResponse{})
	dump(t, "SEASON", rawSeasonSchedules, &toyohrpb.SelectSeasonSchedulesResponse{})
	dump(t, "LEAGUE", rawLeagueSchedules, &toyohrpb.SelectLeagueSchedulesResponse{})
}
