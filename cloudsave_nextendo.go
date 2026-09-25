package main

// [Nextendo] CloudSave — real, mergeable persistence.
//
// Format PROVEN from the Proxide captures (real Switch <-> real Nintendo), see
// le corpus de captures NPLN:
//
//   GetSaveRecord  -> SaveRecord{name, save_data (MapValue, 88 top-level keys), create_time, update_time}
//   WriteSaveRecord<- WriteSaveRecordRequest{
//                        name           = "tenants/current/saveRecords/current"
//                        update_time    = the update_time the client last SAW (optimistic-concurrency token)
//                        save_event_type= "tenants/current/saveEventTypes/<Event>"  (LotResult, VersusReward, ...)
//                        event_param    = MapValue (BattleId, MatchMode, ...)
//                        save_record.save_data = PARTIAL MapValue: only what changed, at every depth
//                     }
//   WriteSaveRecord-> WriteSaveRecordResponse{timestamp}  <- becomes the NEXT request's update_time
//
// Merge rule (verified by replaying the 24 writes of that session onto its own GetSaveRecord):
//   - MapValue merges RECURSIVELY, key by key
//   - anything that is not a map (scalar, ArrayValue, Timestamp, bytes) REPLACES wholesale
//     (proof: VendorUpdateGear{"Clean":0} sends HaveGearHeadMap.3014.ExSkillArray = [] to wipe a
//      3-element array — a merge would have kept the old skills)
//   - null_value is stored as-is (the record Nintendo serves already contains 33 stored nulls)
// Result of the replay: Money 226831 -> 180860, PlayerRank 21 -> 22, HaveWeaponMap 4 -> 6 keys,
// top-level key count unchanged at 88. Coherent with the events.

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// ---------------------------------------------------------------- merge

// normStored applies Nintendo's STORAGE normalisation to a value about to be written:
// an EMPTY ArrayValue is stored as null_value, recursively, at every depth.
//
// Proven against Nintendo's own server: between the 2026-08-07 GetSaveRecord and the
// 2026-08-10 one, the client sent exactly four empty ArrayValues
// (HaveGear{Shoes,Head,Clothes}Map/27306/ExSkillArray via AcquireMissionReward and
// HaveGearHeadMap/3014/ExSkillArray via VendorUpdateGear{"Clean":0}) and exactly those four
// paths came back as null_value — the stored-null count went 33 -> 37, +4, no other change.
func normStored(v *commonpb.Value) *commonpb.Value {
	if av := v.GetArrayValue(); av != nil {
		if len(av.GetValues()) == 0 {
			return &commonpb.Value{ValueType: &commonpb.Value_NullValue{}}
		}
		out := &commonpb.ArrayValue{Values: make([]*commonpb.Value, 0, len(av.GetValues()))}
		for _, x := range av.GetValues() {
			out.Values = append(out.Values, normStored(x))
		}
		return &commonpb.Value{ValueType: &commonpb.Value_ArrayValue{ArrayValue: out}}
	}
	if mv := v.GetMapValue(); mv != nil {
		out := &commonpb.MapValue{Fields: make(map[string]*commonpb.Value, len(mv.GetFields()))}
		for k, x := range mv.GetFields() {
			out.Fields[k] = normStored(x)
		}
		return &commonpb.Value{ValueType: &commonpb.Value_MapValue{MapValue: out}}
	}
	return proto.Clone(v).(*commonpb.Value)
}

// mergeSaveData applies a WriteSaveRecord delta onto the stored save_data.
//
// The four rules are not inferred, they are REPRODUCED: starting from the SaveRecord Nintendo
// served on 2026-08-07 and applying the 24 captured WriteSaveRecord deltas, this function
// yields a save_data that is proto.Equal to the SaveRecord Nintendo served on 2026-08-10 —
// 88 top-level keys, Money 226831 -> 180860, PlayerRank 21 -> 22, zero differences.
//
//	R1  MapValue merges RECURSIVELY, key by key.
//	R2  anything else (scalar, Timestamp, bytes, non-empty ArrayValue) REPLACES wholesale.
//	R3  an EMPTY ArrayValue is stored as null_value (see normStored).
//	R4  an explicit null_value DELETES the key from its parent map.
//	    Proof: OpenCoopReward sent Coop.Rewards.RewardAry {"0": null, "1": null} over a map
//	    that held "0"; the later record has RewardAry = {} — the key is gone and the stored-null
//	    count did NOT rise by 2. Storing the null instead leaves phantom reward entries behind.
func mergeSaveData(dst, src *commonpb.MapValue) *commonpb.MapValue {
	if src == nil {
		return dst
	}
	if dst == nil {
		dst = &commonpb.MapValue{}
	}
	if dst.Fields == nil {
		dst.Fields = map[string]*commonpb.Value{}
	}
	for k, sv := range src.GetFields() {
		if _, isNull := sv.GetValueType().(*commonpb.Value_NullValue); isNull {
			delete(dst.Fields, k) // R4
			continue
		}
		if sm := sv.GetMapValue(); sm != nil { // R1
			if dv, ok := dst.Fields[k]; ok && dv.GetMapValue() != nil {
				mergeSaveData(dv.GetMapValue(), sm)
				continue
			}
		}
		dst.Fields[k] = normStored(sv) // R2 + R3
	}
	return dst
}

// ---------------------------------------------------------------- store

type saveRecordStore struct {
	mu  sync.Mutex
	mem map[string]*toyohrpb.SaveRecord
}

var recordStore = &saveRecordStore{mem: map[string]*toyohrpb.SaveRecord{}}

func recordPath(uid string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, uid)
	return filepath.Join(saveDir(), safe+".record.pb")
}

func starterGrantPath(uid string) string {
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, uid)
	return filepath.Join(saveDir(), safe+".starter-grant.v1")
}

// starterGrantAppliedLocked checks the sidecar marker while s.mu is held. Keeping this outside
// SaveData prevents a server-only flag from leaking into the game's save format.
func (s *saveRecordStore) starterGrantAppliedLocked(uid string) (bool, error) {
	_, err := os.Stat(starterGrantPath(uid))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("check starter grant marker for %q: %w", uid, err)
}

func (s *saveRecordStore) hasRecordLocked(uid string) (bool, error) {
	if _, ok := s.mem[uid]; ok {
		return true, nil
	}
	_, err := os.Stat(recordPath(uid))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("check existing cloud save %q: %w", uid, err)
}

// markStarterGrantAppliedLocked durably records that this account has passed through the starter
// grant decision. The caller holds s.mu so record and marker updates are ordered per account.
func (s *saveRecordStore) markStarterGrantAppliedLocked(uid string) error {
	path := starterGrantPath(uid)
	if err := os.MkdirAll(saveDir(), 0o700); err != nil {
		return fmt.Errorf("create starter grant directory: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil
		}
		return fmt.Errorf("create starter grant marker for %q: %w", uid, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return fmt.Errorf("sync starter grant marker for %q: %w", uid, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close starter grant marker for %q: %w", uid, err)
	}
	return nil
}

// loadLocked returns the player's record. The caller holds s.mu.
// The seed is the capture of ANOTHER account (u-exemple5000000000000, UserName "gen"), so its
// name/identity fields are rewritten onto the caller before anything is persisted.
func (s *saveRecordStore) loadLocked(uid string) (*toyohrpb.SaveRecord, error) {
	if rec, ok := s.mem[uid]; ok {
		rec = proto.Clone(rec).(*toyohrpb.SaveRecord)
		if ensureSaveIdentifier(uid, rec) {
			rec.UpdateTime = timestamppb.New(time.Now().UTC())
			if err := s.persistLocked(uid, rec); err != nil {
				return nil, err
			}
			s.mem[uid] = rec
		}
		return rec, nil
	}
	if blob, err := os.ReadFile(recordPath(uid)); err == nil {
		if len(blob) == 0 {
			return nil, fmt.Errorf("cloud save %q is empty", uid)
		}
		rec := &toyohrpb.SaveRecord{}
		if err := proto.Unmarshal(blob, rec); err != nil {
			return nil, fmt.Errorf("cloud save %q is not valid protobuf: %w", uid, err)
		}
		if rec.GetSaveData() == nil {
			return nil, fmt.Errorf("cloud save %q has no save data", uid)
		}
		if ensureSaveIdentifier(uid, rec) {
			rec.UpdateTime = timestamppb.New(time.Now().UTC())
			if err := s.persistLocked(uid, rec); err != nil {
				return nil, err
			}
		}
		s.mem[uid] = rec
		return rec, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read cloud save %q: %w", uid, err)
	}

	// ⚠️ Amorcer depuis la capture, c'est donner au joueur la sauvegarde de QUELQU'UN D'AUTRE :
	// captured_boot/toyohr.v1.CloudSave.GetSaveRecord.grpc est le record du compte
	// u-exemple5000000000000, pseudo « gen », 88 cles, Money=2658, PlayerRank=1, Identifier=7724.
	// C'est l'origine directe de l'incident « un nouveau joueur a toujours l'identite gen » :
	// load() est appele par apply() (WriteSaveRecord) et setString() (ChangeUserName), donc le
	// PREMIER delta d'un joueur sans record materialisait le record de gen sous son uid, et au
	// lancement suivant has(uid) etait vrai. Traces sur le VPS :
	// /opt/npln/saves/amorcees-depuis-gen-20260811-200429/ (15 424 et 15 419 o, la taille du blob
	// de gen). Cette amorce est desormais reservee au diagnostic, derriere NPLN_SAVE_NEW_PLAYER,
	// comme les autres chemins qui fabriquent une sauvegarde a la place du jeu. Par defaut un
	// joueur sans record part de RIEN — c'est ce que fait le serveur de Nintendo.
	rec := &toyohrpb.SaveRecord{}
	if seedFromCapture() {
		if data, err := capturedBoot.ReadFile("captured_boot/toyohr.v1.CloudSave.GetSaveRecord.grpc"); err == nil && len(data) > 5 {
			if err := proto.Unmarshal(data[5:], rec); err != nil {
				log.Printf("[NPLN CloudSave] amorce illisible: %v", err)
			}
		}
		log.Printf("[NPLN CloudSave] ⚠️ uid=%s amorce depuis la capture de « gen » (NPLN_SAVE_NEW_PLAYER=capture)", uid)
	} else {
		log.Printf("[NPLN CloudSave] uid=%s : aucun record, on part de rien (pas d'amorce)", uid)
	}
	if rec.GetSaveData() == nil {
		rec.SaveData = &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
	}
	rec.Name = nplnTenant + "/saveRecords/" + uid
	// The seed belongs to one captured player. Give each recipient a stable discriminator.
	if seedFromCapture() {
		rec.SaveData.Fields["Identifier"] = &commonpb.Value{
			ValueType: &commonpb.Value_StringValue{StringValue: identifierFor(uid)},
		}
	}
	ensureSaveIdentifier(uid, rec)
	if rec.CreateTime == nil {
		rec.CreateTime = timestamppb.Now()
	}
	rec.UpdateTime = timestamppb.Now()
	return rec, nil
}

func (s *saveRecordStore) load(uid string) (*toyohrpb.SaveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked(uid)
}

func (s *saveRecordStore) persistLocked(uid string, rec *toyohrpb.SaveRecord) error {
	if err := os.MkdirAll(saveDir(), 0o700); err != nil {
		return fmt.Errorf("create cloud save directory: %w", err)
	}
	blob, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal cloud save %q: %w", uid, err)
	}
	tmp := recordPath(uid) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open cloud save temp file: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("protect cloud save temp file: %w", err)
	}
	if _, err := f.Write(blob); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write cloud save temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync cloud save temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close cloud save temp file: %w", err)
	}
	if err := os.Rename(tmp, recordPath(uid)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace cloud save %q: %w", uid, err)
	}
	return nil
}

// apply merges a delta and returns the new update_time the client must echo back next time.
func (s *saveRecordStore) apply(uid string, delta *commonpb.MapValue) (*timestamppb.Timestamp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, err := s.loadLocked(uid)
	if err != nil {
		return nil, err
	}
	rec.SaveData = mergeSaveData(rec.GetSaveData(), delta)
	ensureSaveIdentifier(uid, rec)
	// Must move FORWARD on every write: the client feeds this value back as the next request's
	// update_time. The old canned ack replayed a constant (2026-06-28T11:48:18.444778Z), so the
	// client's idea of the cloud version never advanced.
	rec.UpdateTime = timestamppb.New(time.Now().UTC())
	if err := s.persistLocked(uid, rec); err != nil {
		return nil, err
	}
	s.mem[uid] = rec
	return rec.UpdateTime, nil
}

// snapshot returns a DEEP COPY of the player's record, safe to marshal on the gRPC goroutine.
//
// load() hands back the live pointer that apply()/setString() mutate under s.mu. Marshalling it
// outside the lock (SendMsg on the GetSaveRecord path) races a concurrent WriteSaveRecord: Go
// panics on "concurrent map read and map write", and this server installs NO panic-recovery
// interceptor, so that one panic takes the whole npln process down — hall, matchmaking, auth.
func (s *saveRecordStore) snapshot(uid string) (*toyohrpb.SaveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.loadLocked(uid)
	if err != nil {
		return nil, err
	}
	return proto.Clone(rec).(*toyohrpb.SaveRecord), nil
}

// snapshotForLogin returns the account's save. A save below level 10 is eligible for the starter
// floors once; level 10+ saves are left alone. The sidecar marker prevents later logins from
// restoring funds/licenses that the player has spent. New saves receive the floors in create().
func (s *saveRecordStore) snapshotForLogin(uid string) (*toyohrpb.SaveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Existing records predate the grant marker. Preserve level 10+ records; a lower-level save is
	// eligible for one grant below. New saves receive floors in create(), while a newly materialized
	// record receives them below.
	hadRecord, err := s.hasRecordLocked(uid)
	if err != nil {
		return nil, err
	}

	rec, err := s.loadLocked(uid)
	if err != nil {
		return nil, err
	}
	penaltyCleared := clearOnlinePlayPenalty(rec)
	granted, err := s.starterGrantAppliedLocked(uid)
	if err != nil {
		return nil, err
	}
	changed := penaltyCleared
	if !granted {
		if !hadRecord || rec.GetSaveData().GetFields()["PlayerRank"].GetIntegerValue() < 10 {
			changed = ensureSaveProgressionMinimums(rec) || changed
		}
	}
	if changed {
		rec.UpdateTime = timestamppb.New(time.Now().UTC())
		if err := s.persistLocked(uid, rec); err != nil {
			return nil, err
		}
		s.mem[uid] = rec
	}
	if !granted {
		if err := s.markStarterGrantAppliedLocked(uid); err != nil {
			return nil, err
		}
		return proto.Clone(rec).(*toyohrpb.SaveRecord), nil
	}
	return proto.Clone(rec).(*toyohrpb.SaveRecord), nil
}

// clearOnlinePlayPenalty removes the server-issued disconnect penalty from the cloud save.
// It runs on every login response, independently of the one-time starter grant marker, so an
// ApplyRedCard delta from a previous session does not survive the next game launch.
func clearOnlinePlayPenalty(rec *toyohrpb.SaveRecord) bool {
	if rec == nil || rec.GetSaveData() == nil {
		return false
	}
	if _, exists := rec.GetSaveData().GetFields()["OnlinePlayPenalty"]; !exists {
		return false
	}
	delete(rec.SaveData.Fields, "OnlinePlayPenalty")
	return true
}

// setString writes one top-level string field (used by ChangeUserName).
func (s *saveRecordStore) setString(uid, key, val string) (*toyohrpb.SaveRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, err := s.loadLocked(uid)
	if err != nil {
		return nil, err
	}

	if rec.SaveData == nil {
		rec.SaveData = &commonpb.MapValue{}
	}
	if rec.SaveData.Fields == nil {
		rec.SaveData.Fields = map[string]*commonpb.Value{}
	}
	rec.SaveData.Fields[key] = &commonpb.Value{ValueType: &commonpb.Value_StringValue{StringValue: val}}
	rec.SaveData.Fields["ChangeNameTimestamp"] = &commonpb.Value{
		ValueType: &commonpb.Value_IntegerValue{IntegerValue: time.Now().Unix()},
	}
	rec.UpdateTime = timestamppb.New(time.Now().UTC())
	if err := s.persistLocked(uid, rec); err != nil {
		return nil, err
	}
	if err := s.markStarterGrantAppliedLocked(uid); err != nil {
		return nil, err
	}
	s.mem[uid] = rec
	return proto.Clone(rec).(*toyohrpb.SaveRecord), nil
}

func saveIdentifier(rec *toyohrpb.SaveRecord) string {
	if v, ok := rec.GetSaveData().GetFields()["Identifier"]; ok {
		return v.GetStringValue()
	}
	return ""
}

// deltaKeys lists the top-level keys a delta touches, for the log line.
func deltaKeys(m *commonpb.MapValue) []string {
	out := make([]string, 0, len(m.GetFields()))
	for k := range m.GetFields() {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// identifierFor derives the stable 4-digit discriminator shown after the pseudo, so two players
// seeded from the same capture do not both end up as "#7724".
func identifierFor(uid string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(uid))
	return fmt.Sprintf("%04d", h.Sum32()%10000)
}

// ensureSaveIdentifier makes sure every cloud record has the four-digit tag the game expects.
// Preserve a non-empty tag supplied by the client; fill only missing/empty values so the result
// stays stable across reads, renames, and save deltas.
func ensureSaveIdentifier(uid string, rec *toyohrpb.SaveRecord) bool {
	if rec == nil {
		return false
	}
	if rec.SaveData == nil {
		rec.SaveData = &commonpb.MapValue{}
	}
	if rec.SaveData.Fields == nil {
		rec.SaveData.Fields = map[string]*commonpb.Value{}
	}
	if current, ok := rec.SaveData.Fields["Identifier"]; ok && current.GetStringValue() != "" {
		return false
	}
	rec.SaveData.Fields["Identifier"] = &commonpb.Value{
		ValueType: &commonpb.Value_StringValue{StringValue: identifierFor(uid)},
	}
	return true
}

// ensureSaveProgressionMinimums applies the starter floors to a save at its one-time grant point.
// Values already above the configured minimums are preserved.
func ensureSaveProgressionMinimums(rec *toyohrpb.SaveRecord) bool {
	if rec == nil {
		return false
	}
	if rec.SaveData == nil {
		rec.SaveData = &commonpb.MapValue{}
	}
	if rec.SaveData.Fields == nil {
		rec.SaveData.Fields = map[string]*commonpb.Value{}
	}

	changed := false
	for key, minimum := range map[string]int64{
		"PlayerRank":    9,
		"Money":         50000,
		"WeaponLicense": 10,
	} {
		current, ok := rec.SaveData.Fields[key]
		if ok && current.GetIntegerValue() >= minimum {
			continue
		}
		rec.SaveData.Fields[key] = &commonpb.Value{
			ValueType: &commonpb.Value_IntegerValue{IntegerValue: minimum},
		}
		changed = true
	}
	return changed
}

// create stores a record the client built itself (CreateSaveRecord). This is the clean path for a
// brand-new player: the client uploads its LOCAL save, pseudo included, instead of inheriting the
// capture's. Reachable only when GetSaveRecord answers NOT_FOUND (NPLN_SAVE_NEW_PLAYER=notfound).
func (s *saveRecordStore) create(uid string, rec *toyohrpb.SaveRecord) (*toyohrpb.SaveRecord, error) {
	if rec == nil {
		rec = &toyohrpb.SaveRecord{}
	}
	if rec.SaveData == nil {
		rec.SaveData = &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	granted, err := s.starterGrantAppliedLocked(uid)
	if err != nil {
		return nil, err
	}
	hadRecord, err := s.hasRecordLocked(uid)
	if err != nil {
		return nil, err
	}
	if !granted && !hadRecord {
		ensureSaveProgressionMinimums(rec)
	}
	ensureSaveIdentifier(uid, rec)
	rec.Name = nplnTenant + "/saveRecords/" + uid
	if rec.CreateTime == nil {
		rec.CreateTime = timestamppb.Now()
	}
	rec.UpdateTime = timestamppb.New(time.Now().UTC())
	if err := s.persistLocked(uid, rec); err != nil {
		return nil, err
	}
	if !granted {
		if err := s.markStarterGrantAppliedLocked(uid); err != nil {
			return nil, err
		}
	}
	s.mem[uid] = rec
	return proto.Clone(rec).(*toyohrpb.SaveRecord), nil
}

// has reports whether this player already has a stored record (no capture seeding).
func (s *saveRecordStore) has(uid string) bool {
	s.mu.Lock()
	_, ok := s.mem[uid]
	s.mu.Unlock()
	if ok {
		return true
	}
	st, err := os.Stat(recordPath(uid))
	return err == nil && st.Size() > 0
}

// seedFromCapture reports whether an unknown player inherits the captured record. Default yes
// (that is what boots today). Set NPLN_SAVE_NEW_PLAYER=notfound to answer NOT_FOUND instead and
// let the client push its own save through CreateSaveRecord.
// seedFromCapture : amorcer la sauvegarde d'un nouveau joueur depuis la CAPTURE.
//
// Par defaut : NON. C'est la capture d'un AUTRE compte (u-exemple5000000000000) — pseudo "gen",
// mais aussi son niveau, son argent, son equipement, ses rangs, tout. L'amorcer revenait a donner
// a chaque nouveau joueur la progression de quelqu'un d'autre, puis a la PERSISTER dans son propre
// fichier : c'est ce qui est arrive a tout le monde, y compris au compte principal.
//
// Le chemin propre existe et est deja implemente (replay.go, GetSaveRecord) : repondre NOT_FOUND a
// un joueur inconnu fait televerser au client SA sauvegarde locale via CreateSaveRecord — son vrai
// pseudo, son vrai niveau, sa vraie progression.
//
// NPLN_SAVE_NEW_PLAYER=capture reactive l'amorcage pour du diagnostic.
func seedFromCapture() bool { return os.Getenv("NPLN_SAVE_NEW_PLAYER") == "capture" }

var rawSaveDataVierge = capture("captured/SaveDataVierge.bin")

// creerRecordJoueurNeuf fabrique la sauvegarde cloud d'un joueur qui n'en a pas encore, a partir
// de celle qu'un VRAI compte neuf a televersee chez Nintendo.
//
// POURQUOI ELLE EXISTE. Trois comportements ont ete essayes pour un joueur sans record, et mesures :
//
//	NOT_FOUND        -> le jeu ne cree rien, part en boucle d'erreur de communication ;
//	succes vide      -> le jeu s'arrete net : ni CreateSaveRecord, ni FestSchedule, ni documents,
//	                    et l'ecran Stages affiche « informations non disponibles hors ligne » ;
//	record de gen    -> tout repart (FestSchedule, GetDocument, WriteSaveRecord apparaissent enfin),
//	                    mais le joueur herite du pseudo ET de la progression d'un autre compte.
//
// Le troisieme essai a prouve ce que le jeu attend : une sauvegarde cloud RESOLUE. Le premier et le
// second prouvent qu'il ne la creera pas de lui-meme dans nos conditions. On la lui fournit donc,
// mais VIERGE — pas celle d'un inconnu.
//
// D'ou vient cette base : de la capture du 2026-08-12 sur un compte Nintendo qui n'avait jamais
// lance le jeu (une capture de compte neuf*). C'est le CreateSaveRecord que le jeu
// lui-meme a televerse, 15012 octets, 373 cles, tous les compteurs a zero. Rien d'invente : c'est
// exactement ce qu'un joueur neuf possede.
func creerRecordJoueurNeuf(uid, pseudo string) *toyohrpb.SaveRecord {
	donnees := &commonpb.MapValue{}
	if err := proto.Unmarshal(rawSaveDataVierge, donnees); err != nil {
		log.Printf("[NPLN CloudSave] base joueur neuf illisible (%v) — record vide", err)
		donnees = &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
	}

	// Le pseudo de la capture appartient au compte qui l'a produite : on met celui du joueur des
	// qu'on le connait. Sinon le jeu le corrigera lui-meme au premier ChangeUserName.
	if pseudo != "" && donnees.GetFields() != nil {
		donnees.Fields["UserName"] = &commonpb.Value{
			ValueType: &commonpb.Value_StringValue{StringValue: pseudo},
		}
	}
	rec := &toyohrpb.SaveRecord{
		Name:       nplnTenant + "/saveRecords/" + uid,
		SaveData:   donnees,
		CreateTime: timestamppb.Now(),
		UpdateTime: timestamppb.Now(),
	}
	ensureSaveProgressionMinimums(rec)
	ensureSaveIdentifier(uid, rec)

	recordStore.mu.Lock()
	if err := recordStore.persistLocked(uid, rec); err != nil {
		log.Printf("[NPLN CloudSave] %s: cannot persist new-player record: %v", uid, err)
	} else {
		if err := recordStore.markStarterGrantAppliedLocked(uid); err != nil {
			log.Printf("[NPLN CloudSave] %s: cannot persist starter-grant marker: %v", uid, err)
		} else {
			recordStore.mem[uid] = rec
		}
	}
	recordStore.mu.Unlock()

	log.Printf("[NPLN CloudSave] %s n'avait pas de sauvegarde cloud -> record de joueur neuf cree (%d cles, pseudo=%q)",
		uid, len(donnees.GetFields()), pseudo)
	return rec
}
