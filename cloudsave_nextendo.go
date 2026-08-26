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

// load returns the player's record, seeding it from the capture the first time.
// The seed is the capture of ANOTHER account (u-exemple5000000000000, UserName "gen"), so its
// name/identity fields are rewritten onto the caller before anything is persisted.
func (s *saveRecordStore) load(uid string) *toyohrpb.SaveRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, ok := s.mem[uid]; ok {
		return rec
	}
	if blob, err := os.ReadFile(recordPath(uid)); err == nil && len(blob) > 0 {
		rec := &toyohrpb.SaveRecord{}
		if proto.Unmarshal(blob, rec) == nil && rec.GetSaveData() != nil {
			s.mem[uid] = rec
			return rec
		}
		log.Printf("[NPLN CloudSave] %s illisible, re-amorcage depuis la capture", recordPath(uid))
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
	// The seed is one specific player's record; its 4-digit discriminator ("#7724", shown next to
	// the pseudo) would otherwise be identical for everyone we seed. Derive it from the uid.
	if rec.SaveData.Fields != nil {
		if _, ok := rec.SaveData.Fields["Identifier"]; ok {
			rec.SaveData.Fields["Identifier"] = &commonpb.Value{
				ValueType: &commonpb.Value_StringValue{StringValue: identifierFor(uid)},
			}
		}
	}
	if rec.CreateTime == nil {
		rec.CreateTime = timestamppb.Now()
	}
	rec.UpdateTime = timestamppb.Now()

	s.mem[uid] = rec
	s.persistLocked(uid, rec)
	return rec
}

func (s *saveRecordStore) persistLocked(uid string, rec *toyohrpb.SaveRecord) {
	if err := os.MkdirAll(saveDir(), 0o755); err != nil {
		log.Printf("[NPLN CloudSave] mkdir %s: %v", saveDir(), err)
		return
	}
	blob, err := proto.Marshal(rec)
	if err != nil {
		log.Printf("[NPLN CloudSave] marshal %s: %v", uid, err)
		return
	}
	tmp := recordPath(uid) + ".tmp"
	if err := os.WriteFile(tmp, blob, 0o644); err != nil {
		log.Printf("[NPLN CloudSave] ecriture %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, recordPath(uid)); err != nil {
		log.Printf("[NPLN CloudSave] rename %s: %v", recordPath(uid), err)
	}
}

// apply merges a delta and returns the new update_time the client must echo back next time.
func (s *saveRecordStore) apply(uid string, delta *commonpb.MapValue) *timestamppb.Timestamp {
	rec := s.load(uid)

	s.mu.Lock()
	defer s.mu.Unlock()

	rec.SaveData = mergeSaveData(rec.GetSaveData(), delta)
	// Must move FORWARD on every write: the client feeds this value back as the next request's
	// update_time. The old canned ack replayed a constant (2026-06-28T11:48:18.444778Z), so the
	// client's idea of the cloud version never advanced.
	rec.UpdateTime = timestamppb.New(time.Now().UTC())
	s.persistLocked(uid, rec)
	return rec.UpdateTime
}

// snapshot returns a DEEP COPY of the player's record, safe to marshal on the gRPC goroutine.
//
// load() hands back the live pointer that apply()/setString() mutate under s.mu. Marshalling it
// outside the lock (SendMsg on the GetSaveRecord path) races a concurrent WriteSaveRecord: Go
// panics on "concurrent map read and map write", and this server installs NO panic-recovery
// interceptor, so that one panic takes the whole npln process down — hall, matchmaking, auth.
func (s *saveRecordStore) snapshot(uid string) *toyohrpb.SaveRecord {
	rec := s.load(uid)
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(rec).(*toyohrpb.SaveRecord)
}

// setString writes one top-level string field (used by ChangeUserName).
func (s *saveRecordStore) setString(uid, key, val string) *toyohrpb.SaveRecord {
	rec := s.load(uid)

	s.mu.Lock()
	defer s.mu.Unlock()

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
	s.persistLocked(uid, rec)
	return proto.Clone(rec).(*toyohrpb.SaveRecord)
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

// create stores a record the client built itself (CreateSaveRecord). This is the clean path for a
// brand-new player: the client uploads its LOCAL save, pseudo included, instead of inheriting the
// capture's. Reachable only when GetSaveRecord answers NOT_FOUND (NPLN_SAVE_NEW_PLAYER=notfound).
func (s *saveRecordStore) create(uid string, rec *toyohrpb.SaveRecord) *toyohrpb.SaveRecord {
	if rec == nil {
		rec = &toyohrpb.SaveRecord{}
	}
	if rec.SaveData == nil {
		rec.SaveData = &commonpb.MapValue{Fields: map[string]*commonpb.Value{}}
	}
	rec.Name = nplnTenant + "/saveRecords/" + uid
	if rec.CreateTime == nil {
		rec.CreateTime = timestamppb.Now()
	}
	rec.UpdateTime = timestamppb.New(time.Now().UTC())

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem[uid] = rec
	s.persistLocked(uid, rec)
	return rec
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

	recordStore.mu.Lock()
	recordStore.mem[uid] = rec
	recordStore.persistLocked(uid, rec)
	recordStore.mu.Unlock()

	log.Printf("[NPLN CloudSave] %s n'avait pas de sauvegarde cloud -> record de joueur neuf cree (%d cles, pseudo=%q)",
		uid, len(donnees.GetFields()), pseudo)
	return rec
}
