package main

// save_api — exposer la sauvegarde cloud Splatoon 3 au site Nextendo.
//
// Le site liste les sauvegardes depuis nextendo-account, qui lit des fichiers
// saveDir/<pid>/<titleId>.bin. Le record S3, lui, vit ICI : /data/saves/<uid>.record.pb, au format
// protobuf SaveRecord, et il n'est pas produit par le meme chemin (c'est le service CloudSave de
// NPLN qui l'ecrit, pas l'upload olsc/scsi). Sans pont, l'espace perso ne peut pas le montrer.
//
// On expose donc le record par PID — la meme cle que le site emploie — avec sa taille, sa date, et
// les quelques champs que le joueur reconnait (pseudo, niveau, argent). Ces trois champs sont lus
// dans le record REEL, jamais devines : leurs noms viennent de la capture (UserName, PlayerRank,
// Money) et sont ceux que la fusion reproduit a zero difference.

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

// s3TitleID est le titleId de Splatoon 3, tel que le site nomme ses sauvegardes.
const s3TitleID = "0100c2500fc20000"

type saveRecordView struct {
	Exists    bool   `json:"exists"`
	TitleID   string `json:"titleId"`
	Size      int64  `json:"size"`
	UpdatedAt string `json:"updatedAt,omitempty"`
	UserName  string `json:"userName,omitempty"`
	Level     int64  `json:"level,omitempty"`
	Money     int64  `json:"money,omitempty"`
	Keys      int    `json:"keys,omitempty"`
}

func intField(m *commonpb.MapValue, key string) int64 {
	if v, ok := m.GetFields()[key]; ok {
		return v.GetIntegerValue()
	}
	return 0
}

func strField(m *commonpb.MapValue, key string) string {
	if v, ok := m.GetFields()[key]; ok {
		return v.GetStringValue()
	}
	return ""
}

// handleSaveRecord : GET /api/save-record?pid=<pid> — l'etat de la sauvegarde S3 d'un compte.
// Consomme par nextendo-account pour l'espace perso du site (serveur a serveur).
func handleSaveRecord(w http.ResponseWriter, r *http.Request) {
	pid, err := strconv.ParseUint(r.URL.Query().Get("pid"), 10, 64)
	if err != nil || pid == 0 {
		http.Error(w, "bad pid", http.StatusBadRequest)
		return
	}

	// Le PID se resout en uid NPLN par le service de comptes : c'est la meme correspondance que
	// celle qui decide du proprietaire d'une sauvegarde, donc les deux ne peuvent pas diverger.
	acc, err := accountFriends(pid)
	if err != nil || acc.UserID == "" {
		// Les deux echecs renvoyaient la meme reponse : impossible de savoir lequel s'etait produit.
		log.Printf("[NPLN save-api] pid=%d : identite irresolvable (err=%v, uid=%q)", pid, err, safeUID(acc))
		writeSaveJSON(w, saveRecordView{TitleID: s3TitleID})
		return
	}

	path := recordPath(acc.UserID)
	info, serr := os.Stat(path)
	if serr != nil {
		// Pas de journal ici : l'immense majorite des comptes n'a jamais joue a Splatoon 3, et le
		// site interroge cet endpoint a chaque ouverture d'espace perso (70 appels en 10 min
		// mesures). Une ligne par compte noyait le debogage du jeu sans rien apprendre. Seul
		// l'echec d'identite, lui anormal, reste journalise.
		writeSaveJSON(w, saveRecordView{TitleID: s3TitleID})
		return
	}

	view := saveRecordView{
		Exists:    true,
		TitleID:   s3TitleID,
		Size:      info.Size(),
		UpdatedAt: info.ModTime().UTC().Format("2006-01-02T15:04:05Z"),
	}

	if rec := recordStore.snapshot(acc.UserID); rec != nil {
		d := rec.GetSaveData()
		view.Keys = len(d.GetFields())
		view.UserName = strField(d, "UserName")
		view.Level = intField(d, "PlayerRank")
		view.Money = intField(d, "Money")
	}

	writeSaveJSON(w, view)
}

func writeSaveJSON(w http.ResponseWriter, v saveRecordView) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// safeUID evite un dereferencement nul dans le journal quand la resolution a echoue.
func safeUID(a *nplnAccountData) string {
	if a == nil {
		return ""
	}
	return a.UserID
}
