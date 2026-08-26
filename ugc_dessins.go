package main

// ugc_dessins — publication des dessins de la place (boîte aux lettres de Cité-Clabousse).
//
// Mesure du 2026-08-15, capture d'une vraie console publiant un dessin (capture-dessin/) : l'image
// ne transite JAMAIS par NPLN. La publication se fait en quatre temps :
//
//	1. Ugcstore/IssueUploadUri   requête {1:"tenants/current"} -> une URL de dépôt SIGNÉE
//	2. PUT de l'image sur cette URL (chez Nintendo : storage.googleapis.com, en direct)
//	3. Ugcstore/CommitDocuments  le document du dessin :
//	     tenants/current/documents/services/toyohr/interfaces/canola/picturePosts/<uuid>
//	     Picture { IsHidden, Score, Data (~531 o), FestRegion }
//	4. Canola/RegisterDocument   requête = le NOM du document, réponse VIDE -> publié
//
// C'est la même architecture que les sauvegardes cloud : le serveur délivre une permission de
// dépôt, le stockage est ailleurs. Nous pointons donc vers l'API de comptes Nextendo, qui tient
// déjà le quota (5 Mo, 10 Mo pour les boosters), la porte Discord et l'espace personnel — les
// dessins comptent dans le même total plutôt que dans un stockage parallèle.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"

	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ugcBaseURL est l'hôte PUBLIC vers lequel la console dépose l'image. Il doit être joignable depuis
// la console et présenter un certificat qu'elle accepte — contrairement à accountBaseURL, qui est
// l'adresse interne entre conteneurs.
var ugcBaseURL = envOr("NEXTENDO_UGC_BASE", "https://nextendo.network")

// dureeDepot : validité d'une permission de dépôt. Nintendo signe ses URL pour environ une heure.
const dureeDepot = time.Hour

// jetonDepot fabrique la permission de dépôt : <pid>.<expiration>.<signature>.
//
// Signée avec le secret partagé que l'API de comptes connaît déjà, elle prouve QUI dépose sans
// qu'aucun état ne soit partagé entre les deux services. L'API n'a qu'à vérifier la signature pour
// imputer l'image au bon compte, et donc au bon quota.
func jetonDepot(pid uint64) string {
	exp := time.Now().Add(dureeDepot).Unix()
	corps := fmt.Sprintf("%d.%d", pid, exp)

	mac := hmac.New(sha256.New, loadNextendoSecret())
	mac.Write([]byte(corps))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return corps + "." + sig
}

// uriDeDepot rend l'URL que la console utilisera pour téléverser son dessin.
func uriDeDepot(pid uint64) string {
	return fmt.Sprintf("%s/api/ugc/depot?jeton=%s", strings.TrimRight(ugcBaseURL, "/"), jetonDepot(pid))
}

// --- réponses protobuf, encodées à la main ---------------------------------------------------
//
// Ces trois messages ne figurent pas dans nos .proto générés (le service Ugcstore n'est pas
// enregistré : l'enregistrer capturerait aussi GetDocument et RunQuery, qui fonctionnent
// aujourd'hui par rejeu). On encode donc les réponses directement, à la forme mesurée.

func varintProto(champ int, v uint64) []byte {
	out := metVarintUgc(uint64(champ)<<3 | 0)
	return append(out, metVarintUgc(v)...)
}

func chaineProto(champ int, s string) []byte {
	out := metVarintUgc(uint64(champ)<<3 | 2)
	out = append(out, metVarintUgc(uint64(len(s)))...)
	return append(out, s...)
}

func messageProto(champ int, corps []byte) []byte {
	out := metVarintUgc(uint64(champ)<<3 | 2)
	out = append(out, metVarintUgc(uint64(len(corps)))...)
	return append(out, corps...)
}

func metVarintUgc(v uint64) []byte {
	var out []byte
	for v >= 0x80 {
		out = append(out, byte(v)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

// horodatageProto encode un google.protobuf.Timestamp {1: seconds, 2: nanos}.
func horodatageProto() []byte {
	t := timestamppb.Now()
	corps := varintProto(1, uint64(t.GetSeconds()))
	return append(corps, varintProto(2, uint64(uint32(t.GetNanos())))...)
}

// reponseIssueUploadUri : {1: "<url>"} — un seul champ, mesuré sur la capture (580 o d'URL signée).
func reponseIssueUploadUri(pid uint64) []byte {
	return chaineProto(1, uriDeDepot(pid))
}

// reponseCommitDocuments : un résultat par document écrit, chacun portant l'horodatage du dépôt.
// Forme mesurée : deux entrées de champ 1, la seconde portant en plus un champ 2 imbriqué.
func reponseCommitDocuments(nbDocs int) []byte {
	if nbDocs < 1 {
		nbDocs = 1
	}
	var out []byte
	for i := 0; i < nbDocs; i++ {
		resultat := messageProto(1, horodatageProto())
		out = append(out, messageProto(1, resultat)...)
	}
	return out
}

// --- registre des dessins publiés ------------------------------------------------------------

type dessinPublie struct {
	nom    string // tenants/…/canola/picturePosts/<uuid>
	uid    string
	quand  time.Time
	octets []byte // le document tel que le jeu l'a écrit (métadonnées, pas l'image)
}

var dessins = struct {
	mu sync.Mutex
	m  map[string]*dessinPublie
}{m: map[string]*dessinPublie{}}

func retenirDessin(nom, uid string, doc []byte) {
	if nom == "" {
		return
	}
	dessins.mu.Lock()
	dessins.m[nom] = &dessinPublie{nom: nom, uid: uid, quand: time.Now(), octets: append([]byte(nil), doc...)}
	n := len(dessins.m)
	dessins.mu.Unlock()
	log.Printf("[NPLN UGC] dessin retenu %q (uid=%s) — %d dessin(s) connus", nom, uid, n)
}

// nomDocumentDansCommit extrait le nom du document d'une requête CommitDocuments, sans décoder tout
// le message : le nom est la première chaîne qui ressemble à un chemin de document.
func nomDocumentDansCommit(req []byte) string {
	for i := 0; i+2 < len(req); i++ {
		if req[i] != 0x0a && req[i] != 0x12 { // champ 1 ou 2, wire type 2
			continue
		}
		n, j, ok := litVarintUgc(req, i+1)
		if !ok || n < 20 || j+int(n) > len(req) {
			continue
		}
		s := string(req[j : j+int(n)])
		if strings.Contains(s, "/documents/services/") && strings.Contains(s, "/picturePosts/") {
			return s
		}
	}
	return ""
}

func litVarintUgc(b []byte, i int) (uint64, int, bool) {
	var v uint64
	var d uint
	for i < len(b) {
		c := b[i]
		i++
		v |= uint64(c&0x7f) << d
		if c&0x80 == 0 {
			return v, i, true
		}
		d += 7
		if d > 63 {
			return 0, i, false
		}
	}
	return 0, i, false
}
