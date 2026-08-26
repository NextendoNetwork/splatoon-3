package main

// Minimal ugcstore (nn.npln.ugcstore.v1.Ugcstore). MP Jamboree and Splatoon 3 read
// and write a per-user save document here right after auth. We answer GetDocument
// with NotFound (a fresh user with no saved document) and accept CommitDocuments.
//
// Registering this service is REQUIRED: without it the generic capture-replay
// (UnknownServiceHandler) answered GetDocument by replaying a Splatoon-3-tenant
// document. MP Jamboree tried to deserialize that foreign document and crashed with
// a null-continuation in its nn.npln.Worker (Invalid memory access at 0x0). Returning
// a clean NotFound lets the game create a fresh profile and proceed, exactly like the
// first working run.

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	common "npln.nintendo.net/npln-practice/proto/common"
	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

var rawPointCardTotal = capture("captured/PointCardTotal.bin")

var rawCoopUserAttribute = capture("captured/CoopUserAttribute.bin")

type ugcstoreServer struct {
	ugcpb.UnimplementedUgcstoreServer
}

// [Nextendo] Real per-document storage. CommitDocuments used to acknowledge every write and throw
// it away, so a client that wrote a document and read it back never saw its own data. Splatoon 3
// depends on that round-trip at login: it reads its profile, finds nothing, calls
// GameRecord/InitializeAttributes to create it, reads again -- and with a throwaway store it reads
// nothing again, forever. Measured: 1198 GetDocument + 599 InitializeAttributes in two minutes,
// which is the "infinite ink screen". Keeping what the game writes breaks that loop.
// In-memory on purpose for now: it survives a session, not a server restart. Persisting to disk is
// the follow-up once the login path is settled.
var documentStore = struct {
	mu   sync.RWMutex
	docs map[string]*ugcpb.Document
}{docs: map[string]*ugcpb.Document{}}

func storeGetDocument(name string) (*ugcpb.Document, bool) {
	documentStore.mu.RLock()
	doc, ok := documentStore.docs[name]
	documentStore.mu.RUnlock()
	if ok {
		return doc, true
	}

	// REPLI SUR LE DISQUE — indispensable depuis que DEUX PROCESSUS ecrivent ici.
	//
	// L'apparieur (/data/nplns3, conteneur npln) et l'arbitre (/root/nplns3-gamesync, systemd)
	// partagent le meme dossier de documents : le conteneur monte /opt/npln sur /data. Mais chacun
	// ne charge le disque QU'AU DEMARRAGE. Un document ecrit par l'arbitre — la carte de points d'un
	// quart de Salmon Run, par exemple — restait donc invisible au processus qui sert GetDocument,
	// jusqu'au prochain redemarrage. Le joueur finissait son quart et son compteur ne bougeait pas.
	//
	// On relit donc le fichier quand la memoire ne connait pas le nom, et on le garde.
	b, err := os.ReadFile(documentPath(name))
	if err != nil {
		return nil, false
	}
	relu := &ugcpb.Document{}
	if proto.Unmarshal(b, relu) != nil || relu.GetName() == "" {
		return nil, false
	}
	documentStore.mu.Lock()
	documentStore.docs[name] = relu
	documentStore.mu.Unlock()
	return relu, true
}

func storePutDocument(doc *ugcpb.Document) {
	if doc.GetName() == "" {
		return
	}

	// ⚠️ UN NOM QUI NE SE PARSE PAS TUE LE JEU, ET IL SURVIT AU REDEMARRAGE.
	//
	// Le SDK ugcstore exige la grammaire « tenants/<t>/documents/<reste> » (Thunder.nss:0x667f90)
	// et s'abat sur tout le reste : nn::diag::detail::AbortImpl, 2162-0001 dans nn.npln.Worker.
	// Le 2026-08-23 un handler a range ici un document nomme « tenants/current/users/current »,
	// tire du champ parent d'une requete. Le document a ete ecrit sur le disque, donc relu a chaque
	// demarrage, et le jeu a continue de mourir longtemps apres que le code fautif ait ete
	// debranche. Un nom invalide n'entre plus dans le magasin.
	if !strings.Contains(doc.GetName(), "/documents/") {
		log.Printf("[NPLN ugcstore] REFUS d'enregistrer %q : un nom de document doit contenir /documents/",
			doc.GetName())
		return
	}

	documentStore.mu.Lock()
	documentStore.docs[doc.GetName()] = doc
	documentStore.mu.Unlock()

	persisterDocument(doc)
}

// ---- persistance des documents ----
//
// ⚠️ CE MAGASIN ETAIT EN MEMOIRE SEULEMENT, et ca faisait planter le jeu (mesure du 2026-08-12).
//
// Splatoon 3 ecrit ses propres documents (BankaraChallenges/<id>, par exemple) puis les RELIT plus
// tard, en tenant leur existence pour acquise. Quand la relecture revenait vide, le client passait
// le nom vide au parseur de noms de ressources ugcstore, qui abandonne l'application :
// 2162-0001, thread nn.npln.Worker, Thunder.nss:0x667fe8 (le parseur exige
// « tenants/<locataire>/documents/<reste> » et refuse une chaine vide).
//
// Or le magasin ne survivait pas a un redemarrage du serveur — et il y en a eu des dizaines dans la
// journee. C'est aussi ce qui rendait les demarrages NON DETERMINISTES : selon qu'un document
// ecrit avant le dernier redemarrage etait encore la ou non, le jeu passait ou s'abattait.
//
// On persiste donc sur disque, a cote des sauvegardes cloud. Le nom du document sert de cle, rendu
// sur un seul niveau de fichier.

func documentsDir() string { return filepath.Join(filepath.Dir(saveDir()), "documents") }

func documentPath(name string) string {
	h := sha1.Sum([]byte(name))
	return filepath.Join(documentsDir(), hex.EncodeToString(h[:])+".pb")
}

func persisterDocument(doc *ugcpb.Document) {
	if err := os.MkdirAll(documentsDir(), 0o755); err != nil {
		log.Printf("[NPLN ugcstore] dossier des documents : %v", err)
		return
	}
	b, err := proto.Marshal(doc)
	if err != nil {
		log.Printf("[NPLN ugcstore] serialisation de %q : %v", doc.GetName(), err)
		return
	}
	if err := os.WriteFile(documentPath(doc.GetName()), b, 0o644); err != nil {
		log.Printf("[NPLN ugcstore] ecriture de %q : %v", doc.GetName(), err)
	}
}

// chargerDocuments relit tout ce que les joueurs ont ecrit avant le dernier redemarrage.
func chargerDocuments() {
	entrees, err := os.ReadDir(documentsDir())
	if err != nil {
		return // aucun document encore ecrit : normal au premier demarrage
	}
	n := 0
	for _, e := range entrees {
		b, err := os.ReadFile(filepath.Join(documentsDir(), e.Name()))
		if err != nil {
			continue
		}
		doc := &ugcpb.Document{}
		if proto.Unmarshal(b, doc) != nil || doc.GetName() == "" {
			continue
		}
		documentStore.docs[doc.GetName()] = doc
		n++
	}
	if n > 0 {
		log.Printf("[NPLN ugcstore] %d document(s) relus depuis le disque", n)
	}
}

// GetDocument answers from the session store. See documentStore above for why a write must
// actually be kept: the read/initialize/read round-trip is what lets S3 finish logging in.
// absentDocument est ce que le VRAI serveur repond pour un document qui n'existe pas encore.
//
// Mesure sur la capture (le corpus de captures NPLN,
// section riche avec statuts) : sur les 44 GetDocument captures, Nintendo repond TOUJOURS
// "Succeeded". Jamais une seule erreur. Les documents inexistants reviennent avec
// content-length: 0 et un corps vide — un SUCCES vide, pas un NotFound :
//
//	Hammer/PrivateMatch/Data              Succeeded  0 octet   (x6)
//	Hammer/TournamentMatch/Data           Succeeded  0 octet   (x6)
//	Hammer/CoopMatch/Data                 Succeeded  0 octet   (x6)
//	Hammer/UserAttribute/Data             Succeeded  0 octet
//	GameRecord/WeaponPowerMeasurement/16  Succeeded  0 octet   (x7)
//	GameRecord/PointCardRegular/<date>    Succeeded  0 puis 509 octets
//
// Le commentaire d'origine affirmait qu'un flux vide « c'est a quoi ressemble NotFound dans la
// capture » : c'est faux, le champ statut le dit. Et la difference compte pour le jeu — « pas
// encore de reglages » et « erreur serveur » ne se traitent pas pareil. A l'ouverture de l'ecran
// de match prive, S3 lit ses trois documents Hammer ; il recevait trois erreurs de notre part et
// s'arretait la, sans jamais atteindre l'etape de session (d'ou un ServerSession.Id vide et pas
// un seul appel gRPC sur le serveur de session).
//
// ⚠️ Gate par locataire : Mario Party Jamboree, lui, FONCTIONNE avec le NotFound (il cree un
// profil neuf et poursuit — c'est documente en tete de ce fichier). On ne change que S3.
func absentDocument(ctx context.Context, name, pourquoi string) (*ugcpb.Document, error) {
	// ⚠️ REVERT MESURE : renvoyer un Document VIDE fait CRASHER S3 (thread nn.npln.Worker,
	// PC nnSdk:0x17574c, immediatement apres notre reponse sur PointCardRegular). La capture dit
	// « Succeeded, content-length: 0 » : zero octet de corps, donc AUCUN message gRPC. Un Document
	// vide, lui, part quand meme comme un message de longueur nulle (5 octets de cadre) — le jeu le
	// lit, y cherche ses champs et deref un pointeur nul. Emuler le vrai serveur demande de
	// terminer l'appel en OK SANS message, ce qu'un handler unaire ne peut pas faire : c'est le
	// travail en cours. En attendant, on revient au NotFound, avec lequel le jeu ne crashe pas.
	log.Printf("[NPLN ugcstore] GetDocument name=%q -> NotFound (%s)", name, pourquoi)
	return nil, status.Error(codes.NotFound, "document not found")
}

func (u *ugcstoreServer) GetDocument(ctx context.Context, req *ugcpb.GetDocumentRequest) (*ugcpb.Document, error) {
	name := req.GetName()

	// Hand back exactly what this client wrote earlier in the session.
	if doc, ok := storeGetDocument(name); ok {
		log.Printf("[NPLN ugcstore] GetDocument name=%q -> stored document", name)
		return doc, nil
	}

	// [Nextendo] GameRecord documents are created by GameRecord/InitializeAttributes, which we do
	// not implement -- the generic replay answers "empty OK" and creates nothing. S3 then reads,
	// gets NotFound, calls InitializeAttributes, reads again, and spins: measured 905 reads each of
	// PointCardTotal/Data and PointCardRegular/<date> in three minutes, with zero CommitDocuments
	// (the game never writes these itself). Materialising them on first read is what
	// InitializeAttributes was supposed to do, so do it here and keep them for the session.
	// [Nextendo] The captured Switch session answers the two GameRecord reads DIFFERENTLY, and that
	// difference is the whole point: PointCardTotal/Data comes back as a 302-byte document, while
	// PointCardRegular/<date> comes back EMPTY -- an empty gRPC data stream is what NotFound looks
	// like in the capture. The real Switch then reads each exactly once and never calls
	// InitializeAttributes at all (measured across 7 captured sessions: PointCard=2, Initialize=0).
	// Serving a document for BOTH is what made S3 decide its attributes were wrong and re-initialise
	// five times a second. Per-period point cards genuinely do not exist until the period is played.
	// Not yet initialised: NotFound, exactly like the capture (its PointCardRegular response is an
	// empty stream). Once GameRecord/InitializeAttributes has created it, the store answers above.
	// PointCardRegular/<date> : absent tant que la periode n'a pas ete jouee. Meme traitement que
	// les autres documents jamais ecrits — le chemin unique ci-dessous s'en charge.
	if strings.Contains(name, "PointCardRegular/") {
		return absentDocument(ctx, name, "periode pas encore jouee")
	}

	if strings.Contains(name, "/services/GameRecord/") && soirFlag("gabarit") {
		// ⚠️ MESURE DU 2026-08-12 : CE GABARIT FAIT PLANTER LE JEU. Il est desormais eteint par
		// defaut, et ne se rallume que par le drapeau "gabarit", pour pouvoir le remesurer.
		//
		// Ce qu'il faisait : servir la carte de points Salmon Run capturee (ikura_total, kuma_point,
		// job_num, boss_total, rescue_total…) pour N'IMPORTE QUEL document GameRecord, simplement
		// renommee sur le chemin demande. Donc CoopUserAttribute/Data, VsUserAttribute/Data,
		// WeaponPowerMeasurement/16 et PointCardTotal/Data recevaient tous les MEMES huit champs.
		//
		// Ce que fait Nintendo, mesure sur la capture d'un compte neuf
		// (une capture de compte neuf) : ZERO OCTET sur 13 des 14 lectures — dont
		// CoopUserAttribute/Data et VsUserAttribute/Data. Le seul GetDocument non vide de toute la
		// session arrive APRES que GameRecord/InitializeAttributes ait cree le document.
		//
		// La consequence, observee : le jeu lit VsUserAttribute/Data en cherchant son rang et ses
		// puissances X, y trouve des compteurs de Salmon Run, et s'abat — 2162-0001 dans
		// nn.npln.Worker, 4 secondes apres cette rafale de lectures. Un document absent doit rester
		// absent : c'est ce qui pousse le jeu a appeler InitializeAttributes, comme sur console.
		doc := &ugcpb.Document{}
		if err := proto.Unmarshal(rawPointCardTotal, doc); err != nil {
			log.Printf("[NPLN ugcstore] PointCard template unmarshal error: %v", err)
			doc = &ugcpb.Document{Fields: &common.MapValue{Fields: map[string]*common.Value{}}}
		}

		now := timestamppb.Now()
		doc.Name = name
		doc.CreateTime = now
		doc.UpdateTime = now

		storePutDocument(doc)
		log.Printf("[NPLN ugcstore] GetDocument name=%q -> gabarit capture (%d champ(s)) — drapeau \"gabarit\"",
			name, len(doc.GetFields().GetFields()))

		return doc, nil
	}

	// [Nextendo 2026-08-12] UN COMPTE QUI A UNE SAUVEGARDE EXIGE SES DEUX DOCUMENTS D'ATTRIBUTS.
	//
	// La mesure, sur les deux captures Nintendo du meme jour :
	//   - une capture du hall (compte PROGRESSE, GetSaveRecord = 15393 o) : la toute premiere
	//     lecture est CoopUserAttribute/Data et Nintendo repond 436 o, puis VsUserAttribute/Data
	//     4293 o. Le jeu n'appelle JAMAIS InitializeAttributes. Les documents suivants
	//     (WeaponPowerMeasurement, PointCard*, Hammer/*) reviennent bien vides.
	//   - une capture de compte neuf (GetSaveRecord = 0 o) : ces deux memes lectures reviennent
	//     VIDES et le jeu enchaine sur GameRecord/InitializeAttributes pour creer ses documents.
	//
	// Nous, nous avons restaure la sauvegarde du joueur ET repondu vide : un etat qui n'existe pas
	// chez Nintendo. Sans message, le client construit un Document par defaut — son champ `name` est
	// vide — et le passe a l'accesseur « ce nom DOIT se parser » de son SDK ugcstore
	// (Thunder.nss:0x667f90, grammaire tenants/<t>/documents/<reste>) : chaine vide -> echec ->
	// nn::diag::detail::AbortImpl a 0x667fe4, adresse de retour 0x667fe8, 2162-0001 dans
	// nn.npln.Worker. Mesure du journal 18-55-06 : trailers de ce GetDocument vide recus a
	// 00:01:53.367, abort a 00:01:53.368, journal mort a 00:01:53.643.
	//
	// Kill-switch a chaud : `echo attrs_off >> /data/soir.flags` pour revenir au succes vide.
	if !soirFlag("attrs_off") {
		if doc := attributsDeCompte(name); doc != nil {
			storePutDocument(doc)
			log.Printf("[NPLN ugcstore] GetDocument name=%q -> attributs captures (%d champ(s))",
				doc.GetName(), len(doc.GetFields().GetFields()))
			return doc, nil
		}
	}

	// Rien de stocke : succes vide, comme le vrai serveur.
	return absentDocument(ctx, name, "jamais ecrit")
}

// attributsDeCompte rend le document d'attributs capture chez Nintendo, reidentifie sur le joueur de
// la session. Les identifiants npln font tous 22 caracteres, donc l'echange octet a octet ne derange
// aucun prefixe de longueur protobuf. Il reecrit du meme coup le `name` porte par le document, qui
// devient exactement celui que Nintendo renvoie : tenant resolu (t-dce9377b-lp1) et non l'alias
// "current" de la requete. Tout autre document reste absent, conforme a la capture du hall.
// feteDesAttributsCaptures : la fete que nommait la console le jour ou nous avons capture les
// documents d'attributs. Elle est remplacee par la fete courante a chaque service.
const feteDesAttributsCaptures = "JUEA-00201"

func attributsDeCompte(name string) *ugcpb.Document {
	if !strings.Contains(name, "/services/GameRecord/users/") {
		return nil
	}

	var brut []byte
	switch {
	case strings.HasSuffix(name, "/CoopUserAttribute/Data"):
		brut = rawCoopUserAttribute
	case strings.HasSuffix(name, "/VsUserAttribute/Data"):
		brut = rawVsUserAttribute
	default:
		return nil
	}

	uid := uidDuChemin(name)
	if uid == "" || len(uid) != len(capturedUserGameRecord) {
		log.Printf("[NPLN ugcstore] attributs : uid %q inexploitable pour %q, on laisse absent", uid, name)
		return nil
	}

	// ⚠️ LA FETE CITEE DANS LES ATTRIBUTS DOIT ETRE CELLE QU'ON ANNONCE.
	//
	// Le document capture porte un champ fest_id, et il nommait la fete du jour de la capture —
	// JUEA-00201. Le jeu lisait donc ses propres attributs lui disant qu'il appartient a une fete,
	// pendant que SelectFestSchedule lui en annoncait une autre. Il n'en sort pas : mesure du
	// 2026-08-24, 2 300 relectures de VsUserAttribute et 6 921 de WeaponPowerMeasurement en dix
	// minutes, ecran « Connexion a Internet » sans issue.
	//
	// Sur la capture d'une vraie console en festival
	// (le relais de session) les deux
	// concordent : le document dit JUEA-00107 et le serveur annonce JUEA-00107.
	//
	// Tous les identifiants de fete font dix caracteres, donc l'echange se fait octet a octet sans
	// toucher aux prefixes de longueur — meme procede que pour l'identifiant de compte.
	aligne := bytes.ReplaceAll(brut, []byte(capturedUserGameRecord), []byte(uid))
	if fete := identifiantDeFeteMaison(time.Now()); len(fete) == len(feteDesAttributsCaptures) {
		aligne = bytes.ReplaceAll(aligne, []byte(feteDesAttributsCaptures), []byte(fete))
	}

	doc := &ugcpb.Document{}
	if err := proto.Unmarshal(aligne, doc); err != nil {
		log.Printf("[NPLN ugcstore] attributs : capture illisible (%v), on laisse absent", err)
		return nil
	}
	if doc.GetName() == "" {
		return nil // jamais servir un Document sans nom : c'est precisement ce qui aborte le jeu
	}

	now := timestamppb.Now()
	if doc.GetCreateTime() == nil {
		doc.CreateTime = now
	}
	doc.UpdateTime = now

	return doc
}

// uidDuChemin extrait "u-exemple1000000000000" de
// "tenants/current/documents/services/GameRecord/users/u-exemple1000000000000/CoopUserAttribute/Data".
func uidDuChemin(name string) string {
	const marque = "/users/"
	i := strings.Index(name, marque)
	if i < 0 {
		return ""
	}
	reste := name[i+len(marque):]
	if j := strings.Index(reste, "/"); j >= 0 {
		return reste[:j]
	}
	return reste
}

// RunQuery vit desormais dans ugcstore_runquery.go : il doit RENDRE les documents.

func (u *ugcstoreServer) CommitDocuments(ctx context.Context, req *ugcpb.CommitDocumentsRequest) (*ugcpb.CommitDocumentsResponse, error) {
	now := timestamppb.Now()
	ops := req.GetWriteOperations()
	results := make([]*ugcpb.WriteResult, 0, len(ops))
	stored := 0

	for _, op := range ops {
		if upd := op.GetUpdateDocument(); upd != nil {
			if doc := upd.GetDocument(); doc != nil {
				doc.UpdateTime = now
				if doc.GetCreateTime() == nil {
					doc.CreateTime = now
				}
				storePutDocument(doc)
				stored++
			}
		}

		if del := op.GetDeleteDocument(); del != nil {
			documentStore.mu.Lock()
			delete(documentStore.docs, del.GetName())
			documentStore.mu.Unlock()
		}

		results = append(results, &ugcpb.WriteResult{UpdateTime: now})
	}

	log.Printf("[NPLN ugcstore] CommitDocuments ops=%d stored=%d -> OK", len(results), stored)
	return &ugcpb.CommitDocumentsResponse{WriteResults: results, CommitTime: now}, nil
}

// storeInitializedPointCard materialises a point-card document at `name`, using the captured
// PointCardTotal document as the field template. Called by GameRecord/InitializeAttributes so the
// read that immediately follows it succeeds.
func storeInitializedPointCard(name string) {
	doc := &ugcpb.Document{}
	if err := proto.Unmarshal(rawPointCardTotal, doc); err != nil {
		doc = &ugcpb.Document{Fields: &common.MapValue{Fields: map[string]*common.Value{}}}
	}

	now := timestamppb.Now()
	doc.Name = name
	doc.CreateTime = now
	doc.UpdateTime = now
	storePutDocument(doc)

	log.Printf("[NPLN ugcstore] point card initialisee : %s", name)
}

// storeInitializedCoopAttribute materialises the document that InitializeAttributes just told the
// client about. The reply and the document must agree, so the document is built from the very same
// attribute map the reply carried (field 1) under the name it announced (field 2).
func storeInitializedCoopAttribute(name string, initResponse []byte) {
	doc := &ugcpb.Document{Name: name}

	if attrs, ok := pbWalk(initResponse)[1]; ok {
		mv := &common.MapValue{}
		if err := proto.Unmarshal(attrs, mv); err == nil {
			doc.Fields = mv
		}
	}
	if doc.Fields == nil {
		doc.Fields = &common.MapValue{Fields: map[string]*common.Value{}}
	}

	now := timestamppb.Now()
	doc.CreateTime = now
	doc.UpdateTime = now
	storePutDocument(doc)

	log.Printf("[NPLN ugcstore] CoopUserAttribute initialise : %s (%d champ(s))", name, len(doc.GetFields().GetFields()))
}
