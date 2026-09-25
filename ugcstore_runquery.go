package main

// RunQuery doit RENDRE les documents quand il y en a.
//
// CE QU'ON FAISAIT. Un bouchon : toujours un seul message, ne portant que read_time, jamais un
// document. C'etait juste — pour un compte VIERGE. La mesure d'origine (une capture de compte
// neuf, requetes 41 / 51 / 52) montre Nintendo repondre exactement cela quand il n'y a rien, et
// rendre ZERO message a la place faisait boucler la creation du SaveRecord.
//
// CE QUE CA CASSAIT. Splatoon 3 interroge « GameRecord/users/<uid> » pour lister ses batailles :
// c'est de la que vient l'HISTORIQUE EN JEU. Mesure du 2026-08-23 : 103 RunQuery sur ce parent, et
// 103 reponses « aucun document ». L'historique ne pouvait donc qu'etre vide, quoi qu'il se passe
// en partie.
//
// CE QUE FAIT LE VRAI SERVEUR. Capture d'un Splatfest officiel contre Nintendo, meme jour : les
// batailles reviennent en documents nommes
//
//	tenants/t-dce9377b-lp1/documents/services/GameRecord/users/<uid>/VsResults/20260823T000736_35ddcc57-...
//
// soit « <horodatage compact>_<uuid> » sous une collection VsResults. La console n'en ECRIT aucun —
// zero WriteDocuments dans toute la capture — c'est le SERVEUR qui les fabrique et les lui rend.
//
// ON GARDE LA FORME MESUREE POUR LE CAS VIDE. Sans document, on renvoie exactement le message unique
// a read_time seul d'avant : c'est mesure, et le compte neuf en depend.

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

// cheminDeDocument ramene un nom de document a sa partie stable, sans le locataire.
//
// Le jeu ecrit « tenants/current/... » dans ses requetes tandis que les documents que nous
// fabriquons portent le locataire en clair (« tenants/t-dce9377b-lp1/... »). Comparer les noms bruts
// ferait manquer tous les documents ; on compare donc ce qui suit « /documents/ ».
func cheminDeDocument(nom string) string {
	if i := strings.Index(nom, "/documents/"); i >= 0 {
		return nom[i+len("/documents/"):]
	}
	return nom
}

// documentsSousParent rend les documents ranges sous ce parent, les plus recents d'abord.
//
// Le tri se fait sur le nom : les identifiants de bataille commencent par un horodatage compact
// (20260823T000736_...), donc l'ordre lexicographique DECROISSANT donne bien le plus recent en
// tete — ce que le jeu demande en triant sur « started_at ».
func documentsSousParent(parent string) []*ugcpb.Document {
	relireLesNouveauxDocuments()
	return documentsSousParentEnMemoire(parent)
}

// documentsSousParentEnMemoire searches the already-loaded store without disk I/O. The locker and
// deck listings are written by this NPLN process itself, so they do not need the cross-process
// refresh used for GameSync-created match records.
func documentsSousParentEnMemoire(parent string) []*ugcpb.Document {
	prefixe := cheminDeDocument(strings.TrimSuffix(parent, "/")) + "/"

	documentStore.mu.RLock()
	trouves := make([]*ugcpb.Document, 0, 8)
	for nom, doc := range documentStore.docs {
		if strings.HasPrefix(cheminDeDocument(nom), prefixe) {
			trouves = append(trouves, doc)
		}
	}
	documentStore.mu.RUnlock()

	sort.Slice(trouves, func(i, j int) bool {
		return trouves[i].GetName() > trouves[j].GetName()
	})
	return trouves
}

// RunQuery est une requete en flux sur les documents UGC.
func (u *ugcstoreServer) RunQuery(req *ugcpb.RunQueryRequest, stream grpc.ServerStreamingServer[ugcpb.RunQueryResponse]) error {
	docs := documentsSousParent(req.GetParent())

	// ⚠️ LA REQUETE PORTE UNE COLLECTION, ET IL FAUT LA RESPECTER.
	//
	// Le parent est le joueur — .../services/GameRecord/users/<uid> — et tout son GameRecord vit
	// dessous : VsResults, mais aussi VsUserAttribute/Data et CoopUserAttribute/Data. Rendre tout
	// ce qui est sous le parent revient a repondre a « donne-moi mes batailles » par le document
	// d'attributs du joueur, 4 390 octets.
	//
	// Ce que fait le jeu alors, mesure du 2026-08-23 : il abandonne. Trace du fil nn.npln.Worker,
	// abort a Thunder.nss:0x667fe8, l'analyseur de nom de ressource d'ugcstore — 2162-0001. Les
	// deux plantages de la soiree montrent la meme signature : une lecture de 4 515 octets, puis
	// l'abandon dans la seconde.
	//
	// La capture Nintendo, elle, repond a cette meme requete par DIX-NEUF octets : un read_time et
	// aucun document (une capture de compte neuf, echanges 51 et 52).
	if collections := req.GetStructuredQuery().GetFrom(); len(collections) > 0 {
		garde := docs[:0]
		for _, d := range docs {
			for _, c := range collections {
				if id := c.GetCollectionId(); id != "" && strings.Contains(d.GetName(), "/"+id+"/") {
					garde = append(garde, d)
					break
				}
			}
		}
		docs = garde
	}

	if len(docs) == 0 {
		// Forme MESUREE du cas vide : un message, read_time seul. Ne pas y toucher.
		log.Printf("[NPLN ugcstore] RunQuery parent=%q -> 1 message, read_time seul (aucun document)", req.GetParent())
		return stream.Send(&ugcpb.RunQueryResponse{ReadTime: timestamppb.Now()})
	}

	lu := timestamppb.Now()
	for _, d := range docs {
		if err := stream.Send(&ugcpb.RunQueryResponse{Document: d, ReadTime: lu}); err != nil {
			return err
		}
	}
	log.Printf("[NPLN ugcstore] RunQuery parent=%q -> %d document(s)", req.GetParent(), len(docs))
	return nil
}

// ---- rafraichissement interprocessus ---------------------------------------------------------
//
// L'arbitre GameSync et le serveur NPLN ont des magasins en memoire distincts, mais partagent le
// dossier disque. Le journal changes.index indique au serveur NPLN les seuls fichiers modifies
// depuis sa derniere lecture, au lieu de relire tous les documents a chaque changement du dossier.

var derniereRelecture struct {
	sync.Mutex
	offset      int64
	initialisee bool
}

// relireLesNouveauxDocuments consumes the append-only journal of documents written by the sibling
// process. The complete directory is loaded once at startup; runtime refreshes read only changed
// document files instead of rescanning and unmarshalling thousands of unchanged files.
func relireLesNouveauxDocuments() {
	fi, err := os.Stat(documentChangesPath())
	if err != nil {
		return
	}

	derniereRelecture.Lock()
	if !derniereRelecture.initialisee {
		// Tests and callers that have not run chargerDocuments start at the journal beginning.
		derniereRelecture.initialisee = true
	}
	if fi.Size() <= derniereRelecture.offset {
		derniereRelecture.Unlock()
		return
	}

	debut := derniereRelecture.offset
	f, err := os.Open(documentChangesPath())
	if err != nil {
		derniereRelecture.Unlock()
		return
	}
	if _, err := f.Seek(debut, io.SeekStart); err != nil {
		_ = f.Close()
		derniereRelecture.Unlock()
		return
	}
	data, readErr := io.ReadAll(io.LimitReader(f, fi.Size()-debut))
	_ = f.Close()
	if readErr != nil {
		derniereRelecture.Unlock()
		return
	}
	// Ignore a trailing partial line if another process is appending at the same time; it will
	// be consumed on the next call once its newline has been written.
	lastNewline := bytes.LastIndexByte(data, '\n')
	if lastNewline < 0 {
		derniereRelecture.Unlock()
		return
	}
	consumed := data[:lastNewline+1]
	loaded := 0
	for _, key := range strings.Split(string(consumed), "\n") {
		if len(key) != 40 {
			continue
		}
		b, err := os.ReadFile(filepath.Join(documentsDir(), key+".pb"))
		if err != nil {
			log.Printf("[NPLN ugcstore] document journalise %s illisible : %v", key, err)
			continue
		}
		doc := &ugcpb.Document{}
		if err := proto.Unmarshal(b, doc); err != nil || doc.GetName() == "" {
			log.Printf("[NPLN ugcstore] document journalise %s invalide", key)
			continue
		}
		documentStore.mu.Lock()
		// Replace too: the sibling may have updated an existing record, not just created one.
		documentStore.docs[doc.GetName()] = doc
		documentStore.mu.Unlock()
		loaded++
	}
	derniereRelecture.offset += int64(len(consumed))
	derniereRelecture.Unlock()
	if loaded > 0 {
		log.Printf("[NPLN ugcstore] %d document(s) changes relus depuis le journal", loaded)
	}
}
