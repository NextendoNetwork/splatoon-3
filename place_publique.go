package main

// LES CASIERS DE LA PLACE — de vrais joueurs Nextendo, et seulement ceux qui l'ont accepte.
//
// CE QUE FAIT NINTENDO, MESURE. Capture du demarrage, 2026-08-23 : au lancement, le serveur pousse
// a la console 31 documents
//
//	tenants/<locataire>/documents/services/toyohr/interfaces/locker/lockerPosts/<uuid>
//
// et le contenu ne porte QUE le casier — aucune identite :
//
//	Locker { AppVersion, FestRegion, IsPromising,
//	         LockerInfo { ContentInfo { ObjectInfo[ PosX PosY PosZ RotX RotY RotZ
//	                                                GroupID TransIndex Type GeneralBit ] } } }
//
// CE QU'ON FAISAIT. Locker/SelectDocuments tombait dans le rejeu : on renvoyait le casier CAPTURE,
// le meme pour tout le monde, 327 fois par jour.
//
// CE QU'ON FAIT. Les joueurs publient DEJA leur casier chez nous : la console l'ecrit par
// Ugcstore/CommitDocuments — que nous stockons — puis appelle Locker/RegisterDocument pour le
// rendre visible. Mesure du 2026-08-23 : NEUF casiers de nos joueurs dorment ainsi sur le disque,
// plus neuf jeux de cartes. Nous les rangions sans jamais les rendre.
//
// LE CONSENTEMENT EST DEJA DANS LE GESTE. La sauvegarde porte « IsUploadLockerInfo » — la reponse du
// joueur a la question que Splatoon 3 lui pose lui-meme — et 37 joueurs sur 906 ont dit oui. Mais on
// n'a pas besoin de relire ce drapeau : la console ne PUBLIE le document que si le joueur a accepte.
// L'existence du lockerPost EST le consentement, exprime par le jeu lui-meme.
//
// Une piste morte, notee pour qu'on ne la reprenne pas : le casier ne voyage PAS dans la sauvegarde.
// Sur 906 sauvegardes, ZERO porte une vraie cle LockerInfo — le nom n'apparait que comme sous-mot
// d'IsUploadLockerInfo. Fabriquer les casiers depuis les sauvegardes ne pouvait donc rien donner, et
// c'est bien zero que le serveur a publie au premier essai.

import (
	"crypto/sha1"
	"encoding/hex"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

// casiersPousses : combien de casiers on sert d'un coup. Nintendo en pousse 31 ; on s'aligne plutot
// que d'en deverser des centaines dans un hall qui n'en affiche qu'une poignee.
const casiersPousses = 31

// jeuxPousses : Nintendo en pousse SIX au demarrage.
const jeuxPousses = 6

// dureeDuCacheCasiers : relire 905 sauvegardes a chaque appel couterait cher pour un contenu qui ne
// change qu'au rythme ou les joueurs redecorent.
const dureeDuCacheCasiers = 10 * time.Minute

// identifiantDeCasier derive un identifiant STABLE du compte du joueur.
//
// Il doit rester le meme d'un demarrage a l'autre, sinon la console verrait un casier different a
// chaque fois pour le meme joueur. On le derive donc du compte plutot que de le tirer au sort.
func identifiantDeCasier(uid string) string {
	h := sha1.Sum([]byte("lockerPost/" + uid))
	s := hex.EncodeToString(h[:])
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32]
}

// casierDuJoueur fabrique le document de casier d'un joueur, ou nil s'il n'a pas accepte de le
// publier ou s'il n'en a pas.
func casierDuJoueur(uid string, sauvegarde *commonpb.MapValue) *ugcpb.Document {
	if sauvegarde == nil {
		return nil
	}
	f := sauvegarde.GetFields()

	// Le consentement, tel que le joueur l'a donne DANS LE JEU.
	if !f["IsUploadLockerInfo"].GetBooleanValue() {
		return nil
	}
	info := f["LockerInfo"]
	if info.GetMapValue() == nil {
		return nil
	}

	casier := map[string]*commonpb.Value{"LockerInfo": info}
	// Les trois champs d'entete, quand la sauvegarde les porte. On ne les invente pas.
	for _, k := range []string{"AppVersion", "FestRegion", "IsPromising"} {
		if v, ok := f[k]; ok {
			casier[k] = v
		}
	}

	maintenant := timestamppb.Now()
	return &ugcpb.Document{
		Name: npnTenant + "/documents/services/toyohr/interfaces/locker/lockerPosts/" +
			identifiantDeCasier(uid),
		Fields: &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			"Locker": {ValueType: &commonpb.Value_MapValue{MapValue: &commonpb.MapValue{Fields: casier}}},
		}},
		CreateTime: maintenant,
		UpdateTime: maintenant,
	}
}

// uidDuFichierDeSauvegarde retrouve le compte a partir du nom de fichier.
func uidDuFichierDeSauvegarde(nom string) string {
	return strings.TrimSuffix(filepath.Base(nom), ".record.pb")
}

// LES COLLECTIONS DE LA PLACE. Nintendo en pousse trois au demarrage : 31 casiers, 60 dessins et
// 6 jeux de cartes. Nous en tenons deux — les joueurs publient des casiers et des jeux de cartes,
// pas encore de dessins.
const (
	collectionDesCasiers = "documents/services/toyohr/interfaces/locker/lockerPosts"
	collectionDesJeux    = "documents/services/toyohr/interfaces/canola/deckPosts"
)

// cachesDeLaPlace : un cache par collection. Relire le magasin a chaque appel couterait cher pour
// un contenu qui ne change qu'au rythme ou les joueurs redecorent.
var cachesDeLaPlace = struct {
	sync.Mutex
	m map[string]*cacheCollection
}{m: map[string]*cacheCollection{}}

type cacheCollection struct {
	docs []*ugcpb.Document
	vu   time.Time
}

// documentsDeLaPlace rend ce que nos joueurs ont publie dans une collection, plafonne comme
// Nintendo le fait.
//
// LE CONSENTEMENT EST DANS LE GESTE : la console ne publie un document que si le joueur a accepte
// de le montrer. Son existence vaut accord, et il n'y a pas de drapeau a relire.
func documentsDeLaPlace(collection string, plafond int) []*ugcpb.Document {
	cachesDeLaPlace.Lock()
	defer cachesDeLaPlace.Unlock()

	c := cachesDeLaPlace.m[collection]
	if c != nil && time.Since(c.vu) < dureeDuCacheCasiers && c.docs != nil {
		return c.docs
	}

	docs := documentsSousParentEnMemoire(npnTenant + "/" + collection)
	if len(docs) > plafond {
		docs = docs[:plafond]
	}
	cachesDeLaPlace.m[collection] = &cacheCollection{docs: docs, vu: time.Now()}
	return docs
}

// casiersPublics rend les casiers publies par nos joueurs.
func casiersPublics() []*ugcpb.Document {
	return documentsDeLaPlace(collectionDesCasiers, casiersPousses)
}

// jeuxDeCartesPublics rend les jeux de cartes publies par nos joueurs.
//
// Nintendo en pousse SIX au demarrage — bien moins que les 31 casiers. On s'aligne : un hall n'en
// affiche qu'une poignee, et en deverser davantage ne montrerait rien de plus.
func jeuxDeCartesPublics() []*ugcpb.Document {
	return documentsDeLaPlace(collectionDesJeux, jeuxPousses)
}
