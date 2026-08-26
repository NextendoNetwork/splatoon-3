package main

// L'ERREUR EXACTE QUE NINTENDO REND POUR UN DOCUMENT ABSENT.
//
// Releve dans la capture d'un vrai festival sur console —
// le relais de session, flux 55,
// GetDocument sur GameRecord/users/<uid>/WeaponPowerMeasurement/16 :
//
//	:status                  200
//	grpc-status              5
//	grpc-message             NotFound. reason: "Document:"<chemin>" Not Found"
//	grpc-status-details-bin  google.rpc.Status{ code: 5, message: <le meme>, details: [Any{
//	                             type.googleapis.com/nn.npln.errdetails.NError,
//	                             { 1: <32 hexa>, 19: <16 hexa>, 6: 0 } }] }
//
// Deux HEADERS, zero DATA. La forme etait donc juste chez nous depuis longtemps ; ce qui manquait,
// c'est le CONTENU de l'erreur. Nous rendions « document not found » sans aucun detail, et le jeu
// rappelait sans fin — mesure du 2026-08-23 sur un compte inscrit a un festival : 1 036 lectures
// et 2 068 creations de mesure de puissance par minute, ecran « Connexion a Internet » sans issue.
//
// Les deux identifiants hexadecimaux changent a chaque reponse dans la capture : ce sont des
// marqueurs de requete, pas des valeurs a recopier. On en tire de nouveaux a chaque fois.

import (
	"crypto/rand"
	"encoding/hex"
	"strings"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

const typeErreurNpln = "type.googleapis.com/nn.npln.errdetails.NError"

// cheminRelatifDuDocument rend ce que Nintendo cite dans son message : la partie qui suit
// « /documents/ », sans le locataire.
func cheminRelatifDuDocument(nom string) string {
	if i := strings.Index(nom, "/documents/"); i >= 0 {
		return nom[i+len("/documents/"):]
	}
	return nom
}

func hexAleatoire(octets int) string {
	b := make([]byte, octets)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// detailNError reconstruit la charge nn.npln.errdetails.NError a la main : nous n'avons pas son
// schema, seulement les trois champs vus dans la capture.
func detailNError() []byte {
	var b []byte
	b = appendProtoString(b, 1, hexAleatoire(16)) // 32 caracteres hexa
	b = appendProtoString(b, 19, hexAleatoire(8)) // 16 caracteres hexa
	b = append(b, 0x30, 0x00)                     // champ 6, varint 0
	return b
}

// erreurDocumentAbsent rend l'erreur telle que Nintendo la formule, details compris.
func erreurDocumentAbsent(nom string) error {
	message := `NotFound. reason: "Document:"` + cheminRelatifDuDocument(nom) + `" Not Found"`

	st := &spb.Status{
		Code:    int32(codes.NotFound),
		Message: message,
		Details: []*anypb.Any{{TypeUrl: typeErreurNpln, Value: detailNError()}},
	}
	return status.ErrorProto(st)
}
