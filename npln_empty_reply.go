package main

// finirVideCommeNintendo — terminer un appel gRPC en SUCCES sans le moindre message, dans la
// forme EXACTE du vrai serveur.
//
// La mesure : une capture du hall est une session de hall complete capturee entre la console
// et Nintendo (43 echanges, tous repondus par « istio-envoy »). Huit d'entre eux ne portent aucun
// message — sept GetDocument et UserScreening/GetViolation — et leurs en-tetes de reponse sont
// toujours les memes :
//
//	content-type: application/grpc / npln-grpc-type: Unary / content-length: 0
//
// Autrement dit Nintendo emet bien un cadre HEADERS, puis rien, puis les trailers. Or grpc-go,
// si le handler sort sans avoir envoye ni message ni en-tete, condense tout dans un seul cadre
// HEADERS (« Trailers-Only »). C'est cette difference de forme — pas le vide lui-meme — qui
// faisait planter S3 (thread nn.npln.Worker, nnSdk:0x17574c) la ou le vrai serveur passe. On force
// donc l'envoi des en-tetes avant de rendre la main.
import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// enteteVideCommeNintendo emet le PREMIER des deux cadres du vrai serveur : content-type
// (grpc-go l'ajoute), npln-grpc-type: Unary et content-length: 0. Mesure : echanges 5, 8 et 13 de
// vierge.json, et les huit reponses sans corps de hall3.json.
func enteteVideCommeNintendo(stream grpc.ServerStream) error {
	return stream.SendHeader(metadata.Pairs("npln-grpc-type", "Unary", "content-length", "0"))
}

// finirAbsentCommeNintendo — repondre « cette ressource N'EXISTE PAS » dans la forme A DEUX CADRES.
//
// CE QUE LA CAPTURE PROUVE, ET CE QU'ELLE NE PROUVE PAS. Sur les 59 echanges de vierge.json et les
// 43 de hall3.json, le champ `trailers` est VIDE et « grpc-status » n'apparait dans AUCUN
// dictionnaire d'en-tetes — pas meme sur les reponses avec corps. Proxide ne transcrit donc jamais
// le second cadre. Deux consequences :
//   - la reponse n'etait PAS en Trailers-Only (sinon grpc-status figurerait dans `headers`, comme
//     le montre le temoin de npln_empty_reply_wire_test.go) : il y a bien eu deux cadres ;
//   - le grpc-status de Nintendo n'est PAS MESURE. « grpc-status: 0 » etait une hypothese.
//
// POURQUOI ELLE COMPTE. Un appel unaire termine sans le moindre message ne dit pas au client SDK
// « rien ici » : il lui fait construire un objet de reponse PAR DEFAUT. C'est mesure sur ce meme
// client, sur un autre appel — commit 6ed6b4d : pour un document jamais ecrit nous terminions sans
// aucun message, « le client construisait alors un Document par defaut, au nom vide », et
// l'analyseur de nom de ressource s'abattait. Sur GetSaveRecord, le meme mecanisme lui construit un
// SaveRecord par defaut : un record qui EXISTE et qui est VIDE. Le jeu ne cree donc rien, il
// VALIDE — d'ou le WriteSaveRecord evt=Validate portant exactement les cles manquantes
// (CloudRandomSet, HaveGearClothesMap, HaveGearHeadMap, HaveGearShoesMap), evenement qui ne figure
// nulle part dans le corpus Nintendo, ou c'est le Create qui remplit ces champs.
//
// L'essai « NotFound » deja consigne comme elimine (cloudsave_nextendo.go:356) date d'AVANT
// finirVideCommeNintendo : il partait en Trailers-Only, un cadre unique — la forme dont la mesure
// du 2026-08-12 a prouve qu'elle fait echouer S3. Le meme code en DEUX cadres n'a jamais ete
// essaye : en-tetes d'abord (donc grpc-go ne peut plus replier), status ensuite.
func finirAbsentCommeNintendo(stream grpc.ServerStream, msg string) error {
	if err := enteteVideCommeNintendo(stream); err != nil {
		return err
	}
	return status.Error(codes.NotFound, msg)
}

func finirVideCommeNintendo(stream grpc.ServerStream) error {
	// npln-grpc-type est un en-tete que la passerelle de Nintendo ajoute a chaque reponse ; on le
	// reproduit pour rester au plus pres de ce que le client a l'habitude de lire.
	// content-length: 0 — dernier ecart connu avec Nintendo sur cet appel. Sa passerelle (Envoy) le
	// joint systematiquement aux reponses sans message ; grpc-go ne l'emet pas de lui-meme. Le test
	// de fil dira s'il franchit la couche ou s'il est filtre comme en-tete reserve.
	if err := stream.SendHeader(metadata.Pairs("npln-grpc-type", "Unary", "content-length", "0")); err != nil {
		return err
	}
	return nil
}
