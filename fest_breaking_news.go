package main

// SelectFestBreakingNews — les « dernieres nouvelles » d'un festival.
//
// D'OU CA VIENT. Capture du 2026-08-22, Splatfest officiel JUEA-00107 en cours, console reelle
// contre les serveurs de Nintendo (relais-session). Pendant une fete ouverte, le jeu appelle
// QUATRE methodes de FestService :
//
//	SelectFestSchedule        nous l'avons
//	GetFestDecryptionKey      nous l'avons
//	GetFestResult             nous l'avons
//	SelectFestBreakingNews    nous ne l'avions PAS — cette methode n'existe meme pas dans notre proto
//
// Une methode absente d'un service ENREGISTRE ne tombe pas dans l'UnknownServiceHandler : grpc-go
// rend Unimplemented avant nous. Le jeu recevait donc une erreur dure sur un appel que le vrai
// serveur honore.
//
// CE QUE REPOND NINTENDO, MESURE. Sur le flux HTTP/2 numero 87 de la capture, la reponse fait
// CINQ octets de DATA : l'en-tete de trame gRPC, longueur zero, et rien derriere. C'est un
// MESSAGE VIDE — pas une absence de message.
//
// La nuance compte, et elle a deja coute cher ici : « aucun message » (content-length 0, zero
// octet de DATA) est la forme de finirVideCommeNintendo, et servir l'une pour l'autre a fait
// planter S3 par le passe. Ici la capture est nette : cinq octets, donc un message, vide.
//
// Autrement dit, pendant cette fete, Nintendo n'a aucune nouvelle a annoncer — mais l'appel doit
// reussir en rendant une reponse vide. On reproduit exactement cela.
//
// ⚠️ CE QUI N'EST PAS MESURE : la forme d'une reponse qui porterait REELLEMENT des nouvelles. La
// capture n'en montre aucune. Le jour ou l'on en verra une, c'est ici qu'il faudra la decrire.

import (
	"log"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// registerFestService enregistre FestService en lui AJOUTANT SelectFestBreakingNews.
//
// La methode ne figure pas dans le proto genere : plutot que de regenerer tout le paquet pour une
// reponse vide, on recopie la description du service et on y ajoute une entree en flux. C'est le
// meme geste que registerUgcstore, et il laisse les methodes generees intactes.
func registerFestService(s *grpc.Server, impl *toyohrpb.FestServiceServer) {
	desc := toyohrpb.FestService_ServiceDesc // copie

	streams := make([]grpc.StreamDesc, 0, len(desc.Streams)+1)
	streams = append(streams, desc.Streams...)
	streams = append(streams, grpc.StreamDesc{
		StreamName: "SelectFestBreakingNews",
		Handler:    selectFestBreakingNewsHandler,
	})
	desc.Streams = streams

	s.RegisterService(&desc, *impl)
}

// selectFestBreakingNewsHandler rend le message vide que rend le vrai serveur.
//
// On lit la demande dans un Empty : ses champs inconnus y sont CONSERVES, ce qui permet de les
// re-serialiser pour les journaliser. C'est notre seule fenetre sur ce que le jeu demande ici, et
// elle servira le jour ou l'on voudra rendre de vraies nouvelles.
func selectFestBreakingNewsHandler(srv any, stream grpc.ServerStream) error {
	demande := &emptypb.Empty{}
	if err := stream.RecvMsg(demande); err != nil {
		return err
	}
	if brut, err := proto.Marshal(demande); err == nil {
		log.Printf("[NPLN fest] SelectFestBreakingNews : demande de %d octet(s) -> reponse vide (forme Nintendo)",
			len(brut))
	}
	// Cinq octets sur le fil : l'en-tete de trame, longueur zero. Exactement la capture.
	return stream.SendMsg(&emptypb.Empty{})
}
