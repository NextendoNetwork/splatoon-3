package main

// npln-grpc-type — l'en-tete que la passerelle de Nintendo estampille sur CHAQUE reponse.
//
// Mesure sur le corpus de capture (le corpus de captures NPLN, 704 requetes d'une vraie
// console vers t-dce9377b-lp1.lp1.t.npln.srv.nintendo.net:443) : les 704 reponses portent
// `npln-grpc-type`, sans une seule exception, et sa valeur est la NATURE de l'appel :
//
//	Unary                    Auth/IssuePrearrangedUserToken, CloudSave/WriteSaveRecord,
//	                         Matchmaker/CreateMatchmakingTicket, GameSessionService/AllocateIceServerSet…
//	ServerStreaming          Matchmaker/TrackMatchmakingTicket, Ugcstore/RunQuery,
//	                         Friends/SubscribeFriendUsers, LobbyMessaging/RecvMessage…
//	BidirectionalStreaming   PresenceService/KeepAlive
//
// Nous n'en emettions aucun. C'est la derniere difference UNIVERSELLE qui restait entre nos
// reponses et celles de Nintendo, une fois le corps des messages rendu identique au fil.
//
// On l'attache avec SetHeader et non SendHeader : la metadonnee rejoint le cadre d'en-tetes
// quand il part, sans le forcer a partir plus tot. C'est important — l'absence d'une ressource
// doit continuer de se dire en DEUX cadres (en-tetes puis trailers NotFound), et forcer un envoi
// anticipe ici casserait ce comportement patiemment regle (voir npln_empty_reply.go).

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const enteteTypeGrpc = "npln-grpc-type"

// natureDuFlux rend la valeur que Nintendo met dans l'en-tete pour un flux donne.
func natureDuFlux(info *grpc.StreamServerInfo) string {
	switch {
	case info == nil:
		return "Unary"
	case info.IsClientStream && info.IsServerStream:
		return "BidirectionalStreaming"
	case info.IsServerStream:
		return "ServerStreaming"
	case info.IsClientStream:
		return "ClientStreaming"
	}
	return "Unary"
}

// typeGrpcUnaire estampille les reponses unaires.
func typeGrpcUnaire(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	_ = grpc.SetHeader(ctx, metadata.Pairs(enteteTypeGrpc, "Unary"))
	return handler(ctx, req)
}

// typeGrpcFlux estampille les reponses en flux (y compris celles du gestionnaire de service
// inconnu, qui rejoue des captures : Nintendo les estampille aussi).
func typeGrpcFlux(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	_ = ss.SetHeader(metadata.Pairs(enteteTypeGrpc, natureDuFlux(info)))
	return handler(srv, ss)
}

// chaineUnaire enchaine deux intercepteurs unaires : le premier estampille, le second trace.
// grpc-go n'accepte qu'un seul UnaryInterceptor par serveur.
func chaineUnaire(premier, second grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return premier(ctx, req, info, func(c context.Context, r any) (any, error) {
			return second(c, r, info, handler)
		})
	}
}
