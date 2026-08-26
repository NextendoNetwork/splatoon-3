package main

// Verrou sur SelectFestBreakingNews.
//
// Mesure du 2026-08-22, Splatfest officiel JUEA-00107, console reelle contre Nintendo : pendant une
// fete OUVERTE le jeu appelle cette methode, et le vrai serveur repond CINQ octets de DATA —
// l'en-tete de trame gRPC, longueur zero. Un message vide.
//
// Chez nous la methode n'existait pas. Et comme FestService est un service ENREGISTRE, grpc-go
// rendait Unimplemented sans passer par l'UnknownServiceHandler : le jeu recevait une erreur dure
// la ou le vrai serveur honore l'appel.

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

const methodeBreakingNews = "/nn.npln.toyohr.v1.FestService/SelectFestBreakingNews"

func TestFestServiceExposeBreakingNews(t *testing.T) {
	// La methode ne vient pas du proto genere : elle est ajoutee a la description du service.
	// Si quelqu'un revient a RegisterFestServiceServer, elle disparait et le jeu reprend son
	// Unimplemented — d'ou ce test.
	for _, m := range toyohrpb.FestService_ServiceDesc.Methods {
		if m.MethodName == "SelectFestBreakingNews" {
			t.Fatal("le proto genere la connait maintenant : l'ajout manuel n'a plus lieu d'etre")
		}
	}

	var fest toyohrpb.FestServiceServer = &festServer{}
	s := grpc.NewServer()
	registerFestService(s, &fest)
	defer s.Stop()

	svc, ok := s.GetServiceInfo()["nn.npln.toyohr.v1.FestService"]
	if !ok {
		t.Fatal("FestService n'est pas enregistre")
	}
	trouve := false
	for _, m := range svc.Methods {
		if m.Name == "SelectFestBreakingNews" {
			trouve = true
		}
	}
	if !trouve {
		t.Error("SelectFestBreakingNews absente du service : le jeu recevra Unimplemented")
	}
	// Les methodes generees doivent survivre a la recopie de la description.
	for _, attendue := range []string{"GetFestResult", "SelectFestSchedule", "GetFestDecryptionKey"} {
		vu := false
		for _, m := range svc.Methods {
			if m.Name == attendue {
				vu = true
			}
		}
		if !vu {
			t.Errorf("%s a disparu : la recopie de la ServiceDesc a perdu une methode generee", attendue)
		}
	}
}

func TestSelectFestBreakingNewsRendUnMessageVide(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen : %v", err)
	}
	defer lis.Close()

	var fest toyohrpb.FestServiceServer = &festServer{}
	s := grpc.NewServer()
	registerFestService(s, &fest)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client : %v", err)
	}
	defer conn.Close()

	ctx, annule := context.WithTimeout(context.Background(), 5*time.Second)
	defer annule()

	rep := &emptypb.Empty{}
	if err := conn.Invoke(ctx, methodeBreakingNews, &emptypb.Empty{}, rep); err != nil {
		if status.Code(err) == codes.Unimplemented {
			t.Fatal("Unimplemented : c'est exactement l'erreur que le jeu recevait avant le correctif")
		}
		t.Fatalf("appel refuse : %v", err)
	}
	// Un message vide, pas une absence de message : la capture montre cinq octets de DATA.
	if n := len(rep.String()); n != 0 {
		t.Errorf("reponse non vide (%q) — la capture de Nintendo est un message de longueur nulle", rep)
	}
}
