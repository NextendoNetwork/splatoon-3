package main

// Ce test lit le FIL, pas l'API : il compte les cadres HEADERS qu'un client HTTP/2 recoit quand le
// serveur termine un appel en succes sans aucun message.
//
// L'enjeu vient d'une mesure : dans une capture du hall (session de hall reelle console <->
// Nintendo, 43 echanges tous servis par « istio-envoy »), huit reponses ne portent aucun message —
// sept GetDocument et UserScreening/GetViolation — et toutes arrivent avec des en-tetes de reponse
// complets (content-type: application/grpc, npln-grpc-type: Unary, content-length: 0) SUIVIS des
// trailers. Deux cadres, donc.
//
// grpc-go, lui, condense tout dans un seul cadre HEADERS quand le handler sort sans avoir rien
// envoye (reponse dite « Trailers-Only »). C'est cette difference que finirVideCommeNintendo
// corrige, et c'est ce que ce test verrouille : sans le correctif 1 cadre, avec 2.

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc"
)

// compterCadresHeaders ouvre un vrai serveur grpc-go, y appelle une methode en flux, et renvoie le
// nombre de cadres HEADERS recus ainsi que les champs du PREMIER puis du DERNIER d'entre eux (en
// Trailers-Only les deux sont le meme cadre).
func compterCadresHeaders(t *testing.T, handler func(srv any, stream grpc.ServerStream) error) (int, map[string]string, map[string]string) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen : %v", err)
	}
	defer lis.Close()

	s := grpc.NewServer(grpc.ForceServerCodec(newHybridCodec()))
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "essai.Service",
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Vide",
			Handler:       handler,
			ServerStreams: true,
			ClientStreams: true,
		}},
		Metadata: "essai",
	}, new(any))
	go s.Serve(lis)
	defer s.Stop()

	conn, err := net.Dial("tcp", lis.Addr().String())
	if err != nil {
		t.Fatalf("dial : %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatalf("preface : %v", err)
	}
	fr := http2.NewFramer(conn, conn)
	if err := fr.WriteSettings(); err != nil {
		t.Fatalf("settings : %v", err)
	}

	var hbuf bytes.Buffer
	enc := hpack.NewEncoder(&hbuf)
	for _, kv := range [][2]string{
		{":method", "POST"}, {":scheme", "http"}, {":path", "/essai.Service/Vide"},
		{":authority", "essai"}, {"content-type", "application/grpc"}, {"te", "trailers"},
	} {
		_ = enc.WriteField(hpack.HeaderField{Name: kv[0], Value: kv[1]})
	}
	if err := fr.WriteHeaders(http2.HeadersFrameParam{
		StreamID: 1, BlockFragment: hbuf.Bytes(), EndHeaders: true,
	}); err != nil {
		t.Fatalf("headers : %v", err)
	}

	// Un message gRPC vide : 1 octet de compression + 4 octets de longueur a zero.
	msg := make([]byte, 5)
	binary.BigEndian.PutUint32(msg[1:], 0)
	if err := fr.WriteData(1, true, msg); err != nil {
		t.Fatalf("data : %v", err)
	}

	dec := hpack.NewDecoder(4096, nil)
	cadres := 0
	premier := map[string]string{}
	dernier := map[string]string{}
	for {
		f, err := fr.ReadFrame()
		if err != nil {
			break
		}
		t.Logf("cadre recu : %T %v", f, f.Header())
		hf, ok := f.(*http2.HeadersFrame)
		if !ok {
			continue
		}
		cadres++
		champs, _ := dec.DecodeFull(hf.HeaderBlockFragment())
		dernier = map[string]string{}
		for _, c := range champs {
			dernier[c.Name] = c.Value
			if cadres == 1 {
				premier[c.Name] = c.Value
			}
		}
		if hf.StreamEnded() {
			break
		}
	}
	return cadres, premier, dernier
}

// TestReponseAbsente_DeuxCadresPuisNotFound pose le critere AVANT l'essai en jeu : la reponse
// « aucun record » doit presenter au client le MEME premier cadre que Nintendo (content-type,
// npln-grpc-type: Unary, content-length: 0, et surtout PAS de grpc-status), puis un SECOND cadre
// portant grpc-status: 5 (NOT_FOUND). Si ce test tombe a 1 cadre, on est retombe en Trailers-Only,
// c'est-a-dire dans la forme deja mesuree comme fatale a S3 le 2026-08-12 : l'essai en jeu ne
// vaudrait alors rien.
func TestReponseAbsente_DeuxCadresPuisNotFound(t *testing.T) {
	n, premier, dernier := compterCadresHeaders(t, func(srv any, stream grpc.ServerStream) error {
		var m rawMsg
		_ = stream.RecvMsg(&m)
		return finirAbsentCommeNintendo(stream, "save record not found")
	})
	if got := dernier["grpc-status"]; got != "5" {
		t.Fatalf("dernier cadre : grpc-status=%q, attendu 5 (NOT_FOUND) : %v", got, dernier)
	}
	if n != 2 {
		t.Fatalf("%d cadres HEADERS, attendu 2 (en-tetes puis trailers) — Trailers-Only = essai invalide", n)
	}
	if got := premier["content-type"]; got != "application/grpc" {
		t.Fatalf("premier cadre : content-type=%q, attendu application/grpc", got)
	}
	if got := premier["npln-grpc-type"]; got != "Unary" {
		t.Fatalf("premier cadre : npln-grpc-type=%q, attendu Unary", got)
	}
	if got := premier["content-length"]; got != "0" {
		t.Fatalf("premier cadre : content-length=%q, attendu 0 (comme les echanges 5/8/13 de vierge.json)", got)
	}
	if _, ok := premier["grpc-status"]; ok {
		t.Fatalf("premier cadre : grpc-status ne doit PAS y figurer, il appartient aux trailers : %v", premier)
	}
}

func TestReponseVide_FormeDuVraiServeur(t *testing.T) {
	// Sans le correctif : grpc-go replie tout dans un cadre unique, ou grpc-status voisine avec
	// content-type. C'est la forme qui faisait planter S3.
	n, premier, _ := compterCadresHeaders(t, func(srv any, stream grpc.ServerStream) error {
		var m rawMsg
		_ = stream.RecvMsg(&m)
		return nil
	})
	if n != 1 {
		t.Fatalf("temoin : %d cadres HEADERS attendus 1 (grpc-go devrait replier en Trailers-Only)", n)
	}
	if _, ok := premier["grpc-status"]; !ok {
		t.Fatalf("temoin : grpc-status absent du cadre unique, la forme Trailers-Only n'est pas celle attendue : %v", premier)
	}

	// Avec le correctif : deux cadres, comme Nintendo — en-tetes d'abord, trailers ensuite.
	n, premier, _ = compterCadresHeaders(t, func(srv any, stream grpc.ServerStream) error {
		var m rawMsg
		_ = stream.RecvMsg(&m)
		return finirVideCommeNintendo(stream)
	})
	if n != 2 {
		t.Fatalf("avec SendHeader : %d cadres HEADERS, attendu 2 (en-tetes puis trailers)", n)
	}
	if got := premier["content-type"]; got != "application/grpc" {
		t.Fatalf("premier cadre : content-type=%q, attendu application/grpc", got)
	}
	if _, ok := premier["grpc-status"]; ok {
		t.Fatalf("premier cadre : grpc-status ne doit PAS y figurer, il appartient aux trailers : %v", premier)
	}
	t.Logf("premier cadre : %v", premier)
	if got := premier["npln-grpc-type"]; got != "Unary" {
		t.Fatalf("premier cadre : npln-grpc-type=%q, attendu Unary (comme la passerelle de Nintendo)", got)
	}
}
