package main

// Mode LOCAL (sans Traefik) : sert gRPC (h2) ET le REST vermillion/penne (http/1.1) sur UN SEUL
// listener TLS :443, démultiplexé par ALPN après le handshake. Pour tester S3 en pointant le
// DNS de Ryujinx sur 127.0.0.1 : S3 tape vermillion/penne (REST) et t-dce9377b (gRPC), tout sur :443.
// Activé par la variable d'env NPLN_LOCAL. On voit ainsi TOUT le flux natif de S3 côté serveur.

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
)

// chanListener : net.Listener alimenté par un canal — permet de démuxer un listener TLS
// en deux serveurs (grpc h2 / http1.1) selon l'ALPN négocié.
type chanListener struct {
	ch   chan net.Conn
	addr net.Addr
	done chan struct{}
}

func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{ch: make(chan net.Conn, 32), addr: addr, done: make(chan struct{})}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error   { close(l.done); return nil }
func (l *chanListener) Addr() net.Addr { return l.addr }

func (l *chanListener) push(c net.Conn) {
	select {
	case l.ch <- c:
	case <-l.done:
		_ = c.Close()
	}
}

// startLocalCombined sert gRPC + REST sur addr (TLS), démuxé par ALPN. Bloquant.
func startLocalCombined(addr, certFile, keyFile string) {
	tlsCert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("[NPLN LOCAL] load cert: %v", err)
	}
	// [Nextendo] C'est CE point d'entree que les consoles joignent sur le 443, pas celui de
	// main.go. Sans le journal ici, une capture de leur trafic reste indechiffrable — mesure du
	// 2026-08-22 : trois a cinq poignees de main par joueur dans la capture, zero trame lisible,
	// parce que seule l'autre configuration etait instrumentee.
	tlsCfg := avecJournalDesCles(&tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"h2", "http/1.1"},
		MinVersion:   tls.VersionTLS12,
	})
	ln, err := tls.Listen("tcp", addr, tlsCfg)
	if err != nil {
		log.Fatalf("[NPLN LOCAL] listen %s: %v", addr, err)
	}

	grpcSrv := buildServer(nil) // TLS terminée ici → grpc servi en clair (h2c) sur les conns démuxées
	restSrv := &http.Server{Handler: buildRestMux()}
	grpcLn := newChanListener(ln.Addr())
	restLn := newChanListener(ln.Addr())
	go func() { _ = grpcSrv.Serve(grpcLn) }()
	go func() { _ = restSrv.Serve(restLn) }()

	log.Printf("[NPLN LOCAL] combiné gRPC(h2)+REST(http/1.1) sur %s — DNS→127.0.0.1", addr)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Printf("[NPLN LOCAL] accept: %v", err)
			continue
		}
		go func(c net.Conn) {
			tc, ok := c.(*tls.Conn)
			if !ok {
				_ = c.Close()
				return
			}
			if err := tc.Handshake(); err != nil {
				log.Printf("[NPLN LOCAL] handshake KO: %v", err)
				_ = c.Close()
				return
			}
			proto := tc.ConnectionState().NegotiatedProtocol
			log.Printf("[NPLN LOCAL] conn de %s SNI=%q ALPN=%q", tc.RemoteAddr(), tc.ConnectionState().ServerName, proto)
			if proto == "h2" {
				grpcLn.push(traceHTTP2Connection(c))
			} else {
				restLn.push(c)
			}
		}(c)
	}
}
