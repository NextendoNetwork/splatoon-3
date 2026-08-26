package main

// Champ 9 du MatchmakingTicket — mesure, pas hypothese.
//
// Le flux TrackMatchmakingTicket capture le 2026-08-07 (captured/TrackMatchmakingTicket.bin,
// 19 789 o = 3 messages) porte un champ que notre .proto ne connait pas :
//
//	message #0  state=SEARCHING  4052 o  -> champs 1,2,4x4,5          (PAS de champ 9)
//	message #1  state=PLACING    4054 o  -> champs 1,2,4x4,5, 9=1
//	message #2  state=SUCCEEDED 11673 o  -> champs 1,2,4x4,5,6x4,7, 9=1
//
// Il apparait donc a PLACING et ne repart plus. Varint, valeur 1. Nous ne l'emettions jamais.
//
// Notre .proto s'arrete au champ 7 : plutot que de regenerer les descripteurs pour un champ dont
// on ignore le nom, on l'ecrit dans les unknownFields — protobuf les serialise tels quels, a la
// bonne place, et le client ne fait pas la difference.

import (
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// champ9Present = tag 9, wire type 0 (varint), valeur 1  ->  0x48 0x01
var champ9Present = protoreflect.RawFields{0x48, 0x01}

// marquerChamp9 ajoute le champ 9 inconnu au ticket, comme le fait le serveur de Nintendo a partir
// de l'etat PLACING. Sans effet si le ticket le porte deja.
func marquerChamp9(t proto.Message) {
	if t == nil {
		return
	}
	r := t.ProtoReflect()
	u := r.GetUnknown()
	for i := 0; i+1 < len(u); i++ {
		if u[i] == 0x48 {
			return
		}
	}
	r.SetUnknown(append(u, champ9Present...))
}

// dureeMiniRecherche : duree minimale d'un TrackMatchmakingTicket avant d'annoncer le SUCCEEDED.
// Drapeau a chaud « mmlent » (defaut 14 s, proche des 13,8 s mesures chez Nintendo) ; la valeur
// est reglable par « mmlent=<secondes> » pour balayer la plage sans redeployer.
func dureeMiniRecherche() time.Duration {
	v := soirFlagValeur("mmlent")
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 14 * time.Second
}

// dureeMaxFlux : duree au-dela de laquelle un flux KeepUserSession qui n'aboutit pas est referme.
// Drapeau a chaud « fluxmax=<secondes> ». Absent = pas de limite (comportement d'origine).
func dureeMaxFlux() time.Duration {
	v := soirFlagValeur("fluxmax")
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 180 * time.Second
}
