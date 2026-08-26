package main

// Verrou sur l'inventaire des attributs de presence.
//
// Le journal n'imprimait que quatre attributs sur les treize envoyes. Le 2026-08-23, un joueur
// avait choisi son camp de Splatfest, le jeu le lui confirmait a l'ecran, et « FestTeam »
// n'apparaissait nulle part chez nous : impossible de distinguer « la console ne l'envoie pas » de
// « nous ne l'imprimons pas ». Un silence ne doit jamais avoir deux causes possibles.

import (
	"strings"
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

func TestInventaireMontreTousLesAttributs(t *testing.T) {
	// Forme relevee dans la capture Nintendo : la presence porte FestTeam et FestId.
	attrs := map[string]*commonpb.Value{
		"GameStatus":  {ValueType: &commonpb.Value_IntegerValue{IntegerValue: 1}},
		"FestTeam":    {ValueType: &commonpb.Value_IntegerValue{IntegerValue: 1}},
		"FestId":      {ValueType: &commonpb.Value_StringValue{StringValue: "JUEA-00201"}},
		"PlayerName":  {ValueType: &commonpb.Value_StringValue{StringValue: "Kazu"}},
		"UsePassword": {ValueType: &commonpb.Value_BooleanValue{BooleanValue: false}},
		"SessionId":   {ValueType: &commonpb.Value_StringValue{StringValue: ""}},
	}

	inv := inventaireDesAttributs(attrs)
	for _, attendu := range []string{"FestTeam=1", `FestId="JUEA-00201"`, "GameStatus=1", `PlayerName="Kazu"`} {
		if !strings.Contains(inv, attendu) {
			t.Errorf("inventaire sans %s : %s", attendu, inv)
		}
	}
	// Un booleen faux et un entier nul ne doivent pas se confondre.
	if !strings.Contains(inv, "UsePassword=false") {
		t.Errorf("booleen faux mal rendu : %s", inv)
	}
	// Ordre stable, sinon le cache de deduplication reimprimerait sans cesse.
	if inv != inventaireDesAttributs(attrs) {
		t.Error("inventaire instable d un appel a l autre")
	}
}

func TestJournalNeRepetePasLaMemePresence(t *testing.T) {
	derniereEmpreinteAttributs.Lock()
	derniereEmpreinteAttributs.m = map[string]string{}
	derniereEmpreinteAttributs.Unlock()

	if !attributsOntChange("u-a", "GameStatus=1") {
		t.Error("premiere publication : doit etre journalisee")
	}
	if attributsOntChange("u-a", "GameStatus=1") {
		t.Error("presence identique : ne doit PAS etre rejournalisee, les consoles republient en rafale")
	}
	if !attributsOntChange("u-a", "GameStatus=3") {
		t.Error("presence changee : doit etre journalisee")
	}
	if !attributsOntChange("u-b", "GameStatus=1") {
		t.Error("un autre joueur a son propre suivi")
	}
}
