package main

// Verrou sur la forme du cadre SUCCEEDED de TrackMatchmakingTicket.
//
// Loi MESURÉE sur les 17 flux TrackMatchmakingTicket des 7 captures Nintendo
// (le corpus de captures NPLN*.json + session-2026-08-08_000317.bin.json) :
//
//	count(matched_user_sessions) == count(user_definitions), sans exception
//	  ticket f3179563 (Guerre de territoire, solo) : 1 <-> 1
//	  ticket e4f46c7a (groupe de 4)                : 4 <-> 4
//	chaque MatchedUserSession porte user_definition (sous-champ 1) ; le jeton (sous-champ 3)
//	n'est présent QUE sur l'entrée du destinataire (3 des 4 entrées du match de groupe n'en ont pas)
//	user_definition.user est CONCRET : "tenants/t-dce9377b-lp1/users/u-..." et jamais l'alias
//
// Nous violions les trois : 0 user_definition, 2 matched_user_sessions, 2 jetons, tous signés
// avec l'identité de la capture (capturedUser) parce que le ticket réel n'était jamais retrouvé.

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	mmpb "npln.nintendo.net/npln-practice/proto/matchmaking/v1"
)

// gssClaims décode (sans vérifier la signature — ce n'est pas l'objet du test) la charge utile du
// jeton gss, pour lire quelle identité nous avons réellement signée.
func gssClaims(t *testing.T, tok string) map[string]any {
	t.Helper()
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("jeton gss mal formé (%d segments) : %q", len(parts), tok)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("charge utile illisible : %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("charge utile non JSON : %v", err)
	}
	return m
}

func TestSucceededTicketCarriesEachPlayersOwnIdentity(t *testing.T) {
	t.Setenv("NPLN_JWT_KEY", filepath.Join(t.TempDir(), "test_es256.key"))

	// Deux joueurs distincts, chacun avec la user definition que S3 envoie réellement : l'ALIAS
	// déjà résolu vers le user concret par CreateMatchmakingTicket.
	mk := func(uid string, ms int32) *mmWaiter {
		return &mmWaiter{
			uid: uid,
			out: make(chan *mmpb.MatchmakingTicket, 1),
			ticket: &mmpb.MatchmakingTicket{
				Name:              npnTenant + "/matchmakingTickets/" + uuid4(),
				MatchmakingConfig: npnTenant + "/matchmakingConfigs/regular_match_config",
				State:             mmpb.MatchmakingTicket_SEARCHING,
				UserDefinitions: []*mmpb.UserDefinition{{
					User: npnTenant + "/users/" + uid,
					Attributes: &commonpb.MapValue{Fields: map[string]*commonpb.Value{
						"uid": vStr(uid),
					}},
					LatencyData: &mmpb.LatencyData{},
				}},
			},
		}
	}

	a := mk("u-exemple1000000000000", 30)
	b := mk("u-exemple4000000000000", 40)

	m := newMatchmaker()
	m.mu.Lock()
	m.waiting = []*mmWaiter{a, b}
	m.formMatchLocked("regular_match_config", "203.0.113.7", 7575)
	m.mu.Unlock()

	var gs string
	for _, w := range []*mmWaiter{a, b} {
		var got *mmpb.MatchmakingTicket
		select {
		case got = <-w.out:
		default:
			t.Fatalf("%s : aucun ticket SUCCEEDED livré", w.uid)
		}

		if got.GetState() != mmpb.MatchmakingTicket_SUCCEEDED {
			t.Fatalf("%s : état = %v, attendu SUCCEEDED", w.uid, got.GetState())
		}

		// Loi de la capture : autant de matched_user_sessions que de user_definitions.
		if a, b := len(got.GetUserDefinitions()), len(got.GetMatchedUserSessions()); a != b {
			t.Fatalf("%s : count(user_definitions)=%d != count(matched_user_sessions)=%d — "+
				"les 17 flux Nintendo sont toujours à égalité", w.uid, a, b)
		}
		if n := len(got.GetMatchedUserSessions()); n != 1 {
			t.Fatalf("%s : %d matched_user_sessions, attendu 1 (le ticket ne décrit que VOTRE "+
				"groupe, jamais les adversaires)", w.uid, n)
		}

		mus := got.GetMatchedUserSessions()[0]

		// Sous-champ 1 : présent dans 100 % des MatchedUserSession de la capture.
		if mus.GetUserDefinition() == nil {
			t.Fatalf("%s : matched_user_session sans user_definition — le client ne peut pas se "+
				"reconnaître dans le match", w.uid)
		}
		wantUser := npnTenant + "/users/" + w.uid
		if u := mus.GetUserDefinition().GetUser(); u != wantUser {
			t.Fatalf("%s : user = %q, attendu %q (concret, comme la réponse Nintendo)", w.uid, u, wantUser)
		}

		// La capture d'un match régulier porte team:"" — pas d'équipe inventée.
		if tm := mus.GetUserDefinition().GetTeam(); tm != "" {
			t.Fatalf("%s : team = %q, attendu \"\" pour regular_match_config", w.uid, tm)
		}

		// Le jeton doit porter l'identité de SON joueur, jamais le repli capturedUser.
		cl := gssClaims(t, mus.GetMatchmakingIdToken())
		if sub, _ := cl["sub"].(string); sub != w.uid {
			t.Fatalf("%s : jeton gss sub = %q, attendu %q (repli capturedUser = %q)",
				w.uid, sub, w.uid, capturedUser)
		}
		sync, _ := cl["gamesync"].(map[string]any)
		if u, _ := sync["uid"].(string); u != w.uid {
			t.Fatalf("%s : gamesync.uid = %q, attendu %q", w.uid, u, w.uid)
		}
		if tm, _ := sync["team"].(string); tm != "" {
			t.Fatalf("%s : gamesync.team = %q, attendu \"\"", w.uid, tm)
		}

		// Une seule et même GameSession pour les deux : c'est par elle qu'ils se rejoignent.
		if gs == "" {
			gs = got.GetGameSession().GetName()
		} else if got.GetGameSession().GetName() != gs {
			t.Fatalf("%s : game_session = %q, attendu la session partagée %q",
				w.uid, got.GetGameSession().GetName(), gs)
		}
		if h, p := got.GetGameSession().GetHost(), got.GetGameSession().GetPort(); h != "203.0.113.7" || p != 7575 {
			t.Fatalf("%s : hôte de session = %s:%d", w.uid, h, p)
		}
	}

	// Les deux joueurs ne doivent PAS partager la même userSession ni le même jeton.
	ta := <-func() chan *mmpb.MatchmakingTicket {
		c := make(chan *mmpb.MatchmakingTicket, 1)
		c <- a.ticket
		return c
	}()
	tb := <-func() chan *mmpb.MatchmakingTicket {
		c := make(chan *mmpb.MatchmakingTicket, 1)
		c <- b.ticket
		return c
	}()
	if ta.GetMatchedUserSessions()[0].GetUserSession() == tb.GetMatchedUserSessions()[0].GetUserSession() {
		t.Fatal("les deux joueurs partagent la même userSession")
	}
	if ta.GetMatchedUserSessions()[0].GetMatchmakingIdToken() == tb.GetMatchedUserSessions()[0].GetMatchmakingIdToken() {
		t.Fatal("les deux joueurs partagent le même jeton gss — c'était exactement le symptôme")
	}
}

// TestTicketLookupSurvivesTenantAlias : le client CRÉE sous "tenants/<tid>/..." (ce que nous lui
// rendons) et SUIT sous l'alias "tenants/current/...". Journal du 2026-08-13, même uuid :
//
//	18:58:26 CreateMatchmakingTicket -> tenants/t-dce9377b-lp1/matchmakingTickets/2322d7e7-...
//	18:58:26 TrackMatchmakingTicket     tenants/current/matchmakingTickets/2322d7e7-...
func TestTicketLookupSurvivesTenantAlias(t *testing.T) {
	m := newMatchmaker()
	concrete := npnTenant + "/matchmakingTickets/2322d7e7-ab38-4a67-a0dd-5e436690313c"
	alias := "tenants/current/matchmakingTickets/2322d7e7-ab38-4a67-a0dd-5e436690313c"

	m.tickets[lastSeg(concrete)] = &mmpb.MatchmakingTicket{Name: concrete}

	if m.tickets[lastSeg(alias)] == nil {
		t.Fatal("ticket introuvable depuis l'alias : Track repartirait sur le ticket de secours " +
			"sans user_definitions, et les deux joueurs recevraient l'identité de la capture")
	}
}
