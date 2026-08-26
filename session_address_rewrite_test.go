package main

// Verrouille la redirection de la session sur la CAPTURE REELLE : apres reecriture, la reponse de
// Nintendo doit pointer vers notre hote et notre port, et rien d'autre ne doit bouger.

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// adresseDeSession relit l'hote et le port dans le champ 6 d'un message, comme le fait le jeu.
func adresseDeSession(t *testing.T, msg []byte) (string, uint64) {
	t.Helper()

	var hote string
	var port uint64
	for reste := msg; len(reste) > 0; {
		num, typ, n := protowire.ConsumeTag(reste)
		if n < 0 {
			t.Fatalf("etiquette illisible")
		}
		reste = reste[n:]
		if num == champGameSession && typ == protowire.BytesType {
			gs, m := protowire.ConsumeBytes(reste)
			reste = reste[m:]
			for sous := gs; len(sous) > 0; {
				snum, styp, sn := protowire.ConsumeTag(sous)
				if sn < 0 {
					t.Fatalf("sous-etiquette illisible")
				}
				sous = sous[sn:]
				switch {
				case snum == champHote && styp == protowire.BytesType:
					v, m := protowire.ConsumeBytes(sous)
					hote = string(v)
					sous = sous[m:]
				case snum == champPort && styp == protowire.VarintType:
					v, m := protowire.ConsumeVarint(sous)
					port = v
					sous = sous[m:]
				default:
					sous = sous[protowire.ConsumeFieldValue(snum, styp, sous):]
				}
			}
			continue
		}
		reste = reste[protowire.ConsumeFieldValue(num, typ, reste):]
	}
	return hote, port
}

func TestRedirectionSession_SurLaCaptureReelle(t *testing.T) {
	capture := rawTrackGameSessionCreationTicket
	if len(capture) == 0 {
		t.Skip("capture absente")
	}
	// Le fichier embarque est un message nu (pas de cadre gRPC) : on le lit tel quel.
	hote, port := adresseDeSession(t, capture)
	if hote != "35.223.226.237" || port != 7090 {
		t.Fatalf("capture inattendue : %s:%d (attendu l'hote Agones de Nintendo)", hote, port)
	}

	reecrit, err := reecrireAdresseSession(capture, "203.0.113.7", 7575)
	if err != nil {
		t.Fatalf("reecriture : %v", err)
	}

	hote, port = adresseDeSession(t, reecrit)
	if hote != "203.0.113.7" || port != 7575 {
		t.Fatalf("apres reecriture : %s:%d, attendu 203.0.113.7:7575", hote, port)
	}
	if bytes.Contains(reecrit, []byte("35.223.226.237")) {
		t.Fatal("l'hote de Nintendo subsiste dans le message")
	}
	// Un hote plus long que celui de la capture doit passer aussi : c'est tout l'interet de
	// reserialiser au lieu de remplacer des octets.
	long, err := reecrireAdresseSession(capture, "npln.nextendo.network", 7575)
	if err != nil {
		t.Fatalf("reecriture (hote long) : %v", err)
	}
	if h, p := adresseDeSession(t, long); h != "npln.nextendo.network" || p != 7575 {
		t.Fatalf("hote long : %s:%d", h, p)
	}
	// Le reste du message doit survivre : le nom de la session est dans le meme champ 6.
	if !bytes.Contains(reecrit, []byte("gameSessions/")) {
		t.Fatal("le nom de la session a disparu de la reecriture")
	}
}

func TestGreffeIdentifiants_SurLaCaptureReelle(t *testing.T) {
	capture := rawTrackGameSessionCreationTicket
	if len(capture) == 0 {
		t.Skip("capture absente")
	}

	const (
		session = "tenants/t-dce9377b-lp1/gameSessions/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		userSes = session + "/userSessions/11111111-2222-3333-4444-555555555555"
		jeton   = "un.jeton.que-nous-signons-nous-memes"
	)

	out := capture
	for _, g := range []struct {
		chemin []protowire.Number
		val    string
	}{
		{[]protowire.Number{6, 1}, session},
		{[]protowire.Number{5, 2}, userSes},
		{[]protowire.Number{5, 3}, jeton},
	} {
		var err error
		if out, err = reecrireChaine(out, g.chemin, g.val); err != nil {
			t.Fatalf("greffe %v : %v", g.chemin, err)
		}
	}
	out, err := reecrireAdresseSession(out, "203.0.113.7", 7575)
	if err != nil {
		t.Fatalf("adresse : %v", err)
	}

	// Plus rien de Nintendo ne doit subsister dans ce que le client va lire pour se connecter.
	for _, interdit := range []string{
		"gameSessions/1559755f-5154-45c8-92f7-4baa78702243",
		"userSessions/34c551db-be52-479b-84e6-b466c7438b89",
		"35.223.226.237",
	} {
		if bytes.Contains(out, []byte(interdit)) {
			t.Fatalf("identifiant de Nintendo encore present : %s", interdit)
		}
	}
	if !bytes.Contains(out, []byte(session)) || !bytes.Contains(out, []byte(jeton)) {
		t.Fatal("nos identifiants ne sont pas dans le message")
	}
	// La structure riche doit survivre : c'est la seule raison de rejouer la capture.
	for _, garde := range []string{"matchmakingConfigs/", "_BaseConfigName", "gameSessionCreationTickets/"} {
		if !bytes.Contains(out, []byte(garde)) {
			t.Fatalf("la greffe a emporte %q avec elle", garde)
		}
	}
	if h, p := adresseDeSession(t, out); h != "203.0.113.7" || p != 7575 {
		t.Fatalf("adresse apres greffe : %s:%d", h, p)
	}
}
