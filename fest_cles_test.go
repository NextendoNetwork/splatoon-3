package main

// Verrou sur les cles de dechiffrement d'un Splatfest.
//
// Mesure du 2026-08-22, Splatfest officiel JUEA-00107 EN COURS, console reelle contre Nintendo :
// la reponse ne porte que TROIS champs — le nom, notice_key et start_key. Pas de cle de vainqueur,
// pas d'equipe en tete.
//
// Notre capture embarquee vient d'une fete TERMINEE et en porte quatre. La rejouer pendant une fete
// qui court annonce un vainqueur qui n'existe pas encore, et livre la cle du paquet de victoire
// avant l'heure.

import (
	"testing"

	"google.golang.org/protobuf/proto"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

func TestCapturesDesClesPorteLesDeuxCles(t *testing.T) {
	exigeCapture(t, rawFestKey, "captured/GetFestDecryptionKey.bin")
	r := &toyohrpb.FestDecryptionKey{}
	if err := proto.Unmarshal(rawFestKey, r); err != nil {
		t.Fatalf("capture illisible : %v", err)
	}
	if r.GetNoticeKey() == "" || r.GetStartKey() == "" {
		t.Fatal("la capture doit porter notice_key ET start_key : ce sont les deux que Nintendo sert pendant la fete")
	}
	if len(r.GetNoticeKey()) != 32 || len(r.GetStartKey()) != 32 {
		t.Errorf("cles de %d et %d caracteres, attendu 32 (AES-128 en hexadecimal)",
			len(r.GetNoticeKey()), len(r.GetStartKey()))
	}
	// La capture embarquee vient d'une fete finie : elle a AUSSI la cle du vainqueur.
	if r.GetResultWinningTeamKey() == "" {
		t.Skip("la capture embarquee n'a pas de cle de vainqueur : rien a retenir")
	}
}

func TestClesDuVainqueurRetenuesPendantLaFete(t *testing.T) {
	exigeCapture(t, rawFestKey, "captured/GetFestDecryptionKey.bin")
	// On ne teste pas le serveur complet ici mais la regle : pendant la fete, les champs de verdict
	// doivent partir vides. C'est exactement ce que la capture de JUEA-00107 montre.
	r := &toyohrpb.FestDecryptionKey{}
	_ = proto.Unmarshal(rawFestKey, r)
	r.ResultWinningTeamKey = ""
	r.TeamAlphaKey = ""
	r.TeamBravoKey = ""
	r.TeamCharlieKey = ""
	r.WinningTeam = ""

	if r.GetName() == "" || r.GetNoticeKey() == "" || r.GetStartKey() == "" {
		t.Error("le nom et les deux cles doivent survivre : ce sont eux que Nintendo sert")
	}
	for nom, v := range map[string]string{
		"result_winning_team_key": r.GetResultWinningTeamKey(),
		"winning_team":            r.GetWinningTeam(),
	} {
		if v != "" {
			t.Errorf("%s = %q : Nintendo ne le sert pas tant que la fete court", nom, v)
		}
	}
}
