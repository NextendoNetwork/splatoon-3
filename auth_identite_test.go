package main

// Verrou sur le REFUS des identites non resolues.
//
// L'ancien repli rendait pid=0 et l'identite du compte CAPTURE — un compte qui porte une vraie
// progression. Le 2026-08-21 un joueur s'est connecte et a trouve la sauvegarde de quelqu'un
// d'autre ; deux joueurs tombes dedans en meme temps s'ecrasent mutuellement. Mesure du 22/08 : le
// repli se declenchait environ deux fois par heure.

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIdentiteNonResolueEstRefusee(t *testing.T) {
	t.Setenv("NPLN_ALLOW_UNVERIFIED", "")
	pid, chemin, err := gatedIdentity(nil, "tenants/t-dce9377b-lp1")

	if err == nil {
		t.Fatal("une identite non resolue doit etre REFUSEE, pas servie sous une autre identite")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("code %s, attendu PermissionDenied", status.Code(err))
	}
	if pid != 0 || chemin != "" {
		t.Errorf("refus mais pid=%d chemin=%q : rien ne doit etre rendu", pid, chemin)
	}
	// Et surtout : le compte capture ne doit apparaitre nulle part.
	if strings.Contains(chemin, capturedUserGameRecord) || strings.Contains(chemin, capturedUserFriends) {
		t.Error("le compte capture est encore servi : c'est exactement le defaut qu'on ferme")
	}
	if !strings.Contains(err.Error(), "Nextendo") {
		t.Errorf("message %q : il doit dire au joueur quoi faire", err.Error())
	}
}

func TestDrapeauRestaureLAncienRepli(t *testing.T) {
	// Le drapeau existe pour revenir en arriere en une seconde si le refus fermait la porte a des
	// joueurs legitimes. Il doit donc marcher — et rester eteint par defaut.
	if soirFlag("replicapture") {
		t.Error("le drapeau replicapture est ALLUME par defaut : le refus ne s'applique pas")
	}
}
