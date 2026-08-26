package main

// Verrou sur le CLOISONNEMENT de la messagerie de hall.
//
// Signale le 2026-08-22 : un joueur voyait dans ses notifications l'annonce de match prive d'un
// inconnu qui n'etait meme pas son ami. Cause : un canal d'echo UNIQUE pour tout le serveur. Chaque
// SendMessage y etait depose et n'importe quel RecvMessage le ramassait — l'invitation partait donc
// chez un joueur tire au sort par l'ordonnanceur.

import "testing"

func TestUnJoueurNeRecoitQueSesPropresEchos(t *testing.T) {
	a, b := "u-hall-a", "u-hall-b"
	defer oublierCanalDeHall(a)
	defer oublierCanalDeHall(b)

	canalDeHall(a) <- []byte("invitation de A")

	// B ne doit RIEN recevoir : c'est tout l'objet du correctif.
	select {
	case fuite := <-canalDeHall(b):
		t.Fatalf("B a recu %q, qui appartient a A — la fuite est de retour", fuite)
	default:
	}

	// A, lui, recoit bien son echo.
	select {
	case recu := <-canalDeHall(a):
		if string(recu) != "invitation de A" {
			t.Errorf("A recoit %q, attendu son propre message", recu)
		}
	default:
		t.Error("A n'a pas recu son echo : le cloisonnement a coupe le fil au lieu de le rediriger")
	}
}

func TestChaqueJoueurSonCanal(t *testing.T) {
	a, b := "u-hall-c", "u-hall-d"
	defer oublierCanalDeHall(a)
	defer oublierCanalDeHall(b)
	if canalDeHall(a) == canalDeHall(b) {
		t.Fatal("deux joueurs partagent le meme canal : leurs notifications se melangeront")
	}
	// Le meme joueur retrouve TOUJOURS son canal, sinon ses echos se perdraient.
	if canalDeHall(a) != canalDeHall(a) {
		t.Error("le canal d'un joueur change d'un appel a l'autre")
	}
	// Et il disparait quand il s'en va.
	oublierCanalDeHall(a)
	echosDeHall.Lock()
	_, reste := echosDeHall.m[a]
	echosDeHall.Unlock()
	if reste {
		t.Error("le canal survit au depart du joueur : fuite de memoire a chaque connexion")
	}
}

func TestDeposerSiPresentNeCreePasDeCanal(t *testing.T) {
	// Distribuer a une liste d'amis creerait un canal par ami HORS LIGNE si on employait
	// canalDeHall : jamais lu, jamais libere, une fuite a chaque invitation.
	absent := "u-hall-absent"
	if deposerSiPresent(absent, []byte("coucou")) {
		t.Error("un joueur hors ligne ne doit rien recevoir")
	}
	echosDeHall.Lock()
	_, cree := echosDeHall.m[absent]
	echosDeHall.Unlock()
	if cree {
		t.Fatal("un canal a ete cree pour un joueur hors ligne : fuite a chaque publication")
	}

	// Et un joueur en ligne le recoit bien.
	present := "u-hall-present"
	_ = canalDeHall(present)
	defer oublierCanalDeHall(present)
	if !deposerSiPresent(present, []byte("invitation")) {
		t.Error("un joueur en ligne doit recevoir la publication")
	}
	if recu := <-canalDeHall(present); string(recu) != "invitation" {
		t.Errorf("recu %q", recu)
	}
}

func TestLAuteurRecoitTouloursSonEcho(t *testing.T) {
	// C'est son propre echo qui debloque son ecran de connexion : il ne doit jamais dependre de
	// la liste d'amis, qui part dans une autre routine et peut echouer.
	moi := "u-hall-auteur"
	_ = canalDeHall(moi)
	defer oublierCanalDeHall(moi)

	distribuerAuxAmis(moi, []byte("ma publication"))

	select {
	case recu := <-canalDeHall(moi):
		if string(recu) != "ma publication" {
			t.Errorf("recu %q", recu)
		}
	default:
		t.Fatal("l'auteur n'a pas recu son echo")
	}
}
