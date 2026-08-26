package main

// Journaliser les secrets de session TLS, pour pouvoir DECHIFFRER nos propres captures.
//
// POURQUOI. Une capture de paquets sur le serveur montre le comportement du transport — qui
// raccroche, quand, avec quelles retransmissions — mais pas le CONTENU : tout est chiffre. Or c'est
// le contenu qui dirait ce que le jeu a recu juste avant de partir, et ce qu'il attendait.
//
// La cle privee du certificat ne suffit pas : nos echanges negocient des cles ephemeres (ECDHE),
// justement pour qu'une cle volee ne dechiffre pas le passe. Ce qu'il faut, ce sont les secrets de
// CHAQUE session, que la bibliotheque TLS de Go sait ecrire au format NSS — celui que Wireshark et
// tshark lisent nativement.
//
// Avec ce fichier et une capture, on relit un echange complet, exactement comme le relais nous
// donnait le flux de la vraie Switch — mais pour TOUS les joueurs, et sans rien installer chez eux.
//
// ⚠️ CE FICHIER DECHIFFRE TOUT LE TRAFIC. Il contient de quoi relire les echanges de n'importe quel
// joueur : jetons d'acces, sauvegardes, tout. Il est donc :
//
//   - ETEINT par defaut, et ne s'allume que par le drapeau a chaud « clestls » ;
//   - ecrit en 0600, lisible du seul compte root ;
//   - a supprimer des qu'on a fini de diagnostiquer.
//
// Ce n'est pas un reglage a laisser en production.

import (
	"crypto/tls"
	"io"
	"log"
	"os"
	"sync"
)

const cheminClesTLS = "/data/captures-tcp/cles-tls.log"

var journalCles = struct {
	sync.Mutex
	f      *os.File
	ouvert bool
}{}

// ecrivainDeClesTLS rend le journal des secrets si le drapeau est leve, nil sinon.
//
// Go appelle KeyLogWriter a chaque poignee de main : le renvoyer nil coute exactement rien quand la
// journalisation est eteinte, ce qui est le cas par defaut.
func ecrivainDeClesTLS() io.Writer {
	if !soirFlag("clestls") {
		fermerJournalDesCles()
		return nil
	}

	journalCles.Lock()
	defer journalCles.Unlock()
	if journalCles.ouvert {
		return journalCles.f
	}

	f, err := os.OpenFile(cheminClesTLS, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Printf("[NPLN TLS] journal des cles impossible (%v) — capture non dechiffrable", err)
		return nil
	}
	journalCles.f = f
	journalCles.ouvert = true
	log.Printf("[NPLN TLS] ⚠️ journal des secrets de session OUVERT : %s — a eteindre apres diagnostic",
		cheminClesTLS)
	return f
}

// fermerJournalDesCles referme le fichier quand le drapeau retombe, pour que « eteindre le drapeau »
// suffise vraiment a arreter d'ecrire.
func fermerJournalDesCles() {
	journalCles.Lock()
	defer journalCles.Unlock()
	if !journalCles.ouvert {
		return
	}
	_ = journalCles.f.Close()
	journalCles.f = nil
	journalCles.ouvert = false
	log.Printf("[NPLN TLS] journal des secrets de session referme")
}

// avecJournalDesCles branche la journalisation sur une configuration TLS.
//
// On passe par GetConfigForClient plutot que par KeyLogWriter directement : le drapeau est relu a
// chaque poignee de main, donc l'allumer ou l'eteindre prend effet en une seconde, sans
// redemarrage. Une capture se decide rarement a l'avance.
func avecJournalDesCles(cfg *tls.Config) *tls.Config {
	cfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		w := ecrivainDeClesTLS()
		if w == nil {
			return nil, nil // nil = garder la configuration de base, sans journal
		}
		c := cfg.Clone()
		c.GetConfigForClient = nil // pas de recursion
		c.KeyLogWriter = w
		return c, nil
	}
	return cfg
}
