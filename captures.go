package main

// Les reponses mesurees ne sont PAS distribuees avec ce depot.
//
// Plusieurs reponses de ce serveur ont ete etablies en observant le vrai service NPLN. Ces octets
// appartiennent a Nintendo : ils ne sont pas redistribuables, et ce depot n'en contient aucun.
// Le code qui s'en sert les charge donc au DEMARRAGE depuis un dossier que l'operateur fournit
// lui-meme, au lieu de les embarquer dans le binaire.
//
// Disposition attendue, a la racine du serveur (ou sous NPLN_CAPTURES_DIR) :
//
//	captured/<Methode>.bin          une reponse protobuf brute, une par fichier
//	captured_boot/<service>.<Methode>.grpc   un flux gRPC brut (prefixe de 5 octets par message)
//
// Ce qui manque n'empeche jamais le serveur de demarrer : chaque appelant teste le resultat et
// retombe sur son propre chemin. Les fonctions qui n'ont pas d'autre chemin le disent dans le
// journal, en nommant le fichier attendu, plutot que de servir une reponse vide en silence.

import (
	"log"
	"os"
	"path/filepath"
	"sync"
)

// racineDesCaptures : ou chercher « captured/ » et « captured_boot/ ». Reglable par la variable
// d'environnement NPLN_CAPTURES_DIR ; par defaut le repertoire courant du serveur.
func racineDesCaptures() string {
	if v := os.Getenv("NPLN_CAPTURES_DIR"); v != "" {
		return v
	}

	return "."
}

// magasinDeCaptures rend un ReadFile de meme signature que embed.FS, pour que les appelants
// n'aient pas a changer : ils testaient deja l'erreur et retombaient sur leur propre chemin.
type magasinDeCaptures struct{ racine string }

func (m magasinDeCaptures) ReadFile(nom string) ([]byte, error) {
	return os.ReadFile(filepath.Join(m.racine, filepath.FromSlash(nom)))
}

// capturedBoot remplace l'ancien embed.FS des flux de demarrage.
var capturedBoot = magasinDeCaptures{racine: racineDesCaptures()}

// manquantsSignales evite de repeter la meme ligne a chaque appel.
var manquantsSignales sync.Map

// capture rend le contenu d'un fichier mesure, ou nil s'il est absent. L'absence est journalisee
// UNE FOIS, en nommant le chemin attendu : un serveur qui degrade son comportement doit le dire.
func capture(chemin string) []byte {
	b, err := os.ReadFile(filepath.Join(racineDesCaptures(), filepath.FromSlash(chemin)))
	if err != nil {
		if _, deja := manquantsSignales.LoadOrStore(chemin, true); !deja {
			log.Printf("[NPLN captures] %s absent — la fonction qui s'en sert sera degradee. "+
				"Voir captures.go : ce depot ne redistribue aucune donnee mesuree.", chemin)
		}

		return nil
	}

	return b
}
