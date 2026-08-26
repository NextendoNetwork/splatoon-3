package main

// /api/fest-logo — quel logo d'ecran-titre afficher, s'il y en a un.
//
// POURQUOI ICI, ET PAS A COTE. Le choix voyage avec la POSTURE DE FETE, dans soir.flags, aux cotes
// de festid, festressource et festcles. Pose ailleurs, il survivrait a la fin de la fete et le
// logo resterait coince sur un evenement termine. Ici, il s'eteint tout seul : des que la fete
// n'est plus annoncee, cette route rend une reponse vide.
//
// CE QUE L'EMULATEUR EN FAIT. Il permute deux textures que le jeu livre deja — Splatoween, Frosty
// Fest, Spring Fest, Summer Nights, Grand Festival — ou, pour LoveFest, ecrit un bloc qu'il
// apporte. Rien n'est distribue du fichier du joueur, et un nom inconnu ne fait RIEN : le defaut
// est de laisser l'ecran-titre tranquille.

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// logosConnus : les seuls noms que l'emulateur sait traiter. On refuse ici ce qu'il refuserait
// la-bas, pour que le journal du serveur dise tout de suite qu'une valeur est fautive.
var logosConnus = map[string]bool{
	"splatoween": true,
	"frosty":     true,
	"spring":     true,
	"summer":     true,
	"grand":      true,
	"lovefest":   true,
}

// logoDeFete rend le logo demande, ou "" si rien ne doit changer.
// logoDeFete rend le logo de l'instant present.
func logoDeFete() string { return logoDeFeteA(time.Now()) }

// logoDeFeteA repond pour un instant DONNE. L'heure est un parametre et non l'horloge, pour qu'un
// test puisse verifier l'allumage et l'extinction sans attendre le jour dit — ce qui compte quand
// la bascule doit tomber juste a une heure precise et qu'on n'a qu'un seul essai.
func logoDeFeteA(maintenant time.Time) string {
	// ⚠️ ACCROCHE AU LANCEMENT, PAS A L'ANNONCE.
	//
	// Cette porte lisait le drapeau « fest », qui s'allume des l'ANNONCE — soit, pour la fete du
	// 25 aout, douze heures avant le premier match. L'ecran-titre portait donc le logo d'un
	// evenement qui n'avait pas commence. Il suit desormais exactement la fenetre des matchs
	// (voir fest_phase.go) : il s'allume tout seul a l'heure de debut et s'eteint tout seul a la
	// fin, sans qu'aucun drapeau soit a poser le jour J.
	// « festlogotest » montre le logo HORS de la fenetre des matchs, pour pouvoir le VERIFIER
	// avant l'evenement : on n'a qu'un seul essai le jour dit, et un logo qui ne s'affiche pas se
	// constate a l'ecran, jamais dans un journal. A RETIRER apres l'essai — laisse en place, il
	// remet exactement le defaut qu'on vient de corriger : un logo d'evenement pas commence.
	if !matchsDeFeteEnCours(maintenant) && !soirFlag("festlogotest") {
		return ""
	}

	v := strings.ToLower(strings.TrimSpace(soirFlagValeur("festlogo")))
	if v == "" || v == "1" || v == "default" {
		return ""
	}

	if !logosConnus[v] {
		log.Printf("[NPLN fest-logo] valeur inconnue %q — ignoree, l'ecran-titre reste intact", v)

		return ""
	}

	return v
}

func festLogoHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	// Etat vivant, comme la fete elle-meme : on ne le met pas en cache.
	w.Header().Set("Cache-Control", "no-store")

	_ = json.NewEncoder(w).Encode(map[string]string{"logo": logoDeFete()})
}
