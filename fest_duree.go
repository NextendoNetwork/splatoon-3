package main

// Les DUREES d'un festival, reglables a chaud.
//
// Les constantes de fest_genere.go sont celles RELEVEES sur la capture Nintendo : 24 h jusqu'a la
// mi-parcours, 48 h jusqu'a la fin, 50 h jusqu'a la cloture. C'est le bareme du vrai service, et
// c'est le defaut — on n'y touche pas.
//
// Mais une fete de deux jours ne s'eprouve pas en deux jours. Pour tester la chaine complete
// — vote, intermede, tricolore, cloture — il faut pouvoir la comprimer, et pour annoncer une fete
// maison a nos joueurs il faut pouvoir choisir ses horaires plutot que subir un gabarit.
//
// Trois drapeaux, en SECONDES depuis le debut de la fete :
//
//	festmi=<n>        vers la mi-parcours   (defaut 86400)
//	festfin=<n>       vers la fin           (defaut 172800)
//	festcloture=<n>   vers la cloture       (defaut 180000)
//
// ⚠️ RESTER SUR LES CRENEAUX DE DEUX HEURES. Mesure du 2026-08-20 : Splatoon 3 range ses horaires
// par creneaux de 7200 secondes, et Nintendo aligne les cinq dates dessus. Une duree qui ne tombe
// pas sur un multiple de 7200 sortirait du gabarit que le jeu verifie — on la ramene donc au
// creneau inferieur, et on le DIT dans le journal plutot que de corriger en silence.

import (
	"log"
	"strconv"
	"sync"
)

// creneauFest : la taille du creneau, en secondes. Voir alignerSurCreneau dans fest_genere.go.
const creneauFest = 7200

var dureesAnnoncees = struct {
	sync.Mutex
	vu map[string]int64
}{vu: map[string]int64{}}

// dureeDeFete rend une duree reglee a chaud, ou son defaut mesure.
func dureeDeFete(drapeau string, defaut int64) int64 {
	v := soirFlagValeur(drapeau)
	if v == "" || v == "1" {
		return defaut
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return defaut
	}

	aligne := n - (n % creneauFest)
	if aligne <= 0 {
		aligne = creneauFest
	}

	dureesAnnoncees.Lock()
	if dureesAnnoncees.vu[drapeau] != aligne {
		dureesAnnoncees.vu[drapeau] = aligne
		if aligne != n {
			log.Printf("[NPLN fest] %s=%d ramene a %d s (creneaux de 2 h, comme Nintendo)", drapeau, n, aligne)
		} else {
			log.Printf("[NPLN fest] %s = %d s (%.1f h apres le debut)", drapeau, aligne, float64(aligne)/3600)
		}
	}
	dureesAnnoncees.Unlock()

	return aligne
}

func versMiParcours() int64 { return dureeDeFete("festmi", festVersMiParcours) }
func versFin() int64        { return dureeDeFete("festfin", festVersFin) }
func versCloture() int64    { return dureeDeFete("festcloture", festVersCloture) }
