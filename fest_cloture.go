package main

// La fin d'un festival, telle que Nintendo la joue.
//
// MESURE DU 2026-08-24, capture d'une vraie console apres l'ouverture des resultats du festival
// officiel JUEA-00107 : le relais de session
//
// GetFestDecryptionKey NE DIT PAS LA MEME CHOSE selon la phase :
//
//	pendant la fete, 128 o : champ 1 le nom, champ 2 la cle de base, champ 3 celle de l'intermede.
//	                        PAS de troisieme cle, PAS de champ 8.
//	aux resultats,   171 o : la meme chose, PLUS le champ 4 — la cle du paquet de victoire — et le
//	                        champ 8, qui NOMME le camp vainqueur.
//
// C'est donc par cet appel que le jeu apprend qui a gagne, et il ne recoit la cle du paquet de
// victoire qu'a ce moment-la. Nous servions les trois cles des le debut et un champ 8 portant le
// camp DEFENSEUR de l'intermede — deux ecarts avec le vrai serveur.
//
// ⚠️ Le champ 8 n'est PAS le meneur du vote preliminaire. Dans la capture, ce meneur est Alpha
// alors que le champ 8 vaut Charlie, qui est le vainqueur au total. Les deux coincidaient par
// hasard, et lire le champ 8 comme « meneur du yobisai » est une erreur.

import (
	"log"
	"time"
)

// resultatsOuverts dit si notre fete a franchi son heure de cloture, celle ou Nintendo publie le
// verdict — deux heures apres la fin, dans tous les calendriers qu'on a captures.
func resultatsOuverts() bool {
	if !festMaisonActif() {
		return false
	}
	t := festMaison(time.Now()).GetTimetable().GetCloseTime()
	return t != nil && !time.Now().Before(t.AsTime())
}

// campVainqueurDeLaFete rend le camp qui l'emporte, ou "" tant que rien ne permet de le dire.
//
// Notre fete n'a pas encore de batailles comptees : le classement de l'intermede, qui est FIGE,
// est donc la meilleure information disponible et ne bougera plus. Des que des batailles seront
// comptees, c'est le bareme complet qui prendra la main — voir fest_bareme.go.
func campVainqueurDeLaFete(festID string) string {
	// ⚠️ LE VAINQUEUR DOIT SORTIR DU MEME CALCUL QUE LES POINTS AFFICHES.
	//
	// Il venait de l'intermede fige, alors que les points venaient du bareme : le 2026-08-24
	// l'ecran donnait la victoire a un camp pendant que les idoles en annoncaient un autre, et la
	// cle servie ouvrait le paquet du second. Une seule source, desormais.
	_, _, total := classementsDeLaFete(festID)
	if len(total) == 0 {
		return ""
	}
	return classementParPoints(total)[0]
}

// clesAServir taille la liste des cles selon la phase, comme le vrai serveur.
func clesAServir(cles []string, festID string) ([]string, string) {
	if !resultatsOuverts() {
		if len(cles) > 2 {
			cles = cles[:2]
		}
		return cles, ""
	}
	vainqueur := campVainqueurDeLaFete(festID)
	log.Printf("[NPLN fest] resultats ouverts pour %s : %d cle(s) et le camp %q",
		festID, len(cles), vainqueur)
	return cles, vainqueur
}
