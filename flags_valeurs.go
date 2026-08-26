package main

// La valeur d'un drapeau, DANS SA CASSE D'ORIGINE.
//
// ⚠️ LE PROBLEME QUE CA CORRIGE. Le cache des drapeaux range chaque ligne du fichier en minuscules,
// valeur comprise. C'est sans consequence pour un drapeau qui ne porte qu'un nombre — « festmi=259200 »
// se lit pareil dans les deux sens — mais destructeur des que la valeur est du texte :
//
//	festid=JUEA-00209            revenait « juea-00209 », et il fallait le remettre en capitales
//	                             a chaque lecture, sans quoi le jeu ne reconnaissait pas son paquet ;
//	festcamps=Perle & Coralie    reviendrait « perle & coralie », c'est-a-dire un nom d'equipe
//	                             affiche en minuscules dans le tableau de bord.
//
// On conserve donc les deux lectures : le cache « actif » pour savoir qu'un drapeau est present, et
// cette table-ci pour le lire tel qu'il a ete ECRIT.

import (
	"bufio"
	"bytes"
	"strings"
)

// valeursDesDrapeaux relit le fichier et rend « nom en minuscules » -> « valeur telle qu'ecrite ».
// Un drapeau sans « = » vaut "1", comme avant.
func valeursDesDrapeaux(b []byte) map[string]string {
	out := map[string]string{}
	if len(b) == 0 {
		return out
	}

	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		ligne := strings.TrimSpace(strings.SplitN(sc.Text(), "#", 2)[0])
		if ligne == "" {
			continue
		}
		nom, val, avecValeur := strings.Cut(ligne, "=")
		if !avecValeur {
			val = "1"
		}
		out[strings.ToLower(strings.TrimSpace(nom))] = strings.TrimSpace(val)
	}

	return out
}
