package main

import "testing"

// exigeCapture fait PASSER SON TOUR au test quand la reponse mesuree dont il depend n'est pas la.
//
// Ce depot ne redistribue aucun octet mesure chez Nintendo (voir captures.go). Les tests qui
// rejouent une capture ne peuvent donc pas s'executer sur un clone nu — mais ils ne doivent pas
// ECHOUER pour autant : un test rouge par defaut est un test qu'on cesse de lire. Ils sautent, en
// nommant le fichier attendu, et redeviennent actifs des que l'operateur fournit ses propres
// captures dans NPLN_CAPTURES_DIR.
func exigeCapture(t *testing.T, donnees []byte, chemin string) {
	t.Helper()
	if len(donnees) == 0 {
		t.Skipf("capture absente (%s) — ce depot n'en redistribue aucune, voir captures.go", chemin)
	}
}
