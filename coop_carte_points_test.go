package main

// Verrou sur la CARTE DE POINTS de Salmon Run — le « Bonus Meter ».
//
// Le 2026-08-21, quatre joueurs ont fini leur quart avec zero point, et leur catalogue n'a pas
// bouge. Cause mesuree : le jeu LIT ses deux cartes et ne les ecrit jamais ; c'est le serveur qui
// les tient (capture Nintendo : « 0 puis 509 octets » sur la carte du creneau). Nous ne les
// tenions pas.
//
// Les valeurs de reference ci-dessous sont decodees de la capture, pas inventees.

import (
	"os"
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

// cartesNeuves efface les cartes de ce joueur, en memoire ET sur disque.
//
// Depuis que storeGetDocument se replie sur le disque, un test qui laisse ses documents derriere
// lui fausse la course suivante : la carte relue s'ajoute a la nouvelle. Le premier passage
// reussissait, le second doublait les compteurs — c'est exactement ce qui est arrive le 21/08.
func cartesNeuves(t *testing.T, uid string, creneaux ...string) {
	t.Helper()
	noms := []string{nomCarteTotale(uid)}
	for _, c := range creneaux {
		noms = append(noms, nomCarteCreneau(uid, c))
	}
	documentStore.mu.Lock()
	for _, n := range noms {
		delete(documentStore.docs, n)
	}
	documentStore.mu.Unlock()
	for _, n := range noms {
		_ = os.Remove(documentPath(n))
	}
	dejaCredite.Lock()
	dejaCredite.m = map[string]bool{}
	dejaCredite.Unlock()
}

func TestCarteDePointsCumuleUnQuart(t *testing.T) {
	uid := "u-carte-test-1"
	cartesNeuves(t, uid, "20260806160000")
	g := gainCoop{
		oeufsOr: 62, oeufs: 2313, sauvetages: 0, boss: 0,
		orLivre: 62, vagues: 2, gradePt: 260, grade: 2,
		shiftID: "20260806160000", shiftTime: "20260806160000-20260808080000", typeQuart: "Normal",
	}
	crediterCartesDePoints(uid, g)

	tot, ok := storeGetDocument(nomCarteTotale(uid))
	if !ok {
		t.Fatal("la carte cumulee n'a pas ete creee — le compteur du joueur resterait a zero")
	}
	for cle, veut := range map[string]int64{
		"job_num": 1, "golden_ikura_total": 62, "ikura_total": 2313,
		"rescue_total": 0, "boss_total": 0, "version": versionCarteDePoints,
	} {
		if got := entierDuDoc(tot, cle); got != veut {
			t.Errorf("carte cumulee %s = %d, attendu %d", cle, got, veut)
		}
	}
	// 62 oeufs livres, quart reussi : 2x62 + 50 = 174. La capture donne 177 pour ce quart-la.
	if got := entierDuDoc(tot, "kuma_point"); got != pointsDuQuart(62, 2) {
		t.Errorf("kuma_point = %d, attendu %d", got, pointsDuQuart(62, 2))
	}

	// La carte du creneau porte les huit champs de plus releves sur la capture.
	cre, ok := storeGetDocument(nomCarteCreneau(uid, "20260806160000"))
	if !ok {
		t.Fatal("la carte du creneau n'a pas ete creee")
	}
	for cle, veut := range map[string]int64{
		"job_num": 1, "total_clear_wave": 2, "total_grade_point": 260,
		"best_grade_point": 260, "best_golden_ikura_num": 62, "grade": 2,
	} {
		if got := entierDuDoc(cre, cle); got != veut {
			t.Errorf("carte du creneau %s = %d, attendu %d", cle, got, veut)
		}
	}
	f := cre.GetFields().GetFields()
	if f["shift_id"].GetStringValue() != "20260806160000" {
		t.Errorf("shift_id = %q — c'est LUI qui range la carte, le jeu ne la retrouverait pas",
			f["shift_id"].GetStringValue())
	}
	if f["job_type_string"].GetStringValue() != "Normal" {
		t.Errorf("job_type_string = %q, attendu Normal", f["job_type_string"].GetStringValue())
	}
}

func TestCarteDePointsAdditionneLesQuarts(t *testing.T) {
	uid := "u-carte-test-2"
	cartesNeuves(t, uid, "20260806160000")
	base := gainCoop{vagues: 2, gradePt: 260, grade: 2, shiftID: "20260806160000"}

	q1 := base
	q1.oeufsOr, q1.oeufs, q1.sauvetages = 62, 2313, 0
	crediterCartesDePoints(uid, q1)

	q2 := base
	q2.oeufsOr, q2.oeufs, q2.sauvetages = 59, 1687, 7
	q2.gradePt = 240
	crediterCartesDePoints(uid, q2)

	tot, _ := storeGetDocument(nomCarteTotale(uid))
	for cle, veut := range map[string]int64{
		"job_num": 2, "golden_ikura_total": 121, "ikura_total": 4000, "rescue_total": 7,
	} {
		if got := entierDuDoc(tot, cle); got != veut {
			t.Errorf("apres deux quarts, %s = %d, attendu %d", cle, got, veut)
		}
	}

	// Le meilleur score du creneau ne recule pas quand le second quart est moins bon.
	cre, _ := storeGetDocument(nomCarteCreneau(uid, "20260806160000"))
	if got := entierDuDoc(cre, "best_grade_point"); got != 260 {
		t.Errorf("best_grade_point = %d, attendu 260 : un mauvais quart ne doit pas effacer le meilleur", got)
	}
	if got := entierDuDoc(cre, "best_golden_ikura_num"); got != 62 {
		t.Errorf("best_golden_ikura_num = %d, attendu 62", got)
	}
	if got := entierDuDoc(cre, "total_grade_point"); got != 240 {
		t.Errorf("total_grade_point = %d, attendu 240 : c'est la note COURANTE, pas la meilleure", got)
	}
}

func TestCarteDePointsNeCrediteQuUneFois(t *testing.T) {
	// L'arbitrage rejoue a chaque rapport recu : le meme match a produit TROIS verdicts status=1 le
	// 2026-08-21. Sans garde-fou, le quart serait credite trois fois.
	uid := "u-carte-test-3"
	cartesNeuves(t, uid, "20260806160000")
	cle := "20260821T211331_5a9ecfad"
	verdict := map[string]*commonpb.Value{
		"shift_id": gsStr("20260806160000"),
		"team_result": gsMap(map[string]*commonpb.Value{
			"total_clear_wave":  gsInt(3),
			"total_grade_point": gsInt(260),
			"grade":             gsInt(2),
		}),
		"personal_result": gsMap(map[string]*commonpb.Value{
			"session-a": gsMap(map[string]*commonpb.Value{
				"personal_wave_result": gsMap(map[string]*commonpb.Value{
					"golden_ikura_num": gsInt(20),
					"ikura_num":        gsInt(700),
					"rescue_num":       gsInt(1),
				}),
			}),
		}),
	}
	parSession := func(string) string { return uid }

	for i := 0; i < 3; i++ {
		crediterQuartCoop(cle, verdict, parSession)
	}

	tot, ok := storeGetDocument(nomCarteTotale(uid))
	if !ok {
		t.Fatal("aucune carte creee")
	}
	if got := entierDuDoc(tot, "job_num"); got != 1 {
		t.Fatalf("job_num = %d apres trois arbitrages du MEME match, attendu 1", got)
	}
	// Les compteurs se lisent en profondeur : ils vivent dans personal_wave_result, pas a plat.
	if got := entierDuDoc(tot, "golden_ikura_total"); got != 20 {
		t.Errorf("golden_ikura_total = %d, attendu 20 — les compteurs imbriques ne sont pas lus", got)
	}
	if got := entierDuDoc(tot, "ikura_total"); got != 700 {
		t.Errorf("ikura_total = %d, attendu 700", got)
	}
}

func TestFormuleDesPointsGrizzco(t *testing.T) {
	// Les dix-sept quarts de la capture, decodes structurellement. La regle retenue est
	// « 2 par oeuf d'or LIVRE, plus 50 si le quart est reussi ».
	//
	// ⚠️ Deux baremes faux ont precede celui-ci : 60 points par vague (deduit de deux quarts), puis
	// une mediane par vague (qui ignorait les oeufs). Ce test les interdit tous les deux.
	quarts := []struct {
		vague, oeufsLivres, reel int64
	}{
		{2, 62, 177}, {2, 55, 167}, {2, 59, 167}, {2, 57, 165}, {2, 61, 165}, {2, 52, 161},
		{2, 54, 150}, {2, 55, 150}, {2, 46, 134}, {2, 36, 118}, {2, 19, 90},
		{1, 27, 57}, {1, 17, 38}, {1, 9, 17}, {0, 3, 6}, {0, 0, 0},
	}
	var total int64
	for _, q := range quarts {
		p := pointsDuQuart(q.oeufsLivres, q.vague)
		e := p - q.reel
		if e < 0 {
			e = -e
		}
		total += e
		if e > 12 {
			t.Errorf("quart vague=%d oeufs=%d : modele %d contre %d releves (ecart %d)",
				q.vague, q.oeufsLivres, p, q.reel, e)
		}
	}
	if moy := total / int64(len(quarts)); moy > 7 {
		t.Errorf("ecart moyen %d points, mesure a 6 sur la capture", moy)
	}

	// Un quart echoue n'a PAS de prime : c'est le regime ou l'ajustement est quasi exact.
	if pointsDuQuart(27, 1) != 54 {
		t.Errorf("quart echoue : %d, attendu 2 x 27 sans prime", pointsDuQuart(27, 1))
	}
	// Les deux anciens calculs ne doivent plus jamais reapparaitre.
	if pointsDuQuart(0, 1) == 60 || pointsDuQuart(0, 2) == 155 {
		t.Error("un ancien bareme est revenu : les points ne dependent plus des seules vagues")
	}
}
