package main

// Verrou sur le partage des votes d'un Splatfest.
//
// Mesure du 2026-08-22, Splatfest officiel JUEA-00107 en cours, console reelle contre Nintendo.
// GetFestResult rend alors un verdict REMPLI, et c'est ce qui nous manquait :
//
//	yobisai  is_valid = true, Alpha 0.340110 / Bravo 0.330110 / Charlie 0.329810  (somme 1,000030)
//	overall  { weights: {} }
//	unk      3
//	les phases non jouees restent absentes
//
// Nous rendions FestResult{Name} et rien d'autre — un objet sans le moindre chiffre. C'etait le mur
// de la phase « fete commencee ».

import (
	"math"
	"testing"
)

func votesDeTest(t *testing.T, festID string, parCamp map[string]int) {
	t.Helper()
	entreesFest.Lock()
	entreesFest.m = map[string]entreeFest{}
	entreesFest.chargé = true // on n'ira pas lire le disque
	n := 0
	for camp, combien := range parCamp {
		for i := 0; i < combien; i++ {
			n++
			entreesFest.m[festID+"/u-test"+strconv0(n)] = entreeFest{Equipe: camp, Region: "EU"}
		}
	}
	entreesFest.Unlock()
	t.Cleanup(func() {
		entreesFest.Lock()
		entreesFest.m = map[string]entreeFest{}
		entreesFest.chargé = false
		entreesFest.Unlock()
	})
}

func strconv0(n int) string {
	if n == 0 {
		return "0"
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestPartageDesVotesSuitLesInscriptions(t *testing.T) {
	votesDeTest(t, "JUEA-00201", map[string]int{"Alpha": 5, "Bravo": 3, "Charlie": 2})

	y := yobisaiDeLaFete("JUEA-00201")
	if !y.GetIsValid() {
		t.Error("is_valid doit etre vrai : la capture le porte a 1")
	}
	attendu := map[string]float64{"Alpha": 0.5, "Bravo": 0.3, "Charlie": 0.2}
	var somme float64
	for _, p := range y.GetTeamRatios() {
		somme += p.GetRatio()
		if math.Abs(p.GetRatio()-attendu[p.GetFestTeam()]) > 1e-9 {
			t.Errorf("%s : part %.6f, attendu %.6f", p.GetFestTeam(), p.GetRatio(), attendu[p.GetFestTeam()])
		}
	}
	if math.Abs(somme-1.0) > 1e-9 {
		t.Errorf("somme des parts = %.6f, attendu 1 — la capture totalise 1,000030", somme)
	}
	if n := len(y.GetTeamRatios()); n != 3 {
		t.Errorf("%d camp(s) publie(s), attendu 3", n)
	}
}

// TestPartageDesVotesSansAucunVote verrouille la FORME du partage servi avant le premier vote.
//
// Cas non mesure directement : la capture ne montre pas de fete sans vote. Elle montre en revanche
// la forme que Nintendo sert quand il y en a — 0,340110 / 0,330110 / 0,329810, trois valeurs
// DISTINCTES dont la somme vaut 1,000030. C'est cette forme qu'on reproduit : jamais un tiers pile,
// jamais une somme sous l'unite.
//
// Le test lui-meme a ete faux un moment. Il exigeait une somme de 1 tout rond, herite de l'epoque ou
// l'on servait vraiment un tiers pour chacun ; quand les parts sont devenues semees, l'exigence est
// restee sans que rien ne le dise, et la somme reelle avait derive a 0,994010 — six dixiemes de
// pour cent SOUS l'unite, la ou la capture est a trois millemes au-dessus.
func TestPartageDesVotesSansAucunVote(t *testing.T) {
	votesDeTest(t, "JUEA-00201", map[string]int{})

	parts := yobisaiDeLaFete("JUEA-00201").GetTeamRatios()

	var somme float64
	vues := map[float64]string{}
	for _, p := range parts {
		somme += p.GetRatio()
		if p.GetRatio() <= 0 {
			t.Errorf("%s a une part nulle : le jeu attend un partage qui totalise l'unite", p.GetFestTeam())
		}
		if autre, deja := vues[p.GetRatio()]; deja {
			t.Errorf("%s et %s ont la MEME part (%.6f) : Nintendo n'en sert jamais deux identiques",
				autre, p.GetFestTeam(), p.GetRatio())
		}
		vues[p.GetRatio()] = p.GetFestTeam()
	}

	// Un dix-millieme de trop par camp, comme dans la capture : trois camps donnent 1,000030.
	attendu := 1.0 + float64(len(parts))*1e-5
	if math.Abs(somme-attendu) > 1e-9 {
		t.Errorf("somme = %.6f, attendu %.6f (la forme de la capture Nintendo)", somme, attendu)
	}
}

func TestFestResultEnCoursALaFormeDeLaCapture(t *testing.T) {
	votesDeTest(t, "JUEA-00201", map[string]int{"Alpha": 1, "Bravo": 1, "Charlie": 1})

	r := resultatDeFeteEnCours("tenants/t-dce9377b-lp1/festResults/JUEA-00201", "JUEA-00201")

	if r.GetName() == "" {
		t.Error("le nom doit etre reconduit")
	}
	if r.GetYobisai() == nil {
		t.Fatal("yobisai absent : c'est le seul bloc que la capture remplit")
	}
	// Les phases non jouees restent ABSENTES — la capture les montre vides.
	if r.GetHonsaiMidterm() != nil {
		t.Error("honsai_midterm doit rester absent tant que la mi-parcours n'a pas eu lieu")
	}
	if r.GetHonsaiFinal() != nil {
		t.Error("honsai_final doit rester absent")
	}
	// overall existe, avec une carte de poids vide.
	if r.GetOverall() == nil {
		t.Fatal("overall absent : la capture porte overall = { weights: {} }")
	}
	if r.GetOverall().GetWeights() == nil {
		t.Error("overall.weights doit exister, meme vide")
	}
	if n := len(r.GetOverall().GetWeights().GetFields()); n != 0 {
		t.Errorf("overall.weights porte %d entree(s), la capture le montre vide", n)
	}
	if r.GetUnk() != 3 {
		t.Errorf("unk = %d, la capture porte 3 sur une fete a trois camps", r.GetUnk())
	}
}
