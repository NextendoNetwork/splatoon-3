package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// activerRotavance pose le drapeau a chaud dont depend la rotation engendree.
func activerRotavance(t *testing.T) {
	t.Helper()

	chemin := filepath.Join(t.TempDir(), "soir.flags")
	if err := os.WriteFile(chemin, []byte("rotavance\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ancien := cheminFlags
	cheminFlags = chemin
	flagsCache.Lock()
	flagsCache.lu = time.Time{}
	flagsCache.Unlock()
	t.Cleanup(func() { cheminFlags = ancien })
}

func modeleVs(t *testing.T) *toyohrpb.SelectVsSchedulesResponse {
	t.Helper()

	r := &toyohrpb.SelectVsSchedulesResponse{}
	if err := proto.Unmarshal(rawVsSchedules, r); err != nil {
		t.Fatal(err)
	}

	return r
}

// empreinteStages resume les stages d'une phase, pour comparer deux creneaux d'un coup d'oeil.
func empreinteStages(p *toyohrpb.VsSchedule) string {
	out := ""
	for _, v := range p.GetRegularSettings().GetStages() {
		out += string(rune('a' + v))
	}
	for _, b := range p.GetBankaraSettings() {
		for _, v := range b.GetStages() {
			out += string(rune('a' + v))
		}
	}
	for _, v := range p.GetXSettings().GetStages() {
		out += string(rune('a' + v))
	}
	for _, v := range p.GetLeagueSettings().GetStages() {
		out += string(rune('a' + v))
	}

	return out
}

// TestRotationEngendreeAvance : le signalement d'origine — « ça change toutes les deux heures mais
// ce sont les memes stages ». Deux creneaux consecutifs doivent servir des stages DIFFERENTS, et
// une journee entiere ne doit pas se repeter.
func TestRotationEngendreeAvance(t *testing.T) {
	activerRotavance(t)
	m := modeleVs(t)

	depart := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

	vues := map[string]bool{}
	for i := 0; i < 12; i++ {
		g := rotationEngendree(depart.Add(time.Duration(i)*rotationPeriod), m)
		if len(g.GetSchedules()) < 2 {
			t.Fatalf("phase manquante au creneau %d", i)
		}
		// la phase COURANTE est la deuxieme : la premiere est celle qui vient de finir
		vues[empreinteStages(g.GetSchedules()[1])] = true
	}

	if len(vues) < 10 {
		t.Errorf("sur 12 creneaux, seulement %d rotations distinctes — la rotation se repete", len(vues))
	}
}

// TestRotationEngendreeDeterministe : la propriete SANS laquelle tout le reste est faux. Un meme
// creneau doit rendre les memes stages, peu importe quand on le demande — sinon deux consoles qui
// interrogent a une seconde d'ecart voient deux rotations differentes, et le futur change sous les
// pieds du joueur qui relit.
func TestRotationEngendreeDeterministe(t *testing.T) {
	activerRotavance(t)
	m := modeleVs(t)

	base := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

	// meme creneau, demande a trois instants differents A L'INTERIEUR du creneau
	a := rotationEngendree(base, m)
	b := rotationEngendree(base.Add(37*time.Minute), m)
	c := rotationEngendree(base.Add(119*time.Minute), m)

	for i := range a.GetSchedules() {
		ea, eb, ec := empreinteStages(a.GetSchedules()[i]), empreinteStages(b.GetSchedules()[i]), empreinteStages(c.GetSchedules()[i])
		if ea != eb || ea != ec {
			t.Fatalf("phase %d instable dans un meme creneau : %q / %q / %q", i, ea, eb, ec)
		}
	}

	// et le FUTUR vu depuis maintenant doit correspondre au present vu plus tard
	futur := empreinteStages(a.GetSchedules()[3]) // creneau courant + 2
	plusTard := rotationEngendree(base.Add(2*rotationPeriod), m)
	if present := empreinteStages(plusTard.GetSchedules()[1]); present != futur {
		t.Errorf("le futur annonce (%q) ne correspond pas a ce qui est servi le moment venu (%q)", futur, present)
	}
}

// TestRotationEngendreeHorizon : c'est LE test dont l'absence a coute deux pannes completes.
//
// Mesure du 2026-08-18 : le jeu ne refuse pas un calendrier perime, il refuse un HORIZON trop court.
// L'ancrage qui fonctionnait laissait 19 horodatages devant l'instant courant ; ma tentative n'en
// laissait que 7, et S3 redemandait le calendrier en boucle jusqu'a l'erreur, matchmaking a zero.
func TestRotationEngendreeHorizon(t *testing.T) {
	activerRotavance(t)
	m := modeleVs(t)

	maintenant := time.Date(2026, 8, 18, 13, 37, 0, 0, time.UTC)
	g := rotationEngendree(maintenant, m)

	aVenir := 0
	for _, p := range g.GetSchedules() {
		if p.GetEndTime().AsTime().After(maintenant) {
			aVenir++
		}
	}

	if aVenir < 10 {
		t.Errorf("seulement %d phase(s) devant maintenant (minimum 10) : le jeu redemandera le calendrier en boucle", aVenir)
	}
}

// TestRotationEngendreePhaseCourante : il doit exister une phase DEJA COMMENCEE qui couvre
// maintenant, sinon le jeu n'a pas de rotation en cours a afficher.
func TestRotationEngendreePhaseCourante(t *testing.T) {
	activerRotavance(t)
	m := modeleVs(t)

	for _, minute := range []int{0, 1, 59, 119} {
		maintenant := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC).Add(time.Duration(minute) * time.Minute)

		trouve := false
		for _, p := range rotationEngendree(maintenant, m).GetSchedules() {
			d, f := p.GetStartTime().AsTime(), p.GetEndTime().AsTime()
			if !d.After(maintenant) && f.After(maintenant) {
				trouve = true
				break
			}
		}

		if !trouve {
			t.Errorf("a %v, aucune phase ne couvre l'instant courant", maintenant)
		}
	}
}

// TestRotationEngendreeIdsValides : on ne sert QUE des identifiants releves dans la capture
// Nintendo. Un stage que le jeu ne connait pas est un risque gratuit, et invisible en test unitaire
// si on ne le verrouille pas ici.
func TestRotationEngendreeIdsValides(t *testing.T) {
	activerRotavance(t)
	m := modeleVs(t)

	stages := map[int32]bool{}
	for _, v := range stagesConnus {
		stages[v] = true
	}
	regles := map[int32]bool{}
	for _, v := range reglesClassees {
		regles[v] = true
	}

	for i := 0; i < 200; i++ {
		g := rotationEngendree(time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC).Add(time.Duration(i)*rotationPeriod), m)
		for _, p := range g.GetSchedules() {
			verif := func(nom string, ss []int32) {
				if len(ss) != 2 {
					t.Fatalf("%s : %d stage(s) au lieu de 2", nom, len(ss))
				}
				if ss[0] == ss[1] {
					t.Errorf("%s : deux fois le meme stage (%d)", nom, ss[0])
				}
				for _, v := range ss {
					if !stages[v] {
						t.Errorf("%s : stage inconnu %d", nom, v)
					}
				}
			}
			verif("regular", p.GetRegularSettings().GetStages())
			for _, b := range p.GetBankaraSettings() {
				verif("bankara", b.GetStages())
				if !regles[b.GetRule()] {
					t.Errorf("bankara : regle inconnue %d", b.GetRule())
				}
			}
			verif("x", p.GetXSettings().GetStages())
			verif("league", p.GetLeagueSettings().GetStages())
			if !regles[p.GetXSettings().GetRule()] || !regles[p.GetLeagueSettings().GetRule()] {
				t.Error("regle inconnue en X ou league")
			}
		}
	}
}

// TestRotationEngendreeNomsDistincts : chaque phase est une RESSOURCE, elle doit porter son propre
// nom. Ma premiere version reprenait le nom du gabarit pour les douze phases — le jeu n'en voyait
// donc qu'une, et rendait une erreur de communication a la place de la rotation. La capture Nintendo
// porte bien douze noms distincts.
func TestRotationEngendreeNomsDistincts(t *testing.T) {
	exigeCapture(t, rawVsSchedules, "captured/SelectVsSchedules.bin")
	activerRotavance(t)
	m := modeleVs(t)

	g := rotationEngendree(time.Date(2026, 8, 19, 18, 0, 0, 0, time.UTC), m)

	noms := map[string]bool{}
	for _, p := range g.GetSchedules() {
		if p.GetName() == "" {
			t.Fatal("phase sans nom")
		}
		noms[p.GetName()] = true
	}

	if len(noms) != len(g.GetSchedules()) {
		t.Errorf("%d phases mais seulement %d nom(s) distinct(s)", len(g.GetSchedules()), len(noms))
	}

	// et le nom d'un creneau donne ne doit pas changer d'une requete a l'autre
	autre := rotationEngendree(time.Date(2026, 8, 19, 18, 47, 0, 0, time.UTC), m)
	for i := range g.GetSchedules() {
		if a, b := g.GetSchedules()[i].GetName(), autre.GetSchedules()[i].GetName(); a != b {
			t.Errorf("phase %d : nom instable dans un meme creneau (%q vs %q)", i, a, b)
		}
	}
}

// TestRotationSansCollisionDeStage : le vrai jeu ne montre jamais le meme stage dans deux modes a
// la meme heure. Un tirage independant par mode le fait pourtant regulierement — d'ou l'exclusion
// partagee entre les cinq modes d'un creneau.
func TestRotationSansCollisionDeStage(t *testing.T) {
	activerRotavance(t)
	m := modeleVs(t)

	base := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)

	for i := 0; i < 300; i++ {
		g := rotationEngendree(base.Add(time.Duration(i)*rotationPeriod), m)
		for _, p := range g.GetSchedules() {
			vus := map[int32]int{}
			ajoute := func(ss []int32) {
				for _, v := range ss {
					vus[v]++
				}
			}
			ajoute(p.GetRegularSettings().GetStages())
			for _, b := range p.GetBankaraSettings() {
				ajoute(b.GetStages())
			}
			ajoute(p.GetXSettings().GetStages())
			ajoute(p.GetLeagueSettings().GetStages())

			for v, n := range vus {
				if n > 1 {
					t.Fatalf("creneau %v : stage %d present dans %d modes simultanement",
						p.GetStartTime().AsTime(), v, n)
				}
			}
		}
	}
}

// TestRotationRepetitionConsecutiveRare mesure ce que l'exclusion garantit REELLEMENT.
//
// ⚠️ HONNETETE SUR LA LIMITE. Interdire STRICTEMENT qu'un stage revienne au creneau suivant
// exigerait que le tirage du creneau c connaisse le tirage EFFECTIF de c-1, lequel depend de c-2,
// et ainsi de suite : une recursion sans fond, incompatible avec une fonction pure du seul numero
// de creneau — la propriete meme qui rend la rotation publiable a l'avance sur le site.
//
// On exclut donc le tirage NON CONTRAINT du creneau precedent, ce qui elimine l'essentiel des
// repetitions sans rien recursiver. Ce test verrouille le resultat mesure plutot qu'un ideal :
// si une refonte future faisait remonter le taux, il le dirait.
func TestRotationRepetitionConsecutiveRare(t *testing.T) {
	activerRotavance(t)

	m := modeleVs(t)
	base := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	total, repetes := 0, 0

	stagesDe := func(p *toyohrpb.VsSchedule) []int32 {
		var out []int32
		out = append(out, p.GetRegularSettings().GetStages()...)
		for _, b := range p.GetBankaraSettings() {
			out = append(out, b.GetStages()...)
		}
		out = append(out, p.GetXSettings().GetStages()...)
		out = append(out, p.GetLeagueSettings().GetStages()...)
		return out
	}

	// Les phases adjacentes vivent dans la MEME reponse : on compare donc ce qui est reellement
	// servi, pas un tirage intermediaire.
	for i := 0; i < 60; i++ {
		g := rotationEngendree(base.Add(time.Duration(i*11)*rotationPeriod), m)
		ph := g.GetSchedules()
		for j := 1; j < len(ph); j++ {
			precedent := map[int32]bool{}
			for _, v := range stagesDe(ph[j-1]) {
				precedent[v] = true
			}
			for _, v := range stagesDe(ph[j]) {
				total++
				if precedent[v] {
					repetes++
				}
			}
		}
	}

	taux := float64(repetes) / float64(total) * 100
	t.Logf("repetitions d'un creneau au suivant : %d/%d (%.1f %%)", repetes, total, taux)

	if taux > 12 {
		t.Errorf("taux de repetition %.1f %% — au-dela de 12 %% la rotation se remet a begayer", taux)
	}
}

// TestRotationReglesDistinctes : les quatre modes classes d'un meme creneau portent quatre regles
// DIFFERENTES, et la distribution change d'un creneau a l'autre au lieu de tourner mecaniquement.
func TestRotationReglesDistinctes(t *testing.T) {
	activerRotavance(t)
	m := modeleVs(t)

	base := time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	motifs := map[string]bool{}

	for i := 0; i < 300; i++ {
		g := rotationEngendree(base.Add(time.Duration(i)*rotationPeriod), m)
		for _, p := range g.GetSchedules() {
			r := []int32{
				p.GetBankaraSettings()[0].GetRule(),
				p.GetBankaraSettings()[1].GetRule(),
				p.GetXSettings().GetRule(),
				p.GetLeagueSettings().GetRule(),
			}
			vus := map[int32]bool{}
			for _, v := range r {
				if vus[v] {
					t.Fatalf("creneau %v : regle %d servie deux fois", p.GetStartTime().AsTime(), v)
				}
				vus[v] = true
			}
			motifs[string(rune('a'+r[0]))+string(rune('a'+r[1]))+string(rune('a'+r[2]))+string(rune('a'+r[3]))] = true
		}
	}

	if len(motifs) < 8 {
		t.Errorf("seulement %d distribution(s) de regles observee(s) : la rotation des regles est trop mecanique", len(motifs))
	}
}
