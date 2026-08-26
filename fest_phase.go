package main

// La phase d'un festival, nommee a UN SEUL endroit.
//
// POURQUOI CE FICHIER. Trois morceaux du serveur se posaient chacun leur version de la question
// « ou en est la fete ? » : le logo d'ecran-titre regardait le drapeau « fest », les cles
// regardaient l'heure de cloture, le verdict regardait le figeage de l'intermede. Trois lectures
// pour un seul calendrier, donc trois occasions de diverger — et elles ont diverge le 2026-08-24,
// quand le logo s'allumait des l'annonce alors que la fete ne commencait que douze heures plus tard.
//
// Les cinq bornes viennent toutes du meme calendrier que la console recoit, celui que construit
// festMaison. Rien n'est recalcule ici : on ne fait que le LIRE et le nommer.

import "time"

// phaseFest porte les cinq bornes du calendrier et le nom de l'instant present.
type phaseFest struct {
	Cle     string // identifiant stable, pour le code et le tableau de bord
	Libelle string // ce qu'on montre a un humain

	Annonce   time.Time // OpenTime  — la fete est annoncee, on peut choisir son camp
	Debut     time.Time // StartTime — les matchs de fete commencent
	Intermede time.Time // MidTime   — l'intermede fige le classement provisoire
	Fin       time.Time // EndTime   — les matchs s'arretent
	Resultats time.Time // CloseTime — le verdict est publie
}

// phaseDeLaFete rend la phase courante, et false s'il n'y a pas de fete maison servie.
func phaseDeLaFete(maintenant time.Time) (phaseFest, bool) {
	if !festMaisonActif() {
		return phaseFest{}, false
	}

	t := festMaison(maintenant).GetTimetable()
	if t == nil {
		return phaseFest{}, false
	}

	p := phaseFest{
		Annonce:   t.GetOpenTime().AsTime(),
		Debut:     t.GetStartTime().AsTime(),
		Intermede: t.GetMidTime().AsTime(),
		Fin:       t.GetEndTime().AsTime(),
		Resultats: t.GetCloseTime().AsTime(),
	}

	switch {
	case maintenant.Before(p.Annonce):
		p.Cle, p.Libelle = "avant", "pas encore annoncee"
	case maintenant.Before(p.Debut):
		p.Cle, p.Libelle = "annonce", "annoncee — choix du camp"
	case maintenant.Before(p.Intermede):
		p.Cle, p.Libelle = "premiere", "premiere mi-temps"
	case maintenant.Before(p.Fin):
		p.Cle, p.Libelle = "seconde", "seconde mi-temps"
	case maintenant.Before(p.Resultats):
		p.Cle, p.Libelle = "attente", "terminee — verdict a venir"
	default:
		p.Cle, p.Libelle = "resultats", "resultats publies"
	}

	return p, true
}

// matchsDeFeteEnCours dit si les matchs de fete tournent VRAIMENT — ni avant, ni pendant l'annonce, ni
// apres la fin. C'est la fenetre pendant laquelle l'ecran-titre porte le logo de l'evenement.
func matchsDeFeteEnCours(maintenant time.Time) bool {
	p, ok := phaseDeLaFete(maintenant)

	return ok && (p.Cle == "premiere" || p.Cle == "seconde")
}
