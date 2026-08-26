package main

// Les horaires d'un festival doivent tomber sur des creneaux de DEUX HEURES, et sur des creneaux
// DISTINCTS.
//
// L'alignement lui-meme est deja fait par alignerSurCreneau, et son commentaire raconte pourquoi :
// une fete a cheval sur un creneau n'entre dans aucune case, le jeu cesse de la traiter comme
// ouverte et reclame son verdict. Onze consoles l'avaient recue le 2026-08-20, aucune n'avait pu
// jouer.
//
// CE QUI MANQUAIT. L'alignement tronque vers le bas, en silence. Deux decalages differents peuvent
// donc atterrir sur le MEME creneau : le 2026-08-24 j'ai pose festmi=39600 (11:00) et festfin=43000
// (11:56), et les deux sont tombes sur 10:00. Le calendrier annoncait un intermede et une fin a la
// meme seconde, et le jeu est passe hors ligne — sans que rien, ni dans les drapeaux ni dans le
// journal, ne laisse voir la collision.
//
// On la nomme donc a haute voix. Ce n'est pas une correction automatique : deviner l'intention
// derriere un decalage mal choisi serait pire que de le signaler.

import (
	"log"
	"time"
)

// verifierCreneauxDeFete journalise tout decalage qui n'est pas un multiple du creneau, et toute
// collision entre deux phases apres alignement.
func verifierCreneauxDeFete(debut time.Time, mi, fin, cloture int64) {
	pas := int64(dureeCreneauFest / time.Second)

	for _, p := range []struct {
		nom      string
		decalage int64
	}{{"festmi", mi}, {"festfin", fin}, {"festcloture", cloture}} {
		if p.decalage%pas != 0 {
			log.Printf("[NPLN fest] ⚠️ %s=%d n'est pas un multiple de %d : il sera tronque a %d, "+
				"soit %s", p.nom, p.decalage, pas, p.decalage/pas*pas,
				debut.Add(time.Duration(p.decalage/pas*pas)*time.Second).Format("02/01 15:04"))
		}
	}

	phases := []struct {
		nom     string
		instant time.Time
	}{
		{"debut", debut},
		{"mi", debut.Add(time.Duration(mi) * time.Second).Truncate(dureeCreneauFest)},
		{"fin", debut.Add(time.Duration(fin) * time.Second).Truncate(dureeCreneauFest)},
		{"cloture", debut.Add(time.Duration(cloture) * time.Second).Truncate(dureeCreneauFest)},
	}
	for i := 1; i < len(phases); i++ {
		if !phases[i].instant.After(phases[i-1].instant) {
			log.Printf("[NPLN fest] ⚠️ CALENDRIER INCOHERENT : %s et %s tombent tous deux sur %s. "+
				"Le jeu refusera la fete et passera hors ligne. Les decalages doivent etre des "+
				"multiples de %d secondes ET tomber sur des creneaux differents.",
				phases[i-1].nom, phases[i].nom,
				phases[i].instant.Format("02/01 15:04"), pas)
		}
	}
}
