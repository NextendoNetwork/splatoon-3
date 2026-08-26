package main

// Le seuil de formation d'une partie s'ASSOUPLIT avec l'attente.
//
// CE QU'ON MESURE. Soiree du 2026-08-22, entre 18:15 et 20:05 : 67 billets de recherche crees, 20
// annulations, et ZERO partie formee. La file de « regular_match_config » a plafonne a 5 joueurs
// distincts sur les 8 exiges — repartition relevee : 1/8 deux fois, 2/8 quatorze fois, 3/8 trente
// et une fois, 4/8 dix-huit fois, 5/8 deux fois. Jamais 6, jamais 7, jamais 8.
//
// Le seuil de huit n'est pas faux : c'est la capacite d'un Turf War, et Nintendo la remplit parce
// que sa population le permet. La notre ne le permet pas toujours. Exiger huit joueurs SIMULTANES
// dans une population qui en reunit cinq au mieux, c'est garantir que personne ne joue — et c'est
// exactement ce qui s'est passe pendant pres de deux heures.
//
// CE QU'ON FAIT. On garde huit tant qu'il reste une chance de les reunir, puis on descend :
//
//	avant 45 s   seuil nominal          (8 en Turf, 4 en Salmon Run)
//	apres 45 s   trois quarts du nominal (6 en Turf, 3 en Salmon Run)
//	apres 90 s   la moitie du nominal    (4 en Turf, 2 en Salmon Run)
//
// L'attente comptee est celle du joueur qui patiente DEPUIS LE PLUS LONGTEMPS dans cette file :
// c'est lui que l'on fait attendre pour rien, et c'est donc lui qui doit declencher la detente.
//
// POURQUOI CES DEUX PALIERS. Les billets mesures ce soir vivent jusqu'a 207 secondes avant que le
// joueur abandonne. Detendre a 45 puis 90 secondes laisse deux occasions de former la partie avant
// que la patience du joueur ne s'epuise, tout en laissant la premiere minute a une partie pleine.
//
// ON NE DESCEND JAMAIS SOUS LA MOITIE. Une session d'un seul joueur fait echouer S3 en 2321-3072 :
// le jeu ne sait pas lancer un 4v4 depuis une session qui ne tient qu'une personne. La moitie du
// nominal reste une partie a effectifs egaux des deux cotes, ce qui est le minimum defendable.
//
// RETOUR ARRIERE IMMEDIAT : le drapeau a chaud « mmstrict » desactive tout l'assouplissement et
// rend le seuil nominal, en une seconde et sans redeploiement.

import (
	"log"
	"time"
)

// paliersAssouplissement : apres combien d'attente, et quelle fraction du seuil nominal.
// Ordonnes du plus tardif au plus permissif — on prend le premier palier atteint.
var paliersAssouplissement = []struct {
	apres time.Duration
	num   int32 // seuil = nominal * num / den
	den   int32
}{
	{90 * time.Second, 1, 2},
	{45 * time.Second, 3, 4},
}

// seuilAssoupli rend le seuil a exiger compte tenu de l'attente deja subie.
//
// Pure et sans etat : c'est elle que verrouillent les tests. Ne descend jamais sous la moitie du
// nominal, ni sous deux joueurs.
func seuilAssoupli(nominal int32, attente time.Duration) int32 {
	if nominal <= 2 {
		return nominal
	}
	plancher := nominal / 2
	if plancher < 2 {
		plancher = 2
	}
	for _, p := range paliersAssouplissement {
		if attente >= p.apres {
			s := nominal * p.num / p.den
			if s < plancher {
				s = plancher
			}
			return s
		}
	}
	return nominal
}

// attenteLaPlusLongueLocked rend depuis combien de temps patiente le plus ancien joueur de cette
// file. Zero si la file est vide. L'appelant tient m.mu.
func (m *matchmakerServer) attenteLaPlusLongueLocked(cfg string) time.Duration {
	var pire time.Duration
	maintenant := time.Now()
	for _, w := range m.waitersForConfigLocked(cfg) {
		if w.depuis.IsZero() {
			continue
		}
		if d := maintenant.Sub(w.depuis); d > pire {
			pire = d
		}
	}
	return pire
}

// seuilCourantLocked rend le seuil a appliquer MAINTENANT a cette file : le nominal, assoupli par
// l'attente, sauf si « mmstrict » l'interdit. L'appelant tient m.mu.
// ⚠️ REGLE DURE — ON NE DEMARRE JAMAIS UNE PARTIE A MOINS DE JOUEURS QUE CE QUE LE JEU EXIGE.
//
// Une Guerre de territoire se joue a huit, point. Le 2026-08-25 j'ai retire le drapeau « mmstrict »
// pour debloquer une file de fete desequilibree : le serveur s'est mis a former des salons de
// quatre et de six, visibles tels quels dans le tableau de bord (« 1/4 », « 6/6 »). Ce n'est pas un
// compromis acceptable — un salon sous-dimensionne ne peut pas lancer la partie, il occupe les
// joueurs et les fait patienter pour rien.
//
// L'assouplissement est donc DESACTIVE dans le code, pas seulement par drapeau : enlever un drapeau
// ne doit plus pouvoir le ramener. seuilAssoupli reste presente et testee, mais plus personne ne
// l'appelle en production.
func (m *matchmakerServer) seuilCourantLocked(cfg string, nominal int32) (int32, time.Duration) {
	return nominal, m.attenteLaPlusLongueLocked(cfg)
}

// journaliserAssouplissement dit, UNE FOIS par palier et par file, que le seuil vient de descendre.
// Un serveur qui abaisse ses exigences doit l'ecrire : sans cela, une partie a quatre ressemble a
// un bug plutot qu'a une decision.
var dernierSeuilAnnonce = map[string]int32{}

func journaliserAssouplissement(cfg string, nominal, courant int32, attente time.Duration) {
	if courant >= nominal {
		delete(dernierSeuilAnnonce, cfg)
		return
	}
	if dernierSeuilAnnonce[cfg] == courant {
		return
	}
	dernierSeuilAnnonce[cfg] = courant
	log.Printf("[NPLN MM] %s : personne depuis %s, seuil assoupli %d -> %d joueur(s)",
		cfg, attente.Round(time.Second), nominal, courant)
}
