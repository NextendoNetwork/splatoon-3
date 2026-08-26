package main

// flags_soir — activer ou desactiver A CHAUD chacun des changements issus des captures Nintendo du
// 2026-08-12, sans redeployer ni redemarrer le conteneur.
//
// POURQUOI. Ces changements reposent sur de vraies mesures (une capture du hall et
// une capture de compte neuf), mais je les ai deployes EN BLOC, sans relancer le jeu entre
// chacun. Resultat : S3 s'abat en 2162-0001 avant meme d'atteindre penne, et rien ne dit lequel des
// six est en cause — il a fallu tout retirer pour rendre la main au joueur. Une variable
// d'environnement n'aurait pas suffi : sur ce serveur elle impose de recreer le conteneur, donc une
// coupure a chaque essai.
//
// COMMENT. Un fichier /data/soir.flags, une cle par ligne, lu a chaque appel (il est minuscule et le
// cache du systeme de fichiers absorbe la lecture). Absent ou vide = comportement d'AVANT les
// captures, celui avec lequel le jeu demarre. Exemple pour n'activer que la reponse de violation :
//
//	echo violation > /data/soir.flags && (relancer le jeu)
//
// Cles reconnues :
//
//	violation  UserScreening/GetViolation repond vide, comme Nintendo (sinon : chemin fabrique)
//	save       CloudSave/GetSaveRecord repond vide en DEUX cadres (sinon : Trailers-Only)
//	saveabsent CloudSave/GetSaveRecord sans record repond NOT_FOUND en DEUX cadres (prime « save »).
//	           Le vide, meme a deux cadres, fait construire au SDK un SaveRecord PAR DEFAUT : le jeu
//	           croit avoir un record vide, le VALIDE (Write evt=Validate) et n'appelle jamais
//	           CreateSaveRecord. Mesure du 2026-08-13, et meme mecanisme que le GetDocument du
//	           commit 6ed6b4d. L'ancien essai NotFound partait en Trailers-Only : il ne compte pas.
//	getdoc     ugcstore/GetDocument absent repond vide, comme Nintendo (sinon : NotFound)
//	initattr   InitializeAttributes : 3 variantes + base joueur neuf (sinon : ancien partage)
//	gsct       salon prive : rejeu greffe de la capture (sinon : notre reponse generee)
//	trace      journaliser chaque requete REST (diagnostic)

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// cheminFlags est le chemin ATTENDU du fichier de drapeaux. C'est une VARIABLE et non une
// constante uniquement pour que les tests puissent pointer un fichier temporaire.
var cheminFlags = "/data/soir.flags"

// cheminsFlagsDeSecours : ou chercher le fichier quand cheminFlags n'existe pas.
//
// POURQUOI CE DETOUR. « /data » est un chemin de CONTENEUR : c'est la qu'est monte le volume
// /opt/npln. Or le MEME binaire tourne aussi sur l'HOTE, en systemd, pour l'arbitre de session du
// port 7575 (NPLN_GAMESYNC_ONLY=1). La, « /data » existe bien mais ne contient pas soir.flags — le
// fichier vit dans /opt/npln.
//
// MESURE DU 2026-08-22. On cherchait pourquoi le drapeau « clestls » ouvrait le journal des secrets
// TLS sur le port 443 et pas sur le 7575 : l'arbitre lisait un fichier absent. La consequence est
// bien plus large que ce seul drapeau — depuis qu'il existe, l'arbitre n'en avait JAMAIS vu un
// seul, et tout reglage a chaud le laissait dans son comportement par defaut, sans rien signaler.
//
// Plutot qu'un lien symbolique sur le serveur, qu'une reinstallation efface sans bruit, le binaire
// cherche lui-meme aux deux endroits.
var cheminsFlagsDeSecours = []string{"/opt/npln/soir.flags", "soir.flags"}

// sourceDesDrapeaux retient d'ou viennent les drapeaux, pour le journaliser quand ca change. Un
// serveur qui n'obeit pas a ses drapeaux doit le DIRE, au lieu de se taire comme il l'a fait ici.
var sourceDesDrapeaux string

// fichierDeDrapeaux rend le contenu du fichier de drapeaux, et le chemin d'ou il sort.
//
// L'ordre : NPLN_FLAGS_FILE si la variable est posee — elle tranche —, puis cheminFlags, puis les
// secours. Le premier fichier LISIBLE gagne. Un chemin absent n'est pas une erreur : c'est
// simplement « pas de drapeaux ici », et on passe au suivant.
func fichierDeDrapeaux() ([]byte, string) {
	candidats := make([]string, 0, 4)
	if v := os.Getenv("NPLN_FLAGS_FILE"); v != "" {
		candidats = append(candidats, v)
	}
	candidats = append(candidats, cheminFlags)
	candidats = append(candidats, cheminsFlagsDeSecours...)

	for _, c := range candidats {
		if b, err := os.ReadFile(c); err == nil {
			return b, c
		}
	}
	return nil, ""
}

var flagsCache struct {
	sync.Mutex
	lu    time.Time
	actif map[string]bool

	// La meme chose, mais sans perdre la casse des valeurs : voir flags_valeurs.go.
	valeurs map[string]string
}

// soirFlag indique si un changement du 2026-08-12 est actif. Defaut : non.
func soirFlag(nom string) bool {
	flagsCache.Lock()
	defer flagsCache.Unlock()

	// Relire au plus une fois par seconde : le fichier se modifie a la main entre deux essais,
	// jamais en rafale, et on evite un open() par RPC.
	if time.Since(flagsCache.lu) > time.Second {
		actif := map[string]bool{}
		b, source := fichierDeDrapeaux()
		if source != sourceDesDrapeaux {
			if source == "" {
				log.Printf("[NPLN flags] aucun fichier lisible (essayes : %s) — tout reste par defaut",
					strings.Join(append([]string{cheminFlags}, cheminsFlagsDeSecours...), ", "))
			} else {
				log.Printf("[NPLN flags] drapeaux lus dans %s", source)
			}
			sourceDesDrapeaux = source
		}
		if b != nil {
			for _, ligne := range strings.Split(string(b), "\n") {
				if c := strings.TrimSpace(strings.SplitN(ligne, "#", 2)[0]); c != "" {
					actif[strings.ToLower(c)] = true
				}
			}
		}
		flagsCache.actif = actif
		flagsCache.valeurs = valeursDesDrapeaux(b)
		flagsCache.lu = time.Now()
	}
	return flagsCache.actif[nom]
}

// soirFlagValeur rend la valeur d'un drapeau ecrit « nom=valeur » dans le meme fichier.
// « nom » seul (sans « = ») rend "1" : le drapeau est present, sans valeur explicite.
// Chaine vide = drapeau absent.
func soirFlagValeur(nom string) string {
	soirFlag("") // force la relecture du cache selon la meme regle d'une seconde

	flagsCache.Lock()
	defer flagsCache.Unlock()

	return flagsCache.valeurs[nom]
}
