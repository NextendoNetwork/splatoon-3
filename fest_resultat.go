package main

// fest_resultat — GetFestResult, le verdict d'un festival termine.
//
// MESURE DU 2026-08-20. Avec notre fete servie EN COURS, le jeu appelle GetFestResult juste apres
// GetFestDecryptionKey. Le serveur repondait « Unimplemented », une erreur dure, et la console
// s'arretait la : plus un seul appel ensuite. Toutes les consoles qui recevaient la fete se
// taisaient de la meme facon, tandis que la seule qui ne l'avait pas recue continuait de jouer.
//
// La capture d'une vraie session Nintendo pendant un vrai festival ne montre PAS cet appel : la
// console y enchaine GetFestDecryptionKey -> Ugcstore.GetDocument -> Canola.SelectDocuments ->
// GetFestEntry -> CreateFestEntry. Elle demande donc le resultat seulement dans certains etats —
// vraisemblablement quand sa sauvegarde garde trace d'un festival dont elle n'a pas vu la fin, ce
// qui est exactement le cas ici puisque notre fete reprend l'identifiant JUEA-00201 de la capture.
//
// Un festival EN COURS n'a pas de resultat, et c'est une reponse parfaitement normale : NotFound,
// comme GetFestEntry le fait pour un joueur qui n'a pas encore choisi son camp. Ce qui bloquait le
// jeu n'etait pas l'absence de resultat, c'etait qu'on lui dise que la methode n'existe pas.

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// GetFestResult rend le verdict d'un festival, ou NotFound tant qu'il n'y en a pas.
func (s *festServer) GetFestResult(ctx context.Context, req *toyohrpb.GetFestResultRequest) (*toyohrpb.FestResult, error) {
	// MESURE : la console envoie ici un nom que festIDDuChemin ne sait pas decouper — le premier
	// correctif rendait InvalidArgument, et le jeu s'arretait tout autant. Aucune erreur DURE n'a
	// sa place dans cette methode : qu'on sache lire le nom ou non, la seule reponse juste est
	// « pas de resultat ». On journalise le nom brut pour apprendre sa forme.
	festID := festIDDuChemin(req.GetName())
	if festID == "" {
		log.Printf("[NPLN fest] GetFestResult : nom non decoupe %q -> objet vide", req.GetName())
		return &toyohrpb.FestResult{Name: req.GetName()}, nil
	}

	// MESURE DU 2026-08-20. NotFound ne suffit pas : en phase d'ANNONCE le jeu ne demande pas le
	// verdict et lit son paquet BCAT en entier, alors qu'en fete COMMENCEE il le demande, recoit
	// NotFound, et n'ouvre plus rien. L'absence de resultat n'est donc pas un etat qu'il sait
	// traiter ici.
	//
	// Un FestResult porte un verdict PAR PHASE — Yobisai, mi-parcours, final, global. Une fete qui
	// vient de commencer n'en a aucun, mais l'objet existe : on rend donc l'objet, avec son nom et
	// rien d'autre. C'est le meme principe que la sauvegarde d'un compte neuf, ou zero message
	// signifie « objet par defaut » et non « absent ».
	if festMaisonActif() {
		f := festMaison(time.Now())
		if fin := f.GetTimetable().GetCloseTime(); fin != nil && fin.AsTime().After(time.Now()) {
			// MESURE DU 2026-08-22, Splatfest officiel JUEA-00107 : pendant une fete OUVERTE le
			// vrai serveur ne rend pas un objet vide, il rend le PARTAGE DES VOTES —
			// Alpha 0,340110 / Bravo 0,330110 / Charlie 0,329810, somme 1,000030. L'objet vide
			// etait notre mur : le jeu recevait une reponse sans le moindre chiffre.
			log.Printf("[NPLN fest] GetFestResult %s -> partage des votes, la fete court jusqu'au %s",
				festID, fin.AsTime().Format("02/01 15:04"))
			return resultatDeFeteEnCours(req.GetName(), festID), nil
		}
	}

	// FETE CLOSE : le verdict complet, calcule avec le bareme de Nintendo (fest_bareme.go).
	//
	// Jusqu'au 2026-08-24 on rendait NotFound des que la cloture etait passee, et le jeu, prive de
	// verdict, repassait en affichage normal — aucun ecran de resultats, comme si le festival
	// n'avait pas eu lieu.
	if festMaisonActif() {
		return resultatDeFeteTerminee(req.GetName(), festID), nil
	}

	// Fete inconnue : le jeu doit entendre « pas de resultat », jamais « methode absente ».
	log.Printf("[NPLN fest] GetFestResult %s -> aucun resultat connu", festID)
	return nil, status.Error(codes.NotFound, "fest result not found")
}
