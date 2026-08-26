package main

// REPARTITION DES CAMPS DANS UN FESTIMATCH.
//
// CE QUE FAIT NINTENDO, MESURE SUR LA CAPTURE DU SPLATFEST OFFICIEL JUEA-00107 (2026-08-22,
// console reelle contre leurs serveurs, dossier capture-splatfest-officiel-20260822). Le ticket de
// matchmaking de la console porte son camp dans ses attributs — « fest_team » et
// « fest_team_decimal » — et cela pour les TROIS files de fete :
//
//	fest_match_regular_normal_config        le festimatch ouvert
//	fest_match_regular_direct_pair_config   « continuer avec cette equipe »
//	fest_match_challenge_oneshot_config     le festimatch defi
//
// Le serveur, lui, renvoie des creneaux « team1 », « team2 », « team3 » — 372, 372 et 371
// occurrences dans le flux descendant, et les noms de camps Alpha / Bravo / Charlie y figurent 177
// fois. C'est donc le SERVEUR qui place chaque joueur dans son creneau, pas la console.
//
// CE QUE NOUS FAISIONS. Rien : la file prenait les huit premiers joueurs en attente et recopiait
// tel quel le champ « team » envoye par le client, qui vaut la chaine vide sur un match ordinaire.
// Les deux equipes d'un festimatch auraient donc melange les camps, ce qui vide le festival de son
// sens : on ne peut pas compter les victoires d'un camp si ses joueurs sont des deux cotes.
//
// CE QUE FAIT CE FICHIER. Sur une file de fete, on regroupe les joueurs en attente par camp, on
// retient les DEUX camps les mieux represes, et on forme la partie avec autant de joueurs de
// chacun. Chaque joueur recoit « team1 » ou « team2 » selon son camp. Si deux camps ne peuvent pas
// fournir un effectif equilibre, on ne force rien et la file reprend son comportement ordinaire :
// mieux vaut un match melange qu'aucun match.
//
// Drapeau « festmelange » pour desactiver entierement cette repartition.

import (
	"log"
	"sort"
	"strings"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

// campDuTicket rend le camp declare par la console dans son ticket, ou "" s'il n'y en a pas.
func campDuTicket(w *mmWaiter) string {
	if w == nil || w.ticket == nil {
		return ""
	}

	// Les attributs voyagent dans les UserDefinitions du ticket : c'est la que la console range
	// « fest_team », a cote de match_hash et victory_point (capture du 2026-08-22).
	var attrs map[string]*commonpb.Value
	for _, ud := range w.ticket.GetUserDefinitions() {
		if f := ud.GetAttributes().GetFields(); len(f) > 0 {
			attrs = f

			break
		}
	}
	if attrs == nil {
		return ""
	}

	if v := attrs["fest_team"]; v != nil {
		if s := strings.TrimSpace(v.GetStringValue()); s != "" {
			return s
		}
		if c := campDepuisNombre(v.GetIntegerValue()); c != "" {
			return c
		}
	}
	if v := attrs["fest_team_decimal"]; v != nil {
		if c := campDepuisNombre(v.GetIntegerValue()); c != "" {
			return c
		}
		if s := strings.TrimSpace(v.GetStringValue()); s != "" {
			return s
		}
	}

	return ""
}

// campDepuisNombre traduit le camp numerique en son nom. La capture montre les deux formes selon
// les champs, et le reste du serveur raisonne en Alpha / Bravo / Charlie.
func campDepuisNombre(n int64) string {
	// ⚠️ LA NUMEROTATION COMMENCE A ZERO. Je l'avais ecrite 1/2/3, et un joueur inscrit chez
	// Pasquale & Angie (Charlie, qui publie FestTeam=2) etait compte en Bravo. Mesure du 25/08 :
	// 24 joueurs en FestTeam=0, 12 en 1, 6 en 2, pour 122 votes Alpha, 48 Bravo et 26 Charlie —
	// les trois proportions se superposent, l'ordre est donc 0, 1, 2.
	//
	// Consequence du decalage : les Charlie disparaissaient du comptage et grossissaient Bravo,
	// donc la repartition ne trouvait jamais son quatrieme joueur du bon camp.
	switch n {
	case 0:
		return "Alpha"
	case 1:
		return "Bravo"
	case 2:
		return "Charlie"
	}

	return ""
}

// estFileDeFete dit si cette configuration est une file de festimatch.
func estFileDeFete(cfg string) bool {
	return strings.Contains(lastSeg(cfg), "fest")
}

// estTricolore dit si cette configuration est un match tricolore.
//
// MESURE sur capture-tricolore-20260822 (console reelle, 23/08 au petit matin, 28 fichiers). Les
// deux configurations tricolores y figurent :
//
//	fest_match_tricolor_team_config      x92   le tricolore en equipe
//	fest_match_tricolor_oneshot_config   x86   le tricolore solo
//
// Les deux portent « tricolor », donc ce motif suffit a les reconnaitre toutes les deux. La meme
// capture confirme la repartition : team1, team2 et team3 y apparaissent exactement 120 fois
// chacun, et le camp defenseur (Bravo ce jour-la) revient 29 fois contre 10 pour chacun des deux
// autres — le rapport d'un camp a quatre joueurs contre deux camps a deux.
//
// « festconfigtricolore=<motif> » reste disponible si Nintendo en ajoutait une troisieme.
func estTricolore(cfg string) bool {
	c := lastSeg(cfg)
	if m := soirFlagValeur("festconfigtricolore"); m != "" {
		return strings.Contains(c, strings.ToLower(strings.TrimSpace(m)))
	}

	return strings.Contains(c, "tricolor")
}

// repartirTricolore forme un match a trois camps : le defenseur a quatre, les deux attaquants a
// deux chacun.
//
// CE QUI EST MESURE. La ferveur d'une bataille tricolore, relevee le 2026-08-23 sur cinq batailles,
// a la forme [7950 7950 | 16500 16500 | 1223 969 818 702] : deux attaquants, deux attaquants,
// QUATRE defenseurs — et les quatre defenseurs sont tous du meme camp. Le verdict, lui, designe le
// slot vainqueur par team_result.winner valant 1, 2 ou 3.
//
// LE DEFENSEUR EST TOUJOURS LE VAINQUEUR DE L'INTERMEDE — c'est la regle du jeu, et campDefenseur
// ne rend rien d'autre : le premier du classement fige a la mi-parcours.
//
// CE QUI RESTE NON MESURE. Quel numero de slot Nintendo donne au defenseur. Les creneaux
// apparaissent en nombre strictement egal dans la capture, et les camps n'y sont pas adjacents aux
// creneaux, donc l'association ne s'y lit pas directement. C'est sans consequence sur le comptage :
// campVainqueur relit le camp de chaque slot dans personal_result et nos inscriptions, donc il suit
// la repartition quelle qu'elle soit. On place le defenseur en team1 et on l'ecrit dans le journal,
// pour pouvoir le confronter a la premiere vraie bataille.
func repartirTricolore(festID string, parCamp map[string][]*mmWaiter) ([]*mmWaiter, map[*mmWaiter]string) {
	defenseur := campDefenseur(festID)
	if defenseur == "" || len(parCamp[defenseur]) < 4 {
		return nil, nil
	}

	attaquants := make([]string, 0, 2)
	for camp := range parCamp {
		if camp != defenseur && len(parCamp[camp]) >= 2 {
			attaquants = append(attaquants, camp)
		}
	}
	if len(attaquants) < 2 {
		return nil, nil
	}
	sort.Strings(attaquants)

	choisis := make([]*mmWaiter, 0, 8)
	creneau := make(map[*mmWaiter]string, 8)
	for _, w := range parCamp[defenseur][:4] {
		choisis = append(choisis, w)
		creneau[w] = "team1"
	}
	for i, camp := range attaquants[:2] {
		slot := "team2"
		if i == 1 {
			slot = "team3"
		}
		for _, w := range parCamp[camp][:2] {
			choisis = append(choisis, w)
			creneau[w] = slot
		}
	}

	log.Printf("[NPLN MM] tricolore : %s defend a 4 (team1) contre %s (team2) et %s (team3), a 2 chacun",
		defenseur, attaquants[0], attaquants[1])

	return choisis, creneau
}

// repartirParCamp choisit, parmi les joueurs en attente, les camps qui s'affrontent et rend la
// selection equilibree ainsi que le creneau de chacun. Rend nil si la repartition n'est pas
// possible : le caller garde alors son comportement ordinaire.
func repartirParCamp(players []*mmWaiter, seuil, seuilDetendu int, cfg, festID string) ([]*mmWaiter, map[*mmWaiter]string) {
	if seuil < 2 || soirFlag("festmelange") {
		return nil, nil
	}

	parCamp := map[string][]*mmWaiter{}
	for _, w := range players {
		if c := campDuTicket(w); c != "" {
			parCamp[c] = append(parCamp[c], w)
		}
	}
	if estTricolore(cfg) {
		return repartirTricolore(festID, parCamp)
	}
	if len(parCamp) < 2 {
		return nil, nil
	}

	camps := make([]string, 0, len(parCamp))
	for c := range parCamp {
		camps = append(camps, c)
	}
	// Les deux camps les mieux representes ; a effectif egal, l'ordre alphabetique pour rester
	// reproductible d'un tour a l'autre.
	sort.Slice(camps, func(i, j int) bool {
		if len(parCamp[camps[i]]) != len(parCamp[camps[j]]) {
			return len(parCamp[camps[i]]) > len(parCamp[camps[j]])
		}

		return camps[i] < camps[j]
	})

	a, b := camps[0], camps[1]
	parEquipe := seuil / 2

	// Les equipes ne retrecissent PAS. Meme regle que ci-dessus : quatre d'un camp contre quatre
	// d'un AUTRE camp, ou rien. Voir matchmaking_seuil_attente.go.
	if len(parCamp[a]) < parEquipe || len(parCamp[b]) < parEquipe {
		// UN FESTIMATCH OPPOSE DEUX CAMPS DIFFERENTS, POINT.
		//
		// J'avais ajoute ici un match MIROIR — huit joueurs d'un meme camp s'affrontant — en
		// affirmant que Nintendo procedait ainsi quand une population est desequilibree. C'etait une
		// affirmation, pas une mesure : les captures ne le montrent nulle part, et l'utilisateur, qui
		// connait le jeu, dit l'inverse. Chez Nintendo un festimatch, c'est quatre d'un camp contre
		// quatre d'un AUTRE camp, jamais le meme camp face a lui-meme.
		//
		// Quand la file ne peut pas fournir cette composition, on ne forme rien : les joueurs
		// patientent et la tentative suivante reessaiera.
		log.Printf("[NPLN MM] festimatch : pas d'effectif par camp (%s=%d, %s=%d, il en faut %d de chaque) — on attend",
			a, len(parCamp[a]), b, len(parCamp[b]), parEquipe)

		return nil, nil
	}

	choisis := make([]*mmWaiter, 0, parEquipe*2)
	creneau := make(map[*mmWaiter]string, parEquipe*2)
	for _, w := range parCamp[a][:parEquipe] {
		choisis = append(choisis, w)
		creneau[w] = "team1"
	}
	for _, w := range parCamp[b][:parEquipe] {
		choisis = append(choisis, w)
		creneau[w] = "team2"
	}

	log.Printf("[NPLN MM] festimatch : %s (team1) contre %s (team2), %d joueur(s) de chaque",
		a, b, parEquipe)

	return choisis, creneau
}
