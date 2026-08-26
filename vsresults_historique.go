package main

// L'HISTORIQUE EN JEU : c'est le SERVEUR qui fabrique le document de bataille.
//
// CE QU'ON CROYAIT. Que l'historique des batailles vivait dans la sauvegarde. Faux : la cle
// GameRecordHistory du SaveRecord fait 1853 octets IDENTIQUES dans les 150 sauvegardes du serveur,
// y compris les plus grosses — c'est le bloc fige de la sauvegarde d'amorcage, que personne ne
// reecrit jamais. Et sur vingt-quatre heures, aucune ecriture de sauvegarde ne porte cette cle.
//
// CE QUE FAIT NINTENDO. Capture d'un Splatfest officiel, 2026-08-23, relais-session. Apres chaque
// bataille, le serveur cree un document
//
//	tenants/<locataire>/documents/services/GameRecord/users/<uid>/VsResults/<battle_id>
//
// avec battle_id = « 20260823T000736_35ddcc57-431a-481d-b04a-8de6e580219b », soit un horodatage
// compact puis l'identifiant de partie. La console ne l'ECRIT PAS : sur toute la capture elle ne
// fait pas un seul WriteDocuments. Elle LIT, via RunQuery sur « GameRecord/users/<uid> », trie sur
// started_at, et affiche. C'est tout l'historique en jeu.
//
// CHEZ NOUS, la console posait exactement cette question — 103 RunQuery sur ce parent en une
// journee — et recevait 103 fois « aucun document ». L'historique ne pouvait qu'etre vide.
//
// CE QU'ON REMPLIT, ET CE QU'ON LAISSE VIDE. Le rapport que la console nous envoie a la fin porte
// `team_result` et `personal_result` : tous les joueurs, leur equipe, leur encre, leurs eliminations.
// On s'en sert. En revanche on n'INVENTE pas ce qu'on n'a pas — le classement Glicko-2
// (fest_power_before / fest_power_after), les ratios d'encre par equipe calcules par Nintendo, la
// duree exacte : ces champs restent absents tant qu'une mesure ne nous dit pas comment les produire.
// Un historique honnete et incomplet vaut mieux qu'un historique inventé.
//
// Le suffixe du rapport d'arbitrage vaut deja « <horodatage>_<partie> » : c'est EXACTEMENT le format
// de battle_id de Nintendo, on le reprend tel quel plutot que d'en forger un autre.

import (
	"log"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

// nomDuDocumentVsResults construit le nom complet du document de bataille d'un joueur.
func nomDuDocumentVsResults(uid, battleID string) string {
	// npnTenant vaut deja « tenants/t-dce9377b-lp1 » (matchmaking.go).
	return npnTenant + "/documents/services/GameRecord/users/" + uid + "/VsResults/" + battleID
}

// equipeDuJoueur lit le numero d'equipe d'une fiche de resultat personnel.
func equipeDuJoueur(v *commonpb.Value) int64 {
	if m := v.GetMapValue(); m != nil {
		if t, ok := m.GetFields()["team"]; ok {
			return t.GetIntegerValue()
		}
	}
	return 0
}

// fichesParEquipe repartit les fiches personnelles en trois tableaux, comme Nintendo :
// personal_data_team1 / team2 / team3. Une partie ordinaire n'en remplit que deux ; un tricolore
// les trois — le champ « team » prend alors les valeurs 1, 2 et 3.
//
// QUI DEFEND. Mesure du 2026-08-23 sur les CINQ batailles tricolores de la capture : personal_data_-
// team3 porte a chaque fois QUATRE joueurs, et tous du meme camp — Bravo, dans les cinq. Les deux
// autres tableaux portent deux joueurs chacun, Alpha et Charlie.
//
// Le camp defenseur n'est donc pas une propriete du slot ni du hasard de l'appariement : c'est un
// camp FIXE pour toute la duree du festival. C'est celui qui a gagne l'intermede — le tricolore
// oppose le camp en tete a mi-parcours aux deux autres reunis, quatre contre deux plus deux. Le
// champ « defender » du billet de matchmaking dit simplement si le joueur appartient a ce camp-la.
//
// Consequence pour nous : le jour ou nous ferons tourner un tricolore maison, l'equipe a quatre ne
// se tire pas au sort, elle se DEDUIT du resultat de l'intermede (honsai_midterm de GetFestResult).
func fichesParEquipe(personal *commonpb.MapValue) map[string][]*commonpb.Value {
	par := map[string][]*commonpb.Value{
		"personal_data_team1": {},
		"personal_data_team2": {},
		"personal_data_team3": {},
	}
	if personal == nil {
		return par
	}
	for _, fiche := range personal.GetFields() {
		switch equipeDuJoueur(fiche) {
		case 1:
			par["personal_data_team1"] = append(par["personal_data_team1"], fiche)
		case 2:
			par["personal_data_team2"] = append(par["personal_data_team2"], fiche)
		case 3:
			par["personal_data_team3"] = append(par["personal_data_team3"], fiche)
		}
	}
	return par
}

// documentDeBataille fabrique le VsResults d'UN joueur.
func documentDeBataille(uid, battleID, gsid string, rapport *commonpb.MapValue) *ugcpb.Document {
	champs := map[string]*commonpb.Value{
		"battle_id":    gsStr(battleID),
		"npln_user_id": gsStr(uid),
	}

	if rapport != nil {
		if tr, ok := rapport.GetFields()["team_result"]; ok {
			champs["team_result"] = tr
		}
		personal := rapport.GetFields()["personal_result"].GetMapValue()
		for nom, fiches := range fichesParEquipe(personal) {
			champs[nom] = gsTableau(fiches...)
		}
	}

	maintenant := timestamppb.Now()
	return &ugcpb.Document{
		Name:       nomDuDocumentVsResults(uid, battleID),
		Fields:     &commonpb.MapValue{Fields: champs},
		CreateTime: maintenant,
		UpdateTime: maintenant,
	}
}

// publierHistoriqueDeBataille cree le document de bataille de CHAQUE participant.
//
// Un document par joueur, sous son propre chemin : c'est ainsi que Nintendo range les batailles, et
// c'est ce que la console interroge. Publier un seul document partage ne serait vu par personne.
func publierHistoriqueDeBataille(battleID, gsid string, uids []string, rapport *commonpb.MapValue) int {
	if battleID == "" || len(uids) == 0 {
		return 0
	}
	n := 0
	for _, uid := range uids {
		if strings.TrimSpace(uid) == "" {
			continue
		}
		storePutDocument(documentDeBataille(uid, battleID, gsid, rapport))
		n++
	}
	if n > 0 {
		log.Printf("[NPLN historique] bataille %s publiee pour %d joueur(s) | partie=%s", battleID, n, gsid)
	}
	return n
}
