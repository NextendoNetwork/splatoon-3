package main

// coop_carte_points — tenir la CARTE DE POINTS de Salmon Run, celle que le jeu appelle « Bonus
// Meter ».
//
// LE PROBLEME. Apres un quart, le compteur du joueur restait a zero, indefiniment. Mesure du
// 2026-08-21 : le jeu demande ses deux cartes et nous repondons « absent », 56 fois de suite.
//
//	GameRecord/users/<uid>/PointCardTotal/Data           -> NotFound (76x)
//	GameRecord/users/<uid>/PointCardRegular/<creneau>    -> NotFound (56x)
//
// Et il ne les ecrit JAMAIS lui-meme : sur toute une soiree, ses seules ecritures sont sa presence,
// le NAT et ses rapports d'arbitrage. La capture Nintendo dit pourquoi — c'est le SERVEUR qui tient
// ces cartes :
//
//	GameRecord/PointCardRegular/<date>   Succeeded  0 puis 509 octets
//
// Vide avant le premier quart, 509 octets ensuite. Personne ne l'ecrit a notre place.
//
// CE QUI EST MESURE, ET CE QUI NE L'EST PAS.
//
// Les deux documents sont decodes de la capture, champ par champ, valeurs comprises :
//
//	PointCardTotal/Data  (8 champs)   job_num=14 golden_ikura_total=539 ikura_total=20418
//	                                  rescue_total=30 boss_total=0 kuma_point=1506
//	                                  limited_kuma_point=0 version=131072
//	PointCardRegular/…   (15 champs)  + grade, total_grade_point, best_grade_point,
//	                                  best_golden_ikura_num, total_clear_wave, job_type_string,
//	                                  shift_id, shift_time
//
// La cle du creneau est le `shift_id` du verdict lui-meme — releve : « 20260806160000 », et
// `shift_time` porte « 20260806160000-20260808080000 ». On ne devine donc aucune date : on recopie
// ce que la console nous a envoye.
//
// Les compteurs par joueur (golden_ikura_num, ikura_num, rescue_num) MONTENT du client : ils sont
// presents dans les deux sens de la capture. On les additionne tels quels.
//
// ⚠️ `kuma_point`, LUI, NE MONTE PAS. Mesure : 75 occurrences dans le flux descendant, ZERO dans le
// montant. C'est Nintendo qui le calcule, et nous n'avons que deux quarts pour en deduire la regle :
//
//	62 oeufs d'or, 3 vagues reussies -> 177 points
//	59 oeufs d'or, moins de vagues   -> 110 points
//
// Deux mesures ne font pas une formule. Rapporte a la vague, l'ecart se resserre (59 et 55 points
// par vague la ou l'oeuf donne 2,85 et 1,86), donc on compte a la vague — et on JOURNALISE la valeur
// pour qu'une capture plus fournie la corrige. C'est une approximation assumee, pas une mesure.

import (
	"log"
	"strings"
	"sync"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

// LA REGLE DES POINTS GRIZZCO, DEDUITE DE DIX-SEPT QUARTS.
//
//	kuma_point = 2 x oeufs d'or LIVRES par l'equipe, + 50 si le quart est reussi
//
// Obtenue par decodage STRUCTUREL de la capture (tools/decoder-capture-npln.py : trames HTTP/2,
// messages gRPC, puis l'arbre Value de NPLN). Les deux tentatives precedentes etaient fausses
// faute de ce decodage :
//
//	60 points par vague       deduit de 2 quarts   -> 120 points la ou la capture en montre 36
//	medianes par vague        deduites de 17       -> ignore les oeufs, +/- 40 points d'ecart
//
// CE QUI EST MESURE. Dix-sept quarts distincts, chacun avec ses quatre joueurs :
//
//	quarts ECHOUES (4 obs.)  ajustement 2,13 x oeufs livres  -> ecart moyen 1,2 point
//	quarts REUSSIS (12 obs.) ajustement 2,02 x oeufs + 44,3  -> ecart moyen 7,8 points
//	la regle retenue, 2 par oeuf et 50 de prime, tombe a 6,0 points d'ecart moyen sur les 17
//
// Sur les quarts echoues elle est quasi exacte (ecart max 2 points sur 4 observations). Sur les
// quarts reussis un seul cas s'ecarte franchement (+36) ; les onze autres tiennent dans +/-10.
//
// ⚠️ CE QUI COMPTE, C'EST L'OEUF LIVRE, PAS L'OEUF RAMASSE. Le rapport entre les deux varie du
// simple au double d'un quart a l'autre (132 ramasses pour 62 livres sur l'un, 52 pour 19 sur un
// autre) : se tromper de champ, c'est se tromper de moitie. delivered_golden_ikura_num arrive bien
// dans le flux MONTANT de la session, une fois par vague — notre serveur le voit.
//
// kuma_point est une recompense D'EQUIPE : les quatre joueurs d'un meme quart recoivent la meme
// valeur, alors que leurs oeufs personnels different. On ne la calcule donc qu'une fois.
const pointsParOeufLivre = 2

// primeQuartReussi : la prime de fin, ajustee a 44,3 sur douze quarts reussis et arrondie a 50,
// valeur qui minimise l'ecart moyen.
const primeQuartReussi = 50

// vagueDuQuartReussi : final_wave vaut 2 quand les trois vagues sont passees — releve sur les douze
// quarts reussis de la capture, tous a 2, contre 1 ou 0 pour les cinq autres.
const vagueDuQuartReussi = 2

// pointsDuQuart applique la regle ci-dessus.
func pointsDuQuart(oeufsLivres, vagueFinale int64) int64 {
	p := pointsParOeufLivre * oeufsLivres
	if vagueFinale >= vagueDuQuartReussi {
		p += primeQuartReussi
	}
	return p
}

// versionCarteDePoints : releve tel quel sur les deux documents de la capture.
const versionCarteDePoints = 131072

// dejaCredite : un quart ne se credite qu'UNE FOIS PAR JOUEUR.
//
// Mesure du 2026-08-21 : l'arbitrage rejoue a chaque rapport recu, et le meme match a produit
// TROIS verdicts status=1 (24252, 24794 puis 25333 octets) au fur et a mesure que les rapports
// arrivaient. Sans ce garde-fou, un quart a quatre joueurs crediterait trois ou quatre fois.
var dejaCredite = struct {
	sync.Mutex
	m map[string]bool
}{m: map[string]bool{}}

func quartDejaCredite(cleMatch, uid string) bool {
	dejaCredite.Lock()
	defer dejaCredite.Unlock()
	k := cleMatch + "|" + uid
	if dejaCredite.m[k] {
		return true
	}
	dejaCredite.m[k] = true
	return false
}

var carteMu sync.Mutex

// gainCoop : ce qu'un joueur a gagne sur UN quart.
type gainCoop struct {
	oeufsOr    int64 // golden_ikura_num
	oeufs      int64 // ikura_num
	sauvetages int64 // rescue_num
	boss       int64 // 1 si le roi a ete battu
	vagues     int64 // final_wave : 2 = les trois vagues passees
	orLivre    int64 // delivered_golden_ikura_num, somme de l'EQUIPE — c'est lui qui paye
	gradePt    int64 // total_grade_point du quart
	grade      int64
	shiftID    string
	shiftTime  string
	typeQuart  string
}

func nomCarteTotale(uid string) string {
	return "tenants/current/documents/services/GameRecord/users/" + uid + "/PointCardTotal/Data"
}

func nomCarteCreneau(uid, shiftID string) string {
	return "tenants/current/documents/services/GameRecord/users/" + uid + "/PointCardRegular/" + shiftID
}

// entierDuDoc lit un compteur du document, ou zero s'il n'y est pas encore.
func entierDuDoc(doc *ugcpb.Document, cle string) int64 {
	if doc == nil {
		return 0
	}
	return doc.GetFields().GetFields()[cle].GetIntegerValue()
}

// docAvec fabrique (ou reprend) un document et y pose les champs donnes.
func docAvec(nom string, ancien *ugcpb.Document, champs map[string]*commonpb.Value) *ugcpb.Document {
	fusion := map[string]*commonpb.Value{}
	for k, v := range ancien.GetFields().GetFields() {
		fusion[k] = v
	}
	for k, v := range champs {
		fusion[k] = v
	}
	return &ugcpb.Document{Name: nom, Fields: &commonpb.MapValue{Fields: fusion}}
}

// crediterCartesDePoints ajoute un quart aux deux cartes du joueur, et les cree si besoin.
func crediterCartesDePoints(uid string, g gainCoop) {
	if uid == "" {
		return
	}

	points := pointsDuQuart(g.orLivre, g.vagues)

	carteMu.Lock()
	defer carteMu.Unlock()

	// ---- carte cumulee ----
	nomT := nomCarteTotale(uid)
	ancT, _ := storeGetDocument(nomT)
	totKuma := entierDuDoc(ancT, "kuma_point") + points
	totJobs := entierDuDoc(ancT, "job_num") + 1
	storePutDocument(docAvec(nomT, ancT, map[string]*commonpb.Value{
		"job_num":            gsInt(totJobs),
		"golden_ikura_total": gsInt(entierDuDoc(ancT, "golden_ikura_total") + g.oeufsOr),
		"ikura_total":        gsInt(entierDuDoc(ancT, "ikura_total") + g.oeufs),
		"rescue_total":       gsInt(entierDuDoc(ancT, "rescue_total") + g.sauvetages),
		"boss_total":         gsInt(entierDuDoc(ancT, "boss_total") + g.boss),
		"kuma_point":         gsInt(totKuma),
		"limited_kuma_point": gsInt(entierDuDoc(ancT, "limited_kuma_point")),
		"version":            gsInt(versionCarteDePoints),
	}))

	// ---- carte du creneau ----
	//
	// Sans shift_id on ne sait pas SOUS QUEL NOM la ranger, et le jeu ne la retrouverait pas : on
	// s'abstient plutot que de la classer au hasard.
	if g.shiftID == "" {
		log.Printf("[NPLN carte] %s : quart credite (+%d pts) mais shift_id absent — carte de creneau non tenue", short(uid), points)
		return
	}

	nomC := nomCarteCreneau(uid, g.shiftID)
	ancC, _ := storeGetDocument(nomC)
	meilleurOr := entierDuDoc(ancC, "best_golden_ikura_num")
	if g.oeufsOr > meilleurOr {
		meilleurOr = g.oeufsOr
	}
	meilleurGrade := entierDuDoc(ancC, "best_grade_point")
	if g.gradePt > meilleurGrade {
		meilleurGrade = g.gradePt
	}
	champs := map[string]*commonpb.Value{
		"job_num":               gsInt(entierDuDoc(ancC, "job_num") + 1),
		"golden_ikura_total":    gsInt(entierDuDoc(ancC, "golden_ikura_total") + g.oeufsOr),
		"ikura_total":           gsInt(entierDuDoc(ancC, "ikura_total") + g.oeufs),
		"rescue_total":          gsInt(entierDuDoc(ancC, "rescue_total") + g.sauvetages),
		"boss_total":            gsInt(entierDuDoc(ancC, "boss_total") + g.boss),
		"kuma_point":            gsInt(entierDuDoc(ancC, "kuma_point") + points),
		"total_clear_wave":      gsInt(entierDuDoc(ancC, "total_clear_wave") + g.vagues),
		"total_grade_point":     gsInt(g.gradePt),
		"best_grade_point":      gsInt(meilleurGrade),
		"best_golden_ikura_num": gsInt(meilleurOr),
		"grade":                 gsInt(g.grade),
		"shift_id":              gsStr(g.shiftID),
		"version":               gsInt(versionCarteDePoints),
	}
	if g.shiftTime != "" {
		champs["shift_time"] = gsStr(g.shiftTime)
	}
	if g.typeQuart != "" {
		champs["job_type_string"] = gsStr(g.typeQuart)
	}
	storePutDocument(docAvec(nomC, ancC, champs))

	log.Printf("[NPLN carte] %s : +%d pts (%d or livre x%d%s) | quart %s | or=%d oeufs=%d sauv=%d "+
		"-> total %d pts sur %d quart(s)",
		short(uid), points, g.orLivre, pointsParOeufLivre,
		map[bool]string{true: " + prime de reussite", false: ""}[g.vagues >= vagueDuQuartReussi], g.shiftID,
		g.oeufsOr, g.oeufs, g.sauvetages, totKuma, totJobs)
}

// ---- lecture du verdict ----

// sommeParNom parcourt une valeur en profondeur et additionne tous les champs portant ce nom.
//
// Les compteurs d'un joueur ne sont pas a plat : ils vivent dans des sous-messages (par vague, par
// arme). Plutot que de fixer une profondeur — que la capture ne nous donne pas de maniere sure —
// on additionne par NOM, ce qui reste juste quel que soit l'emboitement.
func sommeParNom(v *commonpb.Value, nom string) int64 {
	if v == nil {
		return 0
	}
	var total int64
	if m := v.GetMapValue(); m != nil {
		for k, sv := range m.GetFields() {
			if k == nom {
				total += sv.GetIntegerValue() + int64(sv.GetDoubleValue())
				continue
			}
			total += sommeParNom(sv, nom)
		}
	}
	for _, e := range v.GetArrayValue().GetValues() {
		total += sommeParNom(e, nom)
	}
	return total
}

// premierParNom rend la premiere valeur entiere trouvee sous ce nom, en profondeur.
func premierParNom(v *commonpb.Value, nom string) int64 {
	if v == nil {
		return 0
	}
	if m := v.GetMapValue(); m != nil {
		if sv, ok := m.GetFields()[nom]; ok {
			return sv.GetIntegerValue() + int64(sv.GetDoubleValue())
		}
		for _, sv := range m.GetFields() {
			if n := premierParNom(sv, nom); n != 0 {
				return n
			}
		}
	}
	for _, e := range v.GetArrayValue().GetValues() {
		if n := premierParNom(e, nom); n != 0 {
			return n
		}
	}
	return 0
}

// crediterQuartCoop lit un verdict coop arbitre et credite chaque joueur.
//
// `champs` est le verdict complet ; `uidParSession` associe l'identifiant de session d'un joueur a
// son uid, car `personal_result` est indexe par session.
func crediterQuartCoop(cleMatch string, champs map[string]*commonpb.Value, uidParSession func(string) string) {
	perso := champs["personal_result"].GetMapValue()
	if perso == nil {
		return
	}

	equipe := champs["team_result"]
	// final_wave D'ABORD : c'est sur lui que la regle des points a ete ajustee (2 = les trois
	// vagues passees). total_clear_wave dit la meme chose dans l'autre convention, 3 pour un quart
	// reussi ; le seuil « au moins 2 » accepte les deux sans les confondre.
	vagues := premierParNom(equipe, "final_wave")
	if vagues == 0 {
		vagues = premierParNom(equipe, "total_clear_wave")
	}
	commun := gainCoop{
		vagues:    vagues,
		orLivre:   sommeParNom(equipe, "delivered_golden_ikura_num"),
		gradePt:   premierParNom(equipe, "total_grade_point"),
		grade:     premierParNom(equipe, "grade"),
		boss:      premierParNom(equipe, "did_defeat_boss"),
		shiftID:   champs["shift_id"].GetStringValue(),
		shiftTime: champs["shift_time"].GetStringValue(),
		typeQuart: champs["job_type_string"].GetStringValue(),
	}
	if commun.gradePt == 0 {
		commun.gradePt = champs["challenge_grade_point"].GetIntegerValue()
	}
	// Le shift_id peut arriver colle a sa plage (« debut-fin ») : la carte se range sous le debut.
	if i := strings.Index(commun.shiftID, "-"); i > 0 {
		commun.shiftID = commun.shiftID[:i]
	}
	if commun.shiftID == "" && commun.shiftTime != "" {
		if i := strings.Index(commun.shiftTime, "-"); i > 0 {
			commun.shiftID = commun.shiftTime[:i]
		}
	}

	for session, entree := range perso.GetFields() {
		uid := uidParSession(session)
		if uid == "" || quartDejaCredite(cleMatch, uid) {
			continue
		}
		g := commun
		g.oeufsOr = sommeParNom(entree, "golden_ikura_num")
		g.oeufs = sommeParNom(entree, "ikura_num")
		g.sauvetages = sommeParNom(entree, "rescue_num")
		crediterCartesDePoints(uid, g)
	}
}
