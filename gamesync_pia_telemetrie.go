package main

// Ce que la console dit du P2P — et qu'on jetait.
//
// D'OU CA VIENT. Capture d'un Splatfest officiel contre les serveurs de Nintendo, 2026-08-23,
// relais-session. Pendant une partie, chaque console ecrit dans son document de statut
// « docs/__pgn/All/__stu/<uuid> » un bilan de ses liens PAIR PAR PAIR :
//
//	p2p  nn::pia::nplnd::NplnPlugin   le greffon Pia qui rapporte
//	lu   u-...                        l'utilisateur LOCAL (celui qui parle)
//	ru   u-...                        l'utilisateur DISTANT (le pair juge)
//	rc   1                            un compteur de connexion pour ce pair
//	re   false                        un drapeau d'erreur pour ce pair
//
// Le meme document porte « pl », neuf octets dont le HUITIEME suit l'effectif de la session.
// Mesure sur quatre sessions consecutives de la meme capture : 8 dans la partie qui s'est jouee,
// 7 dans le salon qui s'est vide, puis 2, puis 4 et 5 dans les salons qui se remplissaient. Les
// huit autres octets n'ont jamais varie. C'est une lecture INFEREE de cinq observations, pas une
// specification : on la journalise en la nommant, jamais on ne s'en sert pour decider.
//
// POURQUOI CA COMPTE. Le 2026-08-22, huit consoles ont quitte une partie ENSEMBLE, quatorze
// secondes apres l'avoir rejointe, sans que rien cote serveur ne l'explique : matchmaking sain,
// allocations TURN a cent pour cent, aucune fermeture de notre fait. Il manquait le point de vue
// des consoles. Elles nous le donnaient depuis le debut, dans ces ecritures, et nous les
// enregistrions sans les lire.
//
// CE QUE CA NE PROUVE PAS. Dans la capture Nintendo, le salon qui s'est VIDE affichait re=false
// et rc=1 sur ses six pairs — exactement comme la partie qui a abouti. Un salon qui se dissout
// n'est donc pas forcement un salon en erreur, et ce journal ne designera pas un coupable a chaque
// fois. Il dit ce que les consoles constatent ; c'est deja ce qui nous manquait.

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	gspb "npln.nintendo.net/npln-practice/proto/gamesync/v1"
)

// prefixeStatutJoueur : le document ou chaque console ecrit son statut de session.
const prefixeStatutJoueur = "docs/__pgn/All/__stu/"

// indiceEffectifDansPl : position, dans les neuf octets de « pl », de l'octet qui suit l'effectif.
// Voir l'en-tete : inference sur cinq observations.
const indiceEffectifDansPl = 7
const tailleAttenduePl = 9

// rapportPia : le bilan d'UN lien, tel que la console le voit.
type rapportPia struct {
	Local      string
	Pair       string
	Connexions int64
	Erreur     bool
	Greffon    string
}

// derniersEtats evite de repeter la meme ligne a chaque ecriture : les consoles ecrivent en
// rafale, et un journal qui se repete est un journal qu'on ne lit plus.
var derniersEtats = struct {
	sync.Mutex
	m map[string]string
}{m: map[string]string{}}

// aChange dit si la valeur associee a cette cle differe de la derniere journalisee.
func aChange(cle, valeur string) bool {
	derniersEtats.Lock()
	defer derniersEtats.Unlock()
	if derniersEtats.m[cle] == valeur {
		return false
	}
	derniersEtats.m[cle] = valeur
	return true
}

// rapportsPia collecte, en profondeur, tout bloc qui decrit un lien vers un pair.
//
// On reconnait un bloc a la presence de « ru » : c'est le seul champ dont la presence signifie
// « ceci parle d'un pair distant ». Chercher par NOM plutot que par position, comme sommeParNom,
// parce que la capture ne nous garantit pas la profondeur.
func rapportsPia(v *commonpb.Value, out *[]rapportPia) {
	if v == nil {
		return
	}
	if m := v.GetMapValue(); m != nil {
		f := m.GetFields()
		if pair, ok := f["ru"]; ok && pair.GetStringValue() != "" {
			*out = append(*out, rapportPia{
				Local:      f["lu"].GetStringValue(),
				Pair:       pair.GetStringValue(),
				Connexions: f["rc"].GetIntegerValue(),
				Erreur:     f["re"].GetBooleanValue(),
				Greffon:    f["p2p"].GetStringValue(),
			})
		}
		for _, sv := range f {
			rapportsPia(sv, out)
		}
	}
	for _, e := range v.GetArrayValue().GetValues() {
		rapportsPia(e, out)
	}
}

// effectifDepuisPl rend l'effectif que la console annonce, et s'il a pu etre lu.
func effectifDepuisPl(m *commonpb.MapValue) (int, bool) {
	if m == nil {
		return 0, false
	}
	b := m.GetFields()["pl"].GetBytesValue()
	if len(b) != tailleAttenduePl {
		return 0, false
	}
	return int(b[indiceEffectifDansPl]), true
}

// courtUID raccourcit un identifiant pour que le resume tienne sur une ligne lisible.
func courtUID(uid string) string {
	if len(uid) <= 10 {
		return uid
	}
	return uid[:10]
}

// journaliserTelemetriePia ecrit ce que les consoles constatent de leurs liens.
//
// Appele depuis WriteDocuments, avant l'application au magasin : on veut la trace meme si
// l'ecriture est ensuite ignoree.
func journaliserTelemetriePia(gsid string, ops []*gspb.WriteOperation) {
	for _, op := range ops {
		var d *gspb.Document
		switch {
		case op.GetUpdateDocument() != nil:
			d = op.GetUpdateDocument().GetDocument()
		case op.GetMergeDocument() != nil:
			d = op.GetMergeDocument().GetDocument()
		}
		if d == nil || !strings.HasPrefix(d.GetName(), prefixeStatutJoueur) {
			continue
		}
		champs := d.GetFields()
		auteur := champs.GetFields()["suid"].GetStringValue()
		if auteur == "" {
			auteur = strings.TrimPrefix(d.GetName(), prefixeStatutJoueur)
		}

		// L'effectif : c'est lui qui montre un salon se remplir, puis se vider.
		if n, ok := effectifDepuisPl(champs); ok {
			if aChange("effectif|"+gsid+"|"+auteur, fmt.Sprintf("%d", n)) {
				log.Printf("[NPLN pia] effectif annonce par la console : %d | partie=%s uid=%s", n, gsid, auteur)
			}
		}

		var liens []rapportPia
		rapportsPia(&commonpb.Value{ValueType: &commonpb.Value_MapValue{MapValue: champs}}, &liens)
		if len(liens) == 0 {
			continue
		}

		sort.Slice(liens, func(i, j int) bool { return liens[i].Pair < liens[j].Pair })
		var degrades []string
		var detail []string
		for _, l := range liens {
			detail = append(detail, fmt.Sprintf("%s(rc=%d,re=%v)", courtUID(l.Pair), l.Connexions, l.Erreur))
			if l.Erreur || l.Connexions > 1 {
				degrades = append(degrades, fmt.Sprintf("%s rc=%d re=%v", l.Pair, l.Connexions, l.Erreur))
			}
		}

		resume := strings.Join(detail, " ")
		if aChange("liens|"+gsid+"|"+auteur, resume) {
			log.Printf("[NPLN pia] %s voit %d pair(s) | partie=%s | %s", auteur, len(liens), gsid, resume)
		}
		// Un lien qui se plaint doit sortir SEUL, sans etre noye dans le resume.
		for _, e := range degrades {
			if aChange("alerte|"+gsid+"|"+auteur+"|"+e, e) {
				log.Printf("[NPLN pia] lien degrade rapporte par %s : %s | partie=%s", auteur, e, gsid)
			}
		}
	}
}
