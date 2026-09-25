package main

// GameRecord/CreateFestPowerMeasurement — la mesure de puissance de festival.
//
// LE PIEGE, deja rencontre avec InitializeAttributes. Le rejeu generique n'avait pas de capture
// pour cet appel et repondait un message VIDE. Le jeu n'apprenait donc pas quel document porte sa
// mesure : il le relisait, ne le trouvait pas, rappelait la creation, et tournait en rond.
// Mesure du 2026-08-23, sur un joueur bloque a « Connexion a Internet » : 1 058 creations et
// 2 120 lectures en DEUX minutes, vingt-six allers-retours par seconde. Chaque reponse partait en
// une milliseconde — ce n'etait pas une lenteur, c'etait une boucle.
//
// CE QU'IL NE FAUT PAS FAIRE, et que j'ai fait d'abord : deduire le nom du document du champ
// `parent` de la requete. Ce parent vaut « tenants/current/users/current », qui n'est PAS un nom
// de document. Le jeu l'a recu, son SDK ugcstore a tente de le parser contre la grammaire
// tenants/<t>/documents/<reste>, a echoue, et a rendu 2162-0001 — ResultErrApplicationAborted
// dans nn.npln.Worker, jeu ferme. Un nom invente est pire qu'une reponse vide.
//
// CE QUE FAIT NINTENDO, mesure sur une capture de compte neuf, echange 47 (requete 65 o,
// reponse 430 o). Le document ne s'appelle pas comme le parent, ni comme le WeaponPowerMeasurement
// que le jeu lit par ailleurs :
//
//	1 (name)     tenants/<t>/users/<uid>/festPowerMeasurements/<festId>
//	2 (fields)   fest_id, npln_user_id, fest_power_challenge, is_initial=true,
//	             created_at, battle_num=0
//	5 (document) tenants/<t>/documents/services/GameRecord/users/<uid>/FestPowerMeasurement/<festId>
//
// On rejoue donc cette capture, avec deux substitutions de meme longueur — l'identifiant de compte
// (22 caracteres) et celui de la fete (10) — pour que le protobuf reste valide octet pour octet.
// L'identifiant de fete vient de la REQUETE : c'est le jeu qui dit de quelle fete il parle.

import (
	"bytes"
	"log"
	"regexp"

	"google.golang.org/protobuf/proto"

	common "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

var rawMesureDeFete = capture("captured/CreateFestPowerMeasurement.bin")

// La fete de la capture. Tous les identifiants de fete font dix caracteres, donc l'echange se fait
// octet a octet sans toucher aux prefixes de longueur.
const feteDeLaCapture = "JUEA-00201"

var motifFeteS3 = regexp.MustCompile(`^[A-Z]{4}-[0-9]{5}$`)

// feteDeLaRequete lit l'identifiant que le jeu vient de soumettre.
func feteDeLaRequete(requete []byte) string {
	req := &toyohrpb.CreateFestPowerMeasurementRequest{}
	if err := proto.Unmarshal(requete, req); err != nil {
		return ""
	}
	v := req.GetFestPowerMeasurement().GetFields().GetFields()["fest_id"]
	return v.GetStringValue()
}

// repondreMesureDeFete rend la reponse capturee, realignee sur la session en cours.
func repondreMesureDeFete(requete []byte, uid string) ([]byte, error) {
	corps := append([]byte(nil), rawMesureDeFete...)
	if len(corps) > 5 {
		corps = corps[5:] // la capture porte l'entete de trame gRPC
	}

	if uid != "" && len(uid) == len(captureVierge) {
		corps = motifUid.ReplaceAllFunc(corps, func(found []byte) []byte {
			if len(found) == len(uid) {
				return []byte(uid)
			}
			return found
		})
	}

	fete := feteDeLaRequete(requete)
	if fete != "" && len(fete) == len(feteDeLaCapture) && motifFeteS3.MatchString(fete) {
		corps = bytes.ReplaceAll(corps, []byte(feteDeLaCapture), []byte(fete))
	} else if fete != "" {
		log.Printf("[NPLN GameRecord] fete %q inattendue, capture laissee sur %s", fete, feteDeLaCapture)
		fete = feteDeLaCapture
	}

	mesure := &toyohrpb.FestPowerMeasurement{}
	if err := proto.Unmarshal(corps, mesure); err != nil {
		return nil, err
	}

	// Materialiser le document que la reponse NOMME — c'est lui que le jeu relit ensuite, et son
	// absence est ce qui relancait la boucle. Le nom vient de la capture, jamais d'une deduction.
	if nom := mesure.GetDocument(); nom != "" {
		champs := mesure.GetFields()
		if champs == nil {
			champs = &common.MapValue{Fields: map[string]*common.Value{}}
		}
		storePutDocument(&ugcpb.Document{
			Name:       nom,
			Fields:     champs,
			CreateTime: mesure.GetCreateTime(),
			UpdateTime: mesure.GetUpdateTime(),
		})
		log.Printf("[NPLN GameRecord] CreateFestPowerMeasurement fete=%s -> document %q (%d champ(s))",
			fete, nom, len(champs.GetFields()))
	} else {
		log.Printf("[NPLN GameRecord] CreateFestPowerMeasurement fete=%s -> reponse capturee sans document", fete)
	}

	return corps, nil
}
