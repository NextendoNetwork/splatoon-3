package main

// NOS PROPRES CLES DE FESTIVAL — pour qu'une fete puisse en suivre une autre.
//
// LE DEFAUT QU'ON CORRIGE. Nous servions toujours la meme fete, JUEA-00201, parce que c'est la
// seule dont nous avions les cles capturees. Or la sauvegarde d'un joueur retient l'identifiant de
// la fete qu'il a rejointe : au deuxieme festival portant le meme identifiant, le jeu affiche « tu
// as deja rejoint cette equipe » et ne propose plus de choisir. Autrement dit, avec un identifiant
// fige, un seul festival est jouable dans toute la vie d'un compte.
//
// CE QUI LE REND POSSIBLE. Le chiffrement des paquets a ete perce le 2026-08-20 : schema nisasyst,
// AES-128-CBC, IV derive du NOM DE RESSOURCE, cle servie par GetFestDecryptionKey. Les deux
// entrees sont donc a NOUS : nous choisissons le nom de ressource, et nous choisissons la cle.
// Rien n'oblige a reprendre celles de Nintendo — il suffit que les trois maillons s'accordent :
//
//	1. le paquet BCAT, chiffre avec NOTRE cle sous NOTRE nom de ressource ;
//	2. « festid » / « festressource » / « festrevision », que le serveur annonce ;
//	3. la reponse de GetFestDecryptionKey, qui nomme la meme fete et porte la meme cle.
//
// COMMENT S'EN SERVIR. Un drapeau a chaud « festcles=<hexa>[,<hexa>[,<hexa>]] », une cle par
// paquet et dans l'ordre ou le jeu les demande : base, puis bh, puis le paquet de resultat. Chaque
// cle fait 32 caracteres hexadecimaux, soit 16 octets. Pose-le avec « festid » et
// « festressource », et rechiffre les paquets avec les memes valeurs (tools/s3-bcat-studio).
//
// Sans ce drapeau, rien ne change : on rend la reponse capturee, comme avant.

import (
	"encoding/hex"
	"log"
	"strings"
)

// clesDeFeteMaison rend les cles posees a chaud, ou nil s'il n'y en a pas.
//
// Une cle mal formee est REFUSEE et signalee, pas servie de travers : une cle fausse donne un
// paquet illisible et une erreur de communication dans le hall, sans rien dire de la cause.
func clesDeFeteMaison() []string {
	v := soirFlagValeur("festcles")
	if v == "" || v == "1" {
		return nil
	}

	out := make([]string, 0, 3)
	for _, morceau := range strings.Split(v, ",") {
		k := strings.TrimSpace(strings.ToLower(morceau))
		if k == "" {
			continue
		}
		if len(k) != 32 {
			log.Printf("[NPLN fest] festcles : %q fait %d caractere(s), il en faut 32 — cle ignoree", k, len(k))
			continue
		}
		if _, err := hex.DecodeString(k); err != nil {
			log.Printf("[NPLN fest] festcles : %q n'est pas de l'hexadecimal — cle ignoree", k)
			continue
		}
		out = append(out, k)
		if len(out) == 3 {
			break // le jeu n'en demande jamais plus de trois
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// reponseClesMaison fabrique la reponse de GetFestDecryptionKey pour NOTRE fete.
//
// Meme forme que la capture : champ 1 le nom de la ressource, champs 2 a 4 les cles, champ 8 le
// camp en tete. On omet une cle absente plutot que d'en inventer une — c'est ce que fait Nintendo
// quand une phase n'a pas encore eu lieu.
func reponseClesMaison(festID string, cles []string, campEnTete string) []byte {
	var b []byte
	b = appendProtoString(b, 1, npnTenant+"/fests/"+festID+"/decryptionKey")
	for i, k := range cles {
		b = appendProtoString(b, 2+i, k) // 2 = notice, 3 = depart, 4 = resultat
	}
	if campEnTete != "" {
		b = appendProtoString(b, 8, campEnTete)
	}
	return b
}
