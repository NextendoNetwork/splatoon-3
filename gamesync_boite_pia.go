package main

// La boite aux lettres Pia — comment les consoles s'echangent leurs points de contact.
//
// D'OU CA VIENT. Captures relais-session SESSION-002/003/004 (vraie Switch contre les serveurs de
// Nintendo). Mesure du 2026-08-25 :
//
//	montant    : la console ecrit son blob « pl » dans __stu/<A>   (9, 78 puis 251 octets)
//	descendant : elle recoit QUATRE blobs (251, 251, 78, 78) sur __stu/<B>, un SEUL document
//
// Les blobs recus ne sont pas ceux qu'elle a ecrits, et le document qui les porte n'est pas celui
// ou elle ecrit. En decodant ce document on trouve :
//
//	suid   «u-exemple8000000000000»   l'utilisateur du PAIR
//	susid  «a1ac3f87-c2e6-44c6-…»     la userSession du PAIR — PAS le nom du document
//	sussid  ENTIER 1
//	suscid  ENTIER 10001
//	pl      octets                    le blob du PAIR
//
// Autrement dit le document surveille est une BOITE AUX LETTRES : il garde le nom de son
// destinataire, et le serveur y depose tour a tour les donnees de chaque pair, une poussee par
// pair. C'est par la que Pia apprend qui joindre.
//
// CE QU'ON FAISAIT. Nos consoles surveillent leur PROPRE document (mesure : 29 lignes de watch,
// toutes avec watch=<uss>), et nous n'y renvoyions que leur propre etat. Chacune ne voyait donc
// qu'elle-meme : « effectif annonce par la console : 1 », treize fois en dix minutes, alors que le
// serveur asseyait huit joueurs — et le salon se vidait de 8 a 0 en trente-cinq secondes.

import (
	"sort"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

// suscidDeBase : l'ecart constant releve dans la capture entre sussid (1) et suscid (10001).
const suscidDeBase = 10000

// boitesAuxLettresPia rend, pour la console « uss », l'etat de CHAQUE AUTRE participant de sa
// partie, au schema exact de la capture. Un envoi par pair, tous sur le document que la console
// surveille deja.
func (g *gamesyncServer) boitesAuxLettresPia(uss string) []*commonpb.MapValue {
	g.mu.Lock()
	defer g.mu.Unlock()

	moi := g.sess[uss]
	if moi == nil || moi.gsid == "" {
		return nil
	}

	autres := make([]string, 0, 8)
	for u, s := range g.sess {
		if u == uss || s == nil || s.gsid != moi.gsid {
			continue
		}
		autres = append(autres, u)
	}
	// Ordre stable : deux poussees successives doivent presenter les pairs dans le meme ordre,
	// sinon la console voit un remaniement a chaque envoi.
	sort.Strings(autres)

	out := make([]*commonpb.MapValue, 0, len(autres))
	for _, u := range autres {
		info := g.sess[u]
		stocke := g.store[cleStore(info.gsid, prefixeStatutJoueur+u)]
		pl := stocke.GetFields()["pl"]
		if pl == nil || len(pl.GetBytesValue()) == 0 {
			continue // ce pair n'a encore rien publie : rien a transmettre
		}
		rang := info.rang
		if rang <= 0 {
			rang = 1
		}
		out = append(out, &commonpb.MapValue{Fields: map[string]*commonpb.Value{
			"suid":   gsStr(info.uid),
			"susid":  gsStr(u), // la userSession DU PAIR
			"sussid": gsInt(int64(rang)),
			"suscid": gsInt(int64(suscidDeBase + rang)),
			"pl":     pl,
		}})
	}
	return out
}
