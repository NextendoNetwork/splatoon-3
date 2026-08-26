package main

// session_address_rewrite — pointer une reponse capturee vers NOTRE serveur de session, au niveau
// protobuf.
//
// LE PROBLEME MESURE
// La reponse de Nintendo a TrackGameSessionCreationTicket (captured/TrackGameSessionCreationTicket
// .bin, 1954 octets) decrit la session dans son champ 6, dont deux sous-champs donnent l'adresse a
// composer :
//
//	6.8 = "35.223.226.237"   (hote, chaine)
//	6.9 = 7090               (port, varint)
//
// Le rejeu se contentait d'un remplacement d'octets sur l'IP, avec deux consequences : le port
// restait 7090 alors que notre transport de session ecoute sur 7575, et l'hote etait COMPLETE PAR
// DES ESPACES pour conserver la longueur du message (relayHostPadded) — le jeu composait donc
// litteralement « 203.0.113.7 ». Ce chemin ne pouvait pas aboutir, quelle que soit la suite.
//
// LA CORRECTION
// On reecrit les deux sous-champs pour de vrai : le message est reserialise, les longueurs
// recalculees, et l'hote peut avoir n'importe quelle taille. Tout le reste de la capture — dont le
// bloc MatchedUserSession et le jeton signe — passe octet pour octet, puisqu'on ne touche qu'aux
// champs nommes.

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// Numeros releves sur la capture elle-meme (voir l'en-tete du fichier).
const (
	champGameSession protowire.Number = 6 // champ 6 de la reponse : la GameSession
	champHote        protowire.Number = 8 // sous-champ 8 : l'hote a composer
	champPort        protowire.Number = 9 // sous-champ 9 : le port
)

// reecrireAdresseSession remplace l'hote et le port de la GameSession portee par msg.
func reecrireAdresseSession(msg []byte, hote string, port uint32) ([]byte, error) {
	return reecrireChamp(msg, champGameSession, func(gs []byte) ([]byte, error) {
		return reecrireHotePort(gs, hote, port)
	})
}

// reecrireChamp reserialise msg en remplacant le contenu du champ numero cible par ce que rend
// muter. Les autres champs sont recopies tels quels, dans l'ordre d'origine.
func reecrireChamp(msg []byte, cible protowire.Number, muter func([]byte) ([]byte, error)) ([]byte, error) {
	var out []byte
	trouve := false

	for reste := msg; len(reste) > 0; {
		num, typ, n := protowire.ConsumeTag(reste)
		if n < 0 {
			return nil, fmt.Errorf("etiquette illisible : %w", protowire.ParseError(n))
		}
		reste = reste[n:]

		if num == cible && typ == protowire.BytesType {
			val, m := protowire.ConsumeBytes(reste)
			if m < 0 {
				return nil, fmt.Errorf("champ %d illisible : %w", cible, protowire.ParseError(m))
			}
			reste = reste[m:]

			nouveau, err := muter(val)
			if err != nil {
				return nil, err
			}
			out = protowire.AppendTag(out, num, typ)
			out = protowire.AppendBytes(out, nouveau)
			trouve = true
			continue
		}

		m := protowire.ConsumeFieldValue(num, typ, reste)
		if m < 0 {
			return nil, fmt.Errorf("champ %d illisible : %w", num, protowire.ParseError(m))
		}
		out = protowire.AppendTag(out, num, typ)
		out = append(out, reste[:m]...)
		reste = reste[m:]
	}

	if !trouve {
		return nil, fmt.Errorf("champ %d absent du message", cible)
	}
	return out, nil
}

// reecrireChaine remplace la valeur d'un champ chaine designe par son CHEMIN dans le message
// (par exemple {6, 1} pour le nom de la GameSession, {5, 2} pour celui de l'UserSession). Les
// messages intermediaires sont reserialises, donc la longueur de la nouvelle valeur est libre.
func reecrireChaine(msg []byte, chemin []protowire.Number, val string) ([]byte, error) {
	if len(chemin) == 0 {
		return nil, fmt.Errorf("chemin vide")
	}
	if len(chemin) == 1 {
		return remplacerChaine(msg, chemin[0], val)
	}
	return reecrireChamp(msg, chemin[0], func(sous []byte) ([]byte, error) {
		return reecrireChaine(sous, chemin[1:], val)
	})
}

// remplacerChaine reecrit (ou ajoute) le champ chaine numero cible.
func remplacerChaine(msg []byte, cible protowire.Number, val string) ([]byte, error) {
	var out []byte
	vu := false

	for reste := msg; len(reste) > 0; {
		num, typ, n := protowire.ConsumeTag(reste)
		if n < 0 {
			return nil, fmt.Errorf("etiquette illisible : %w", protowire.ParseError(n))
		}
		reste = reste[n:]

		if num == cible && typ == protowire.BytesType {
			_, m := protowire.ConsumeBytes(reste)
			if m < 0 {
				return nil, fmt.Errorf("champ %d illisible : %w", cible, protowire.ParseError(m))
			}
			reste = reste[m:]
			out = protowire.AppendTag(out, cible, protowire.BytesType)
			out = protowire.AppendString(out, val)
			vu = true
			continue
		}

		m := protowire.ConsumeFieldValue(num, typ, reste)
		if m < 0 {
			return nil, fmt.Errorf("champ %d illisible : %w", num, protowire.ParseError(m))
		}
		out = protowire.AppendTag(out, num, typ)
		out = append(out, reste[:m]...)
		reste = reste[m:]
	}

	if !vu {
		out = protowire.AppendTag(out, cible, protowire.BytesType)
		out = protowire.AppendString(out, val)
	}
	return out, nil
}

// reecrireHotePort remplace les sous-champs hote et port d'une GameSession. Si l'un des deux
// manque dans la capture, il est ajoute : une session sans adresse ne sert a rien.
func reecrireHotePort(gs []byte, hote string, port uint32) ([]byte, error) {
	var out []byte
	vuHote, vuPort := false, false

	for reste := gs; len(reste) > 0; {
		num, typ, n := protowire.ConsumeTag(reste)
		if n < 0 {
			return nil, fmt.Errorf("etiquette de GameSession illisible : %w", protowire.ParseError(n))
		}
		reste = reste[n:]

		switch {
		case num == champHote && typ == protowire.BytesType:
			_, m := protowire.ConsumeBytes(reste)
			if m < 0 {
				return nil, fmt.Errorf("hote illisible : %w", protowire.ParseError(m))
			}
			reste = reste[m:]
			out = protowire.AppendTag(out, champHote, protowire.BytesType)
			out = protowire.AppendString(out, hote)
			vuHote = true

		case num == champPort && typ == protowire.VarintType:
			_, m := protowire.ConsumeVarint(reste)
			if m < 0 {
				return nil, fmt.Errorf("port illisible : %w", protowire.ParseError(m))
			}
			reste = reste[m:]
			out = protowire.AppendTag(out, champPort, protowire.VarintType)
			out = protowire.AppendVarint(out, uint64(port))
			vuPort = true

		default:
			m := protowire.ConsumeFieldValue(num, typ, reste)
			if m < 0 {
				return nil, fmt.Errorf("sous-champ %d illisible : %w", num, protowire.ParseError(m))
			}
			out = protowire.AppendTag(out, num, typ)
			out = append(out, reste[:m]...)
			reste = reste[m:]
		}
	}

	if !vuHote {
		out = protowire.AppendTag(out, champHote, protowire.BytesType)
		out = protowire.AppendString(out, hote)
	}
	if !vuPort {
		out = protowire.AppendTag(out, champPort, protowire.VarintType)
		out = protowire.AppendVarint(out, uint64(port))
	}
	return out, nil
}
