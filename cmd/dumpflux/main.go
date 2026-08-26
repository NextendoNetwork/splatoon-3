// Extrait les messages gRPC d'une capture HTTP/2 en clair, par flux, en identifiant la methode.
//
// Les entetes HTTP/2 sont compressees en HPACK, mais les chemins apparaissent en clair quand le
// client les envoie en litteral non-huffman — ce qui est le cas de Splatoon 3. On repere donc le
// flux qui porte une methode donnee en cherchant sa chaine dans les trames HEADERS, puis on
// rassemble les trames DATA de ce flux dans les deux sens.
//
//	usage : dumpflux <montant.bin> <descendant.bin> <fragment-de-methode>
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strings"
)

const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

type trame struct {
	typ  byte
	flux uint32
	corps []byte
}

func trames(b []byte) []trame {
	i := 0
	if len(b) >= len(preface) && string(b[:len(preface)]) == preface {
		i = len(preface)
	}
	var out []trame
	for i+9 <= len(b) {
		taille := int(b[i])<<16 | int(b[i+1])<<8 | int(b[i+2])
		typ := b[i+3]
		flux := binary.BigEndian.Uint32(b[i+5:i+9]) & 0x7fffffff
		fin := i + 9 + taille
		if fin > len(b) {
			break
		}
		out = append(out, trame{typ: typ, flux: flux, corps: b[i+9 : fin]})
		i = fin
	}
	return out
}

// messages decoupe une charge gRPC en messages (1 o compresse + 4 o longueur + corps).
func messages(charge []byte) [][]byte {
	var out [][]byte
	for p := 0; p+5 <= len(charge); {
		n := int(binary.BigEndian.Uint32(charge[p+1 : p+5]))
		if n < 0 || p+5+n > len(charge) {
			break
		}
		out = append(out, charge[p+5:p+5+n])
		p += 5 + n
	}
	return out
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("usage : dumpflux <montant.bin> <descendant.bin> <fragment-de-methode>")
		os.Exit(2)
	}
	mont, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println("montant :", err)
		os.Exit(1)
	}
	desc, err := os.ReadFile(os.Args[2])
	if err != nil {
		fmt.Println("descendant :", err)
		os.Exit(1)
	}
	cible := os.Args[3]

	// 1. Reperer les flux dont les HEADERS portent la methode cherchee.
	voulus := map[uint32]bool{}
	for _, t := range trames(mont) {
		if t.typ == 0x1 && strings.Contains(string(t.corps), cible) {
			voulus[t.flux] = true
		}
	}
	if len(voulus) == 0 {
		fmt.Printf("aucun flux ne porte %q dans ses entetes\n", cible)
		return
	}
	var ids []uint32
	for f := range voulus {
		ids = append(ids, f)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	fmt.Printf("methode %q -> flux %v\n", cible, ids)

	// 2. Rassembler les DATA de ces flux, dans les deux sens.
	for _, sens := range []struct {
		nom string
		b   []byte
	}{{"REQUETE (console -> Nintendo)", mont}, {"REPONSE (Nintendo -> console)", desc}} {
		parFlux := map[uint32][]byte{}
		for _, t := range trames(sens.b) {
			if t.typ == 0x0 && voulus[t.flux] {
				parFlux[t.flux] = append(parFlux[t.flux], t.corps...)
			}
		}
		for _, f := range ids {
			charge := parFlux[f]
			if len(charge) == 0 {
				continue
			}
			for n, m := range messages(charge) {
				fmt.Printf("\n===== %s — flux %d, message %d (%d o) =====\n", sens.nom, f, n, len(m))
				fmt.Printf("%s\n", champs(m, "  "))
			}
		}
	}
}

// champs rend un apercu lisible d'un message protobuf sans son schema.
func champs(b []byte, ind string) string {
	var sb strings.Builder
	i := 0
	for i < len(b) {
		cle, j, ok := varint(b, i)
		if !ok {
			break
		}
		num, fil := cle>>3, cle&7
		switch fil {
		case 0:
			v, k, ok2 := varint(b, j)
			if !ok2 {
				return sb.String()
			}
			fmt.Fprintf(&sb, "%schamp %d : varint %d\n", ind, num, v)
			i = k
		case 2:
			n, k, ok2 := varint(b, j)
			if !ok2 || k+int(n) > len(b) {
				return sb.String()
			}
			corps := b[k : k+int(n)]
			if lisible(corps) {
				fmt.Fprintf(&sb, "%schamp %d : %q\n", ind, num, string(corps))
			} else if sous := champs(corps, ind+"  "); sous != "" {
				fmt.Fprintf(&sb, "%schamp %d : message (%d o)\n%s", ind, num, len(corps), sous)
			} else {
				fmt.Fprintf(&sb, "%schamp %d : %d octets %x\n", ind, num, len(corps), tronque(corps))
			}
			i = k + int(n)
		case 5:
			fmt.Fprintf(&sb, "%schamp %d : fixed32\n", ind, num)
			i = j + 4
		case 1:
			fmt.Fprintf(&sb, "%schamp %d : fixed64\n", ind, num)
			i = j + 8
		default:
			return sb.String()
		}
	}
	return sb.String()
}

func lisible(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

func tronque(b []byte) []byte {
	if len(b) > 32 {
		return b[:32]
	}
	return b
}

func varint(b []byte, i int) (uint64, int, bool) {
	var v uint64
	var d uint
	for i < len(b) {
		c := b[i]
		i++
		v |= uint64(c&0x7f) << d
		if c&0x80 == 0 {
			return v, i, true
		}
		d += 7
		if d > 63 {
			return 0, i, false
		}
	}
	return 0, i, false
}
