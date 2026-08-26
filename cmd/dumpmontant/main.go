// Decode le flux MONTANT (console -> hote de session) capture par le relais.
//
// Le flux descendant nous a donne ce que Nintendo REPOND ; celui-ci donne ce que le jeu DEMANDE —
// en particulier la forme exacte de ses cibles de surveillance : liste de documents nommes, ou
// collection ? Notre serveur traite les deux differemment, et le salon prive depend de ce choix.
//
// On ne lit que les trames DATA : les HEADERS sont compressees en HPACK et ne portent que la
// methode gRPC, qu'on identifie de toute facon par le numero de flux.
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	gspb "npln.nintendo.net/npln-practice/proto/gamesync/v1"
)

const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: dumpmontant <fichier.montant.bin> [flux]")
		return
	}
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Println("lecture :", err)
		return
	}
	fluxVoulu := -1
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &fluxVoulu)
	}

	i := 0
	if len(b) >= len(preface) && string(b[:len(preface)]) == preface {
		i = len(preface)
	}

	// Concatener les DATA par flux : un message gRPC peut etre coupe en plusieurs trames.
	parFlux := map[uint32][]byte{}
	ordre := []uint32{}
	for i+9 <= len(b) {
		taille := int(b[i])<<16 | int(b[i+1])<<8 | int(b[i+2])
		typ := b[i+3]
		flux := binary.BigEndian.Uint32(b[i+5:i+9]) & 0x7fffffff
		fin := i + 9 + taille
		if fin > len(b) {
			break
		}
		if typ == 0x0 { // DATA
			if _, vu := parFlux[flux]; !vu {
				ordre = append(ordre, flux)
			}
			parFlux[flux] = append(parFlux[flux], b[i+9:fin]...)
		}
		i = fin
	}

	sort.Slice(ordre, func(a, c int) bool { return ordre[a] < ordre[c] })
	for _, flux := range ordre {
		if fluxVoulu >= 0 && uint32(fluxVoulu) != flux {
			continue
		}
		charge := parFlux[flux]
		fmt.Printf("\n############ FLUX %d — %d octets de DATA ############\n", flux, len(charge))
		n := 0
		for p := 0; p+5 <= len(charge); {
			taille := int(binary.BigEndian.Uint32(charge[p+1 : p+5]))
			if taille < 0 || p+5+taille > len(charge) {
				break
			}
			corps := charge[p+5 : p+5+taille]
			p += 5 + taille
			var r gspb.KeepUserSessionRequest
			if err := proto.Unmarshal(corps, &r); err != nil {
				fmt.Printf("--- message %d (%d o) : illisible comme KeepUserSessionRequest : %v\n", n, taille, err)
				n++
				continue
			}
			fmt.Printf("--- message %d (%d o) ---\n%s", n, taille, prototext.Format(&r))
			n++
		}
		fmt.Printf("############ %d message(s) ############\n", n)
	}
}
