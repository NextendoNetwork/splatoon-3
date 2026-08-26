// Commande de diagnostic : decode une capture Nintendo de MatchmakingTicket en texte lisible,
// pour la comparer champ par champ a ce que notre serveur emet.
package main

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	mmpb "npln.nintendo.net/npln-practice/proto/matchmaking/v1"
)

func main() {
	for _, f := range os.Args[1:] {
		b, err := os.ReadFile(f)
		if err != nil {
			fmt.Println("ERR", err)
			continue
		}

		// Corps protobuf nu (pas d'entete gRPC) : c'est ce que SendMsg encadre a l'emission.
		var whole mmpb.MatchmakingTicket
		if err := proto.Unmarshal(b, &whole); err == nil {
			fmt.Printf("========== %s : corps nu (%d o) ==========\n", f, len(b))
			fmt.Println(prototext.Format(&whole))
			continue
		} else {
			fmt.Println("(corps nu illisible:", err, ")")
		}

		// Sinon : un ou plusieurs cadres gRPC (5 o d'entete chacun).
		off, idx := 0, 0
		for off+5 <= len(b) {
			n := int(b[off+1])<<24 | int(b[off+2])<<16 | int(b[off+3])<<8 | int(b[off+4])
			if n <= 0 || off+5+n > len(b) {
				break
			}
			var t mmpb.MatchmakingTicket
			if err := proto.Unmarshal(b[off+5:off+5+n], &t); err != nil {
				fmt.Printf("== %s cadre#%d (%d o) illisible: %v\n", f, idx, n, err)
			} else {
				fmt.Printf("========== %s cadre#%d (%d o) ==========\n", f, idx, n)
				fmt.Println(prototext.Format(&t))
			}
			off += 5 + n
			idx++
		}
		fmt.Printf("---- %s : %d cadre(s), %d/%d octets consommes ----\n", f, idx, off, len(b))
	}
}
