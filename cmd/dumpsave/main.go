// Affiche l'identite d'un SaveRecord S3 : de qui est cette sauvegarde, et ou elle en est.
//
// Sert avant toute operation qui ecrase une sauvegarde : on regarde la cible AVANT d'ecrire.
package main

import (
	"fmt"
	"os"
	"sort"

	"google.golang.org/protobuf/proto"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// Les cles qui disent qui est le joueur et ou il en est.
var interessantes = []string{
	"UserName", "PlayerName", "Identifier", "Money", "PlayerRank", "Rank", "PaintPoint",
	"NameId", "Byname", "PlayerSkinId", "SaveDataVersion",
}

func main() {
	for _, f := range os.Args[1:] {
		blob, err := os.ReadFile(f)
		if err != nil {
			fmt.Printf("%s : illisible (%v)\n", f, err)
			continue
		}
		var rec toyohrpb.SaveRecord
		if err := proto.Unmarshal(blob, &rec); err != nil {
			fmt.Printf("%s : decodage impossible (%v)\n", f, err)
			continue
		}
		champs := rec.GetSaveData().GetFields()
		fmt.Printf("\n===== %s (%d o) =====\n", f, len(blob))
		fmt.Printf("  name        : %s\n", rec.GetName())
		fmt.Printf("  create_time : %s\n", rec.GetCreateTime().AsTime())
		fmt.Printf("  update_time : %s\n", rec.GetUpdateTime().AsTime())
		fmt.Printf("  cles        : %d\n", len(champs))
		for _, k := range interessantes {
			if v, ok := champs[k]; ok {
				fmt.Printf("  %-16s = %v\n", k, valeur(v))
			}
		}
		// Les plus grosses cles donnent une idee de la progression reelle.
		type kv struct {
			k string
			n int
		}
		var gros []kv
		for k, v := range champs {
			gros = append(gros, kv{k, proto.Size(v)})
		}
		sort.Slice(gros, func(a, b int) bool { return gros[a].n > gros[b].n })
		fmt.Print("  plus grosses cles :")
		for i := 0; i < len(gros) && i < 5; i++ {
			fmt.Printf(" %s(%do)", gros[i].k, gros[i].n)
		}
		fmt.Println()
	}
}

func valeur(v interface{ String() string }) string { return v.String() }
