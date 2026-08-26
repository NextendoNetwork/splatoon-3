// Decode un KeepUserSessionResponse capture sur l'hote de session de Nintendo.
package main

import (
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	gspb "npln.nintendo.net/npln-practice/proto/gamesync/v1"
)

func main() {
	for _, f := range os.Args[1:] {
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var r gspb.KeepUserSessionResponse
		if err := proto.Unmarshal(b, &r); err != nil {
			fmt.Printf("== %s (%d o) illisible : %v\n", f, len(b), err)
			continue
		}
		fmt.Printf("========== %s (%d o) ==========\n%s\n", f, len(b), prototext.Format(&r))
	}
}
