// Recopie un SaveRecord S3 vers un AUTRE joueur, en le retargetant proprement.
//
// Sert a doter un testeur d'une sauvegarde deja avancee sans reactiver l'amorcage global depuis la
// capture — c'est ce dernier qui avait produit l'incident « tout nouveau joueur a l'identite gen »,
// et il reste desactive par defaut pour cette raison (voir cloudsave_nextendo.go).
//
// Deux champs DOIVENT changer, sinon on cree un doublon d'identite :
//   - `name`, qui porte l'uid du proprietaire ;
//   - `Identifier`, le discriminant a quatre chiffres affiche a cote du pseudo, derive de l'uid
//     exactement comme le fait le serveur (identifierFor).
//
//	usage : seedsave <source.record.pb> <uid-destination> <destination.record.pb>
package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

const tenant = "tenants/t-dce9377b-lp1"

func identifierFor(uid string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(uid))
	return fmt.Sprintf("%04d", h.Sum32()%10000)
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("usage : seedsave <source.record.pb> <uid-destination> <destination.record.pb>")
		os.Exit(2)
	}
	src, uid, dst := os.Args[1], os.Args[2], os.Args[3]

	blob, err := os.ReadFile(src)
	if err != nil {
		fmt.Println("lecture source :", err)
		os.Exit(1)
	}
	var rec toyohrpb.SaveRecord
	if err := proto.Unmarshal(blob, &rec); err != nil {
		fmt.Println("decodage source :", err)
		os.Exit(1)
	}
	if rec.GetSaveData() == nil {
		fmt.Println("source sans save_data — refus")
		os.Exit(1)
	}

	ancienNom := rec.GetName()
	rec.Name = tenant + "/saveRecords/" + uid

	ident := identifierFor(uid)
	if rec.SaveData.Fields == nil {
		rec.SaveData.Fields = map[string]*commonpb.Value{}
	}
	ancienIdent := rec.SaveData.Fields["Identifier"].GetStringValue()
	rec.SaveData.Fields["Identifier"] = &commonpb.Value{
		ValueType: &commonpb.Value_StringValue{StringValue: ident},
	}

	// L'horodatage doit avancer : le client renvoie cette valeur comme update_time de son ecriture
	// suivante, et un horodatage fige lui fait croire que le nuage n'a pas bouge.
	rec.UpdateTime = timestamppb.New(time.Now().UTC())

	sortie, err := proto.Marshal(&rec)
	if err != nil {
		fmt.Println("encodage :", err)
		os.Exit(1)
	}
	if err := os.WriteFile(dst, sortie, 0o644); err != nil {
		fmt.Println("ecriture :", err)
		os.Exit(1)
	}

	fmt.Printf("recopie : %s -> %s\n", src, dst)
	fmt.Printf("  name       : %s  ->  %s\n", ancienNom, rec.GetName())
	fmt.Printf("  Identifier : %q  ->  %q\n", ancienIdent, ident)
	fmt.Printf("  cles       : %d\n", len(rec.GetSaveData().GetFields()))
	fmt.Printf("  %d octets ecrits\n", len(sortie))
}
