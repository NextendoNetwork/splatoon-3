package main

// Verrou sur la lecture de la telemetrie Pia.
//
// Les octets et les valeurs viennent de la capture du Splatfest officiel du 2026-08-23 contre les
// serveurs de Nintendo : la partie tricolore qui s'est jouee (effectif 8) et le salon qui s'est
// vide juste apres (effectif 7).

import (
	"testing"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

func entier(n int64) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_IntegerValue{IntegerValue: n}}
}

func chaine(s string) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_StringValue{StringValue: s}}
}

func booleen(b bool) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_BooleanValue{BooleanValue: b}}
}

func octets(b []byte) *commonpb.Value {
	return &commonpb.Value{ValueType: &commonpb.Value_BytesValue{BytesValue: b}}
}

func carte(f map[string]*commonpb.Value) *commonpb.MapValue {
	return &commonpb.MapValue{Fields: f}
}

func TestEffectifLuDansPl(t *testing.T) {
	// Octets releves tels quels dans la capture.
	cas := []struct {
		brut    []byte
		attendu int
		quoi    string
	}{
		{[]byte{0x01, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x01}, 8, "la partie tricolore qui s est jouee"},
		{[]byte{0x01, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x01}, 7, "le salon qui s est vide"},
		{[]byte{0x01, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01}, 2, "un salon qui commence a se remplir"},
	}
	for _, c := range cas {
		n, ok := effectifDepuisPl(carte(map[string]*commonpb.Value{"pl": octets(c.brut)}))
		if !ok {
			t.Errorf("%s : lecture refusee", c.quoi)
			continue
		}
		if n != c.attendu {
			t.Errorf("%s : effectif %d, attendu %d", c.quoi, n, c.attendu)
		}
	}
}

func TestPlDeTailleInattendueEstRefuse(t *testing.T) {
	// L'octet d'effectif est une INFERENCE sur cinq observations. Si la forme change, on prefere
	// ne rien dire plutot que journaliser un chiffre faux.
	if _, ok := effectifDepuisPl(carte(map[string]*commonpb.Value{"pl": octets([]byte{0x01, 0x02})})); ok {
		t.Error("pl de 2 octets accepte : la lecture doit se taire hors de la forme mesuree")
	}
	if _, ok := effectifDepuisPl(carte(map[string]*commonpb.Value{})); ok {
		t.Error("pl absent accepte")
	}
	if _, ok := effectifDepuisPl(nil); ok {
		t.Error("carte nulle acceptee")
	}
}

func TestLiensPairParPairSontCollectes(t *testing.T) {
	// Forme relevee : un bloc par pair, avec p2p / lu / ru / rc / re.
	doc := carte(map[string]*commonpb.Value{
		"suid": chaine("u-exemple7000000000000"),
		"liens": {ValueType: &commonpb.Value_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.Value{
			{ValueType: &commonpb.Value_MapValue{MapValue: carte(map[string]*commonpb.Value{
				"p2p": chaine("nn::pia::nplnd::NplnPlugin"),
				"lu":  chaine("u-exemple7000000000000"),
				"ru":  chaine("u-exemple2000000000000"),
				"rc":  entier(1),
				"re":  booleen(false),
			})}},
			{ValueType: &commonpb.Value_MapValue{MapValue: carte(map[string]*commonpb.Value{
				"p2p": chaine("nn::pia::nplnd::NplnPlugin"),
				"lu":  chaine("u-exemple7000000000000"),
				"ru":  chaine("u-exemple3000000000000"),
				"rc":  entier(3),
				"re":  booleen(true),
			})}},
		}}}},
	})

	var liens []rapportPia
	rapportsPia(&commonpb.Value{ValueType: &commonpb.Value_MapValue{MapValue: doc}}, &liens)
	if len(liens) != 2 {
		t.Fatalf("%d lien(s) collecte(s), attendu 2 — on cherche par NOM, en profondeur", len(liens))
	}

	var vuDegrade bool
	for _, l := range liens {
		if l.Local != "u-exemple7000000000000" {
			t.Errorf("utilisateur local %q perdu", l.Local)
		}
		if l.Pair == "u-exemple3000000000000" {
			vuDegrade = true
			if l.Connexions != 3 || !l.Erreur {
				t.Errorf("lien degrade mal lu : rc=%d re=%v, attendu rc=3 re=true", l.Connexions, l.Erreur)
			}
		}
	}
	if !vuDegrade {
		t.Error("le lien en erreur n a pas ete retrouve : c est precisement celui qu on veut voir")
	}
}

func TestBlocSansPairEstIgnore(t *testing.T) {
	// Le document porte plein de champs qui ne parlent pas de pairs : ils ne doivent rien produire.
	doc := carte(map[string]*commonpb.Value{
		"susid":  chaine("55141144-61a5-4181-a9e8-181d458d7650"),
		"suscid": entier(10008),
		"sussid": entier(8),
	})
	var liens []rapportPia
	rapportsPia(&commonpb.Value{ValueType: &commonpb.Value_MapValue{MapValue: doc}}, &liens)
	if len(liens) != 0 {
		t.Errorf("%d lien(s) inventes a partir d un document sans « ru »", len(liens))
	}
}

func TestJournalNeSeRepetePas(t *testing.T) {
	if !aChange("essai|x", "8") {
		t.Error("premiere valeur : doit etre annoncee")
	}
	if aChange("essai|x", "8") {
		t.Error("meme valeur : ne doit PAS etre reannoncee, les consoles ecrivent en rafale")
	}
	if !aChange("essai|x", "7") {
		t.Error("valeur changee : doit etre annoncee — c est le passage 8 -> 7 qui nous interesse")
	}
}
