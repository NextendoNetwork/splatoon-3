package main

// Journaliser TOUS les attributs qu'une console publie dans sa presence.
//
// POURQUOI. Le journal n'imprimait que quatre attributs choisis a la main — GameStatus, SessionId,
// CurrentParticipants, MaxParticipants — sur les treize que la console envoie. Les neuf autres
// arrivaient et disparaissaient sans laisser de trace.
//
// CE QUE CA A COUTE, mesure du 2026-08-23. Un joueur avait choisi son camp de Splatfest et le jeu
// le lui confirmait a l'ecran, mais notre comptage de ferveur ne voyait rien : zero CreateFestEntry,
// et « FestTeam » introuvable dans les journaux. Impossible de trancher entre « la console ne nous
// dit pas son camp » et « elle nous le dit et nous ne l'imprimons pas » — deux causes opposees, un
// seul silence. La capture Nintendo, elle, montre bien FestTeam dans la presence d'une vraie
// console (voir s3-splatfest-capture) : le doute portait donc sur NOTRE lecture.
//
// La regle qu'on en tire : quand on affiche un sous-ensemble choisi a la main, on se prive de tout
// ce qu'on n'avait pas prevu. On imprime donc l'inventaire complet, une fois par changement.

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
)

// derniereEmpreinteAttributs : les consoles republient leur presence en rafale et l'essentiel ne
// change pas. On ne reimprime que lorsque le contenu bouge reellement.
var derniereEmpreinteAttributs = struct {
	sync.Mutex
	m map[string]string
}{m: map[string]string{}}

// texteValeur rend une valeur NPLN sous une forme courte et lisible.
func texteValeur(v *commonpb.Value) string {
	switch {
	case v == nil:
		return "-"
	case v.GetStringValue() != "":
		return fmt.Sprintf("%q", v.GetStringValue())
	case v.GetBytesValue() != nil:
		return fmt.Sprintf("%d octet(s)", len(v.GetBytesValue()))
	case v.GetMapValue() != nil:
		return fmt.Sprintf("{%d champ(s)}", len(v.GetMapValue().GetFields()))
	case v.GetArrayValue() != nil:
		return fmt.Sprintf("[%d]", len(v.GetArrayValue().GetValues()))
	case v.GetBooleanValue():
		return "true"
	case v.GetIntegerValue() != 0:
		return fmt.Sprintf("%d", v.GetIntegerValue())
	case v.GetDoubleValue() != 0:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	}
	// Reste les zeros explicites : un entier a 0 et un booleen faux se ressemblent, on tranche
	// sur le type porte par la valeur.
	if _, ok := v.GetValueType().(*commonpb.Value_BooleanValue); ok {
		return "false"
	}
	return "0"
}

// inventaireDesAttributs rend la liste complete « cle=valeur », triee, d'une presence.
func inventaireDesAttributs(attrs map[string]*commonpb.Value) string {
	cles := make([]string, 0, len(attrs))
	for k := range attrs {
		cles = append(cles, k)
	}
	sort.Strings(cles)

	morceaux := make([]string, 0, len(cles))
	for _, k := range cles {
		morceaux = append(morceaux, k+"="+texteValeur(attrs[k]))
	}
	return strings.Join(morceaux, " ")
}

// attributsOntChange dit si l'inventaire de ce joueur differe du dernier journalise.
func attributsOntChange(uid, inventaire string) bool {
	derniereEmpreinteAttributs.Lock()
	defer derniereEmpreinteAttributs.Unlock()
	if derniereEmpreinteAttributs.m[uid] == inventaire {
		return false
	}
	derniereEmpreinteAttributs.m[uid] = inventaire
	return true
}
