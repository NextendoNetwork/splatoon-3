package main

// fest_align — make the Splatfest we ANNOUNCE match the one the console can actually load.
//
// The bug (found 2026-07-16 from S3's own popup "A communication error has occurred.
// (BcatInvalid)"): the fest lives in TWO places that must agree.
//
//   - The SCHEDULE comes from us, over NPLN (SelectFestSchedule).
//   - The RESOURCES come from the console's BCAT delivery cache, whose own metadata names the
//     fest it holds.
//
// When the announced fest and the cached packs disagree, S3 asks BCAT for a resource digest it
// cannot find and raises BcatInvalid. Same class of trap as the S2 BCAT schedule fix: the
// METADATA must line up, not the payload.
//
// We cannot synthesise a fest's packs (Nintendo's, encrypted, and we do not have them). We CAN
// announce the fest the cache really holds. That is what this does.
//
// The fest proto is only partially generated (FestSchedule has no Go type), so we rewrite by
// walking the decoded message with protoreflect and substituting string values — which also
// fixes up the wire lengths for free.
//
// ⚠️ AUCUNE VALEUR N'EST CODEE ICI. Les identifiants dependent entierement du cache BCAT que VOUS
// exploitez, et ce depot n'en distribue aucun. Deux drapeaux a chaud les fournissent :
//
//	festcache=<FESTID>              l'identifiant que le cache detient reellement
//	festalias=<de>:<vers>[,…]       les chaines a substituer dans la reponse annoncee
//                                  (identifiant, nom de paquet, empreinte de revision…)
//
// Les deux vides — le cas par defaut — et l'alignement ne fait rien : on sert la reponse telle
// qu'elle a ete construite, sans reecriture.

import (
	"log"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// festIDInCache : l'identifiant de fete que le cache BCAT de la console detient. Vide par defaut.
func festIDInCacheCourant() string {
	return soirFlagValeur("festcache")
}

// festRewriteCourant : les substitutions a appliquer a la reponse annoncee, lues du drapeau
// « festalias ». Rend une map vide si le drapeau est absent ou mal forme.
func festRewriteCourant() map[string]string {
	v := soirFlagValeur("festalias")
	if v == "" {
		return nil
	}
	out := map[string]string{}
	for _, couple := range strings.Split(v, ",") {
		de, vers, ok := strings.Cut(couple, ":")
		if !ok || de == "" || vers == "" {
			log.Printf("[NPLN fest] festalias : %q n'est pas de la forme <de>:<vers> — ignore", couple)

			continue
		}
		out[de] = vers
	}

	return out
}

// rewriteStrings walks every string field (including nested messages, lists and map values) and
// applies the substitutions. Values are matched whole: the fest id also appears inside resource
// paths like "tenants/<t>/festSchedules/JUEA-00106", so those are handled by substring rewrite.
func rewriteStrings(m protoreflect.Message, subs map[string]string, n *int) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					rewriteStrings(mv.Message(), subs, n)
					return true
				})
			}
		case fd.IsList():
			list := v.List()
			for i := 0; i < list.Len(); i++ {
				switch fd.Kind() {
				case protoreflect.MessageKind, protoreflect.GroupKind:
					rewriteStrings(list.Get(i).Message(), subs, n)
				case protoreflect.StringKind:
					if s, changed := applySubs(list.Get(i).String(), subs); changed {
						list.Set(i, protoreflect.ValueOfString(s))
						*n++
					}
				}
			}
		case fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind:
			rewriteStrings(v.Message(), subs, n)
		case fd.Kind() == protoreflect.StringKind:
			if s, changed := applySubs(v.String(), subs); changed {
				m.Set(fd, protoreflect.ValueOfString(s))
				*n++
			}
		}
		return true
	})
}

func applySubs(s string, subs map[string]string) (string, bool) {
	out := s
	for from, to := range subs {
		out = replaceAll(out, from, to)
	}
	return out, out != s
}

func replaceAll(s, from, to string) string {
	if from == "" {
		return s
	}
	out := ""
	for {
		i := indexOf(s, from)
		if i < 0 {
			return out + s
		}
		out += s[:i] + to
		s = s[i+len(from):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// alignFestToBcat decodes the captured fest schedule and rewrites it to the fest the console's
// BCAT cache holds, so S3 can actually load the resources it is told to load.
func alignFestToBcat(name string, raw []byte, msg proto.Message) {
	if err := proto.Unmarshal(raw, msg); err != nil {
		log.Printf("[NPLN toyohr] %s unmarshal: %v", name, err)
		return
	}
	subs := festRewriteCourant()
	if len(subs) == 0 {
		return // aucun alias configure : on sert la reponse telle quelle
	}
	nb := 0
	rewriteStrings(msg.ProtoReflect(), subs, &nb)
	log.Printf("[NPLN toyohr] %s -> aligne sur le cache BCAT local (%s), %d champ(s) reecrit(s)",
		name, festIDInCacheCourant(), nb)
}
