package main

// rotation_generee — la rotation des stages, ENGENDREE plutot que rejouee.
//
// POURQUOI ON PEUT. Pour Splatoon 3, c'est le SERVEUR qui decide du calendrier : le jeu affiche ce
// qu'on lui envoie, il ne le recalcule pas. Contrairement a Splatoon 2, ou l'algorithme vit dans la
// cartouche et ou il a fallu reverser le generateur pseudo-aleatoire, nous sommes ici la source de
// verite. Il n'y a donc rien a retro-concevoir : la structure d'une phase est faite d'ENTIERS.
//
//	schedules { start_time, end_time (2 h), regular_settings{stages x2},
//	            bankara_settings{rule, stages x2} x2, x_settings{rule, stages x2},
//	            league_settings{rule, stages x2}, schedule_set_id }
//
// POURQUOI IL LE FAUT. La capture ne couvre qu'UNE journee (douze creneaux). La rejouer en la
// decalant condamne a choisir entre deux maux, mesures le 2026-08-18 :
//   - poser la phase 1 sur maintenant  -> horizon complet (19 horodatages devant) mais rotation GELEE
//   - avancer la position              -> rotation qui avance mais horizon reduit a 7, et le jeu
//                                         redemande le calendrier en boucle jusqu'a l'erreur
// Les deux se disputent la meme journee. Engendrer les phases dissout le conflit : on produit
// autant de futur qu'on veut, et la phase courante avance avec l'heure reelle.
//
// DETERMINISME — la propriete qui compte. Un meme creneau doit TOUJOURS rendre les memes stages,
// quel que soit le moment de la requete : deux consoles qui demandent le calendrier a une seconde
// d'intervalle doivent voir la meme rotation, et un joueur qui relit ne doit pas voir le futur
// changer sous lui. Le tirage est donc fonction du seul numero de creneau, sans etat ni horloge.

import (
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// stagesConnus : les identifiants releves dans la capture Nintendo. On ne sort PAS de cette liste —
// un identifiant que le jeu ne connait pas est un risque gratuit.
var stagesConnus = []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26}

// reglesClassees : les quatre regles des modes classes, telles qu'observees dans la capture.
var reglesClassees = []int32{1, 2, 3, 4}

// phasesEngendrees : combien de creneaux on publie. La capture Nintendo en portait douze (24 h) ;
// on en produit autant, ce qui garantit un horizon au moins egal a celui qui fonctionne.
const phasesEngendrees = 12

// creneauDe rend le numero de creneau de deux heures d'un instant, compte depuis l'epoque Unix.
// C'est la seule entree du tirage : meme creneau, memes stages, pour toujours.
func creneauDe(t time.Time) int64 { return t.Unix() / int64(rotationPeriod/time.Second) }

// melange rend une valeur pseudo-aleatoire stable a partir d'un creneau et d'un sel. Fonction de
// hachage entiere (splitmix64) : pas d'etat, pas d'horloge, reproductible partout.
func melange(creneau int64, sel uint64) uint64 {
	x := uint64(creneau)*0x9E3779B97F4A7C15 + sel*0xBF58476D1CE4E5B9
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return x
}

// melangeStages rend les stages connus dans un ordre stable propre a ce creneau.
//
// ⚠️ POURQUOI UNE FENETRE GLISSANTE ET PAS UN SIMPLE MELANGE. Un creneau consomme dix stages sur
// vingt-cinq. Avec un melange independant par creneau, deux creneaux voisins repiochent dans tout
// le pool et se recouvrent a 40 % : le meme stage revient sans arret, ce qui est exactement le
// defaut signale. On fait donc glisser une FENETRE de dix dans une permutation : deux creneaux
// consecutifs prennent des tranches disjointes, donc aucun stage ne revient d'un creneau au suivant.
//
// La permutation change tous les cinq creneaux (dix heures) pour que la rotation ne se repete pas
// en boucle courte. Seule la transition entre deux permutations peut laisser passer une repetition,
// soit un creneau sur cinq — le test TestRotationRepetitionConsecutiveRare mesure ce qu'il en reste.
//
// La fonction ne depend QUE du numero de creneau : c'est ce qui permet au site d'annoncer la
// rotation a l'avance et d'afficher le passe, sans rien stocker.
func melangeStages(creneau int64) []int32 {
	groupe := creneau / 5
	if creneau < 0 && creneau%5 != 0 {
		groupe-- // division entiere vers zero : garder des groupes contigus avant l'epoque
	}

	perm := append([]int32(nil), stagesConnus...)
	for i := len(perm) - 1; i > 0; i-- {
		j := int(melange(groupe, uint64(i)+0x57A6) % uint64(i+1))
		perm[i], perm[j] = perm[j], perm[i]
	}

	rang := creneau % 5
	if rang < 0 {
		rang += 5
	}
	decalage := int(10*rang) % len(perm)

	out := make([]int32, 0, len(perm))
	for i := range perm {
		out = append(out, perm[(decalage+i)%len(perm)])
	}

	return out
}

// tireStages choisit deux stages pour un mode, en evitant tout ce qui est deja `exclus` — les
// stages deja attribues aux AUTRES modes du meme creneau, et ceux du creneau PRECEDENT.
//
// POURQUOI CES DEUX CONTRAINTES. Le vrai jeu ne montre pas le meme stage dans deux modes a la meme
// heure, ni deux creneaux d'affilee. Sans elles, un tirage independant par mode le fait
// regulierement, et la rotation « sent » l'aleatoire au lieu de sentir Nintendo.
//
// Le pool compte 25 stages pour 10 tirages par creneau plus 10 exclusions du creneau precedent :
// il reste toujours de quoi choisir. Le repli (ignorer les exclusions) n'existe que pour ne jamais
// rendre une liste incomplete si un jour le pool retrecit.
func tireStages(creneau int64, ordre []int32, exclus map[int32]bool) []int32 {
	var out []int32

	for _, v := range ordre {
		if len(out) == 2 {
			break
		}
		if !exclus[v] {
			out = append(out, v)
			exclus[v] = true
		}
	}

	for _, v := range ordre { // repli : pool trop etroit, on relache les exclusions
		if len(out) == 2 {
			break
		}
		deja := false
		for _, x := range out {
			if x == v {
				deja = true
			}
		}
		if !deja {
			out = append(out, v)
		}
	}

	return out
}

// reglesDuCreneau distribue les quatre regles classees entre les quatre modes qui en portent une,
// en permutation deterministe. Chaque mode a donc une regle DIFFERENTE des trois autres au meme
// creneau, et la permutation change a chaque creneau.
func reglesDuCreneau(creneau int64) []int32 {
	out := append([]int32(nil), reglesClassees...)

	for i := len(out) - 1; i > 0; i-- {
		j := int(melange(creneau, uint64(i)+0x9C1E) % uint64(i+1))
		out[i], out[j] = out[j], out[i]
	}

	return out
}

// identifiantDePhase fabrique l'identifiant de ressource d'un creneau, a la forme observee dans la
// capture : vingt caracteres alphanumeriques minuscules.
//
// ⚠️ CHAQUE PHASE DOIT AVOIR SON PROPRE NOM. La capture en porte douze distincts ; ma premiere
// version reprenait le nom du gabarit pour les douze, et le jeu ne voyait donc qu'UNE ressource
// repetee — erreur de communication a la place de la rotation.
func identifiantDePhase(creneau int64) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

	out := make([]byte, 20)
	h := melange(creneau, 0x7E57)
	for i := range out {
		h = melange(int64(h), uint64(i)+1)
		out[i] = alphabet[h%uint64(len(alphabet))]
	}

	return string(out)
}

// rotationEngendree construit le jeu de calendriers VS autour de maintenant.
//
// On demarre un creneau AVANT le creneau courant : le jeu doit trouver une phase deja commencee
// (celle qui tourne), sinon il n'a pas de rotation courante et repart en boucle.
func rotationEngendree(maintenant time.Time, modele *toyohrpb.SelectVsSchedulesResponse) *toyohrpb.SelectVsSchedulesResponse {
	courant := creneauDe(maintenant)

	out := &toyohrpb.SelectVsSchedulesResponse{Unk: modele.GetUnk(), Etag: modele.GetEtag()}

	// Reprendre le gabarit d'une phase capturee : on ne change que ce qu'on maitrise (horaires,
	// stages, regles) et on laisse le reste tel que Nintendo l'envoie.
	var gabarit *toyohrpb.VsSchedule
	if s := modele.GetSchedules(); len(s) > 0 {
		gabarit = s[0]
	}

	for i := int64(-1); i < int64(phasesEngendrees)-1; i++ {
		c := courant + i
		debut := time.Unix(c*int64(rotationPeriod/time.Second), 0).UTC()

		nom := gabarit.GetName()
		if i := strings.LastIndexByte(nom, '/'); i >= 0 {
			nom = nom[:i+1] + identifiantDePhase(c)
		}

		// Exclure d'emblee les stages du creneau PRECEDENT : aucun stage ne revient deux fois de
		// suite, comme dans le vrai jeu.
		exclus := map[int32]bool{}
		for _, v := range stagesDuCreneau(c - 1) {
			exclus[v] = true
		}

		ordre := melangeStages(c)
		regles := reglesDuCreneau(c)

		ph := &toyohrpb.VsSchedule{
			Name:            nom,
			StartTime:       timestamppb.New(debut),
			EndTime:         timestamppb.New(debut.Add(rotationPeriod)),
			RegularSettings: &toyohrpb.RegularSettings{Stages: tireStages(c, ordre, exclus)},
			BankaraSettings: []*toyohrpb.BankaraSettings{
				{Rule: regles[0], Stages: tireStages(c, ordre, exclus)},
				{Rule: regles[1], Stages: tireStages(c, ordre, exclus)},
			},
			XSettings:      &toyohrpb.XSettings{Rule: regles[2], Stages: tireStages(c, ordre, exclus)},
			LeagueSettings: &toyohrpb.LeagueSettings{Rule: regles[3], Stages: tireStages(c, ordre, exclus)},
			ScheduleSetId:  gabarit.GetScheduleSetId(),
		}
		if g := gabarit.GetLeagueSettings(); g != nil {
			ph.LeagueSettings.Unk1 = g.GetUnk1()
			ph.LeagueSettings.Unk4 = g.GetUnk4()
			ph.LeagueSettings.Unk5 = g.GetUnk5()
		}

		out.Schedules = append(out.Schedules, ph)
	}

	return out
}

// stagesDuCreneau rend les dix stages attribues a un creneau. Sert a exclure le creneau precedent
// sans reconstruire toute la phase — et se calcule de la meme facon, donc reste coherent.
func stagesDuCreneau(creneau int64) []int32 {
	ordre := melangeStages(creneau)
	exclus := map[int32]bool{}

	var out []int32
	for i := 0; i < 5; i++ {
		out = append(out, tireStages(creneau, ordre, exclus)...)
	}

	return out
}
