package main

// presence — nn.npln.friends.v1.PresenceService, servi DYNAMIQUEMENT.
//
// Jusqu'ici ce service tombait dans le rejeu générique : on renvoyait les 11 messages capturés,
// qui décrivent la présence des amis du compte de CAPTURE (u-exemple8000000000000, u-qoahvkaf…).
// Aucun de ces identifiants ne correspond aux vrais amis Nextendo du joueur, donc S3 ne pouvait
// associer aucune présence à sa liste d'amis — et deux joueurs Nextendo ne pouvaient
// structurellement pas se voir « en train de livrer bataille ».
//
// La forme servie ici est calquée sur la capture, décodée champ par champ
// (captured_boot/friends.v1.PresenceService.SubscribePresences.grpc) :
//
//	msg 0 : Heartbeat{ interval: 30s, (champ 2) 50s }
//	msg 1 : Presences{ presences: [ {name: "<tenant>/users/<uid>/presence", state: OFFLINE}, … ],
//	                   resume_token: "<base64>" }
//	msg 2 : PresenceEnumerationDone{}
//	msg 3+: Heartbeat, répété
//
// À noter : dans la capture, TOUS les amis sont OFFLINE (state=2). Une liste « Amis » vide quand
// personne d'autre n'est en jeu est donc le rendu correct, pas une panne.

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "npln.nintendo.net/npln-practice/proto/common"
	friendspb "npln.nintendo.net/npln-practice/proto/friends/v1"
)

type presenceServer struct {
	friendspb.UnimplementedPresenceServiceServer
}

// presenceHeartbeatExtra est le champ 2 du Heartbeat capturé : {2: {1: 50}} = la DEADLINE que le
// client applique à son ping (l'intervalle, lui, est le champ 1 = 30 s). Notre .proto ne déclare
// que `interval`, donc on réémet ce champ tel quel en champ inconnu plutôt que de le perdre : un
// heartbeat amputé changerait le contrat que le client a mesuré chez Nintendo.
var presenceHeartbeatExtra = protoreflect.RawFields([]byte{0x12, 0x02, 0x08, 0x32})

// presenceKeepAliveInterval est la cadence a laquelle NOUS repondons sur le flux KeepAlive. Elle
// vaut l'intervalle que le Heartbeat annonce lui-meme, et c'est celle de la capture : 11 reponses
// en 293 s sur le compte vierge, soit 29,3 s.
const presenceKeepAliveInterval = 30 * time.Second

func presenceHeartbeat() *friendspb.Heartbeat {
	hb := &friendspb.Heartbeat{Interval: durationpb.New(30 * time.Second)}
	hb.ProtoReflect().SetUnknown(presenceHeartbeatExtra)
	return hb
}

// SubscribePresences streams the caller's REAL friends' presence, then holds the stream open.
func (p *presenceServer) SubscribePresences(req *friendspb.SubscribePresencesRequest, stream friendspb.PresenceService_SubscribePresencesServer) error {
	ctx := stream.Context()

	// Heartbeat d'ouverture, exactement comme la capture (le client cale son ping dessus).
	if err := stream.Send(&friendspb.SubscribePresencesResponse{
		Response: &friendspb.SubscribePresencesResponse_Heartbeat{Heartbeat: presenceHeartbeat()},
	}); err != nil {
		return nil
	}

	presences := p.presencesFor(ctx)
	// Le client peut demander un SOUS-ENSEMBLE précis (champ `presences`). Dans ce cas on ne
	// renvoie que celui-là : envoyer toute la liste à une souscription ciblée fait diverger le
	// curseur que le client tient de son côté.
	if want := req.GetPresences(); len(want) > 0 {
		keep := make(map[string]bool, len(want))
		for _, w := range want {
			keep[w] = true
		}
		filtered := presences[:0]
		for _, pr := range presences {
			if keep[pr.GetName()] {
				filtered = append(filtered, pr)
			}
		}
		presences = filtered
	}
	log.Printf("[NPLN Presence] SubscribePresences user=%q demandes=%d -> %d presence(s)",
		short(req.GetUser()), len(req.GetPresences()), len(presences))

	// Une liste VIDE ne se transmet pas du tout. Le compte vierge enchaine battement puis
	// enumeration_done, sans un seul cadre de champ 1 :
	// une capture de compte neuf,
	// 172 o = [1a 08 0a02081e 12020832] [12 00] puis dix battements. Un message `presences` vide,
	// avec un resume_token vide, n'existe nulle part dans le corpus.
	if len(presences) > 0 {
		if err := stream.Send(&friendspb.SubscribePresencesResponse{
			Response: &friendspb.SubscribePresencesResponse_Presences_{
				Presences: &friendspb.SubscribePresencesResponse_Presences{
					Presences:   presences,
					ResumeToken: presenceResumeToken(presences),
				},
			},
		}); err != nil {
			return nil
		}
	}

	// « J'ai fini d'énumérer » : sans ce message le client attend indéfiniment la suite de la liste.
	if err := stream.Send(&friendspb.SubscribePresencesResponse{
		Response: &friendspb.SubscribePresencesResponse_EnumerationDone{
			EnumerationDone: &friendspb.SubscribePresencesResponse_PresenceEnumerationDone{},
		},
	}); err != nil {
		return nil
	}

	// ETAT DEJA ANNONCE, pour ne pousser que ce qui CHANGE.
	connu := make(map[string]string, len(presences))
	for _, pr := range presences {
		connu[pr.GetName()] = empreintePresence(pr)
	}

	t := time.NewTicker(nplnStreamHeartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// SUIVRE LES CHANGEMENTS D'ETAT.
			//
			// Ce flux n'envoyait la liste qu'UNE FOIS, a l'ouverture, puis ne poussait plus que des
			// battements : l'etat qu'un ami avait a cette seconde-la, le client le gardait pour toute
			// sa session. C'etait un instantane deguise en souscription.
			//
			// Mesure du 2026-08-15 : deux joueurs amis, chacun voyant l'autre differemment. Celui
			// dont le jeu avait plante puis redemarre restait « hors ligne » chez l'autre, qui avait
			// ouvert son flux pendant cette absence ; dans l'autre sens il apparaissait bien en
			// ligne, parce que la souscription avait ete ouverte alors qu'il etait deja la. Deux
			// verites opposees pour une meme paire d'amis, et aucune ne se corrigeait jamais.
			tous := p.presencesFor(ctx)
			var change []*friendspb.Presence
			for _, pr := range tous {
				if e := empreintePresence(pr); connu[pr.GetName()] != e {
					connu[pr.GetName()] = e
					change = append(change, pr)
				}
			}
			if len(change) > 0 {
				log.Printf("[NPLN Presence] %d changement(s) d'etat sur %d ami(s) -> pousse sur le flux ouvert",
					len(change), len(tous))
				// ⚠️ LE JETON DE REPRISE DECRIT L'ENSEMBLE ENUMERE, PAS LE DELTA.
				//
				// Premiere version de ce suivi (2026-08-15) : le jeton etait calcule sur les seuls
				// changements. Chez un joueur a 240 amis l'ecran des amis rendait aussitot une erreur
				// de communication et la liste disparaissait, tandis qu'un joueur a 1 ami ne voyait
				// rien — son delta etait toujours vide. Le client recoupe ce jeton avec l'ensemble
				// qu'il tient de son cote ; un jeton qui ne decrit qu'une partie le fait diverger,
				// exactement comme le note deja le traitement des souscriptions ciblees plus haut.
				if err := stream.Send(&friendspb.SubscribePresencesResponse{
					Response: &friendspb.SubscribePresencesResponse_Presences_{
						Presences: &friendspb.SubscribePresencesResponse_Presences{
							Presences:   change,
							ResumeToken: presenceResumeToken(tous),
						},
					},
				}); err != nil {
					return nil
				}
			}

			if err := stream.Send(&friendspb.SubscribePresencesResponse{
				Response: &friendspb.SubscribePresencesResponse_Heartbeat{Heartbeat: presenceHeartbeat()},
			}); err != nil {
				return nil
			}
		}
	}
}

// presencesFor builds one Presence per real Nextendo friend. A friend counts as ONLINE only when
// they are actually talking to THIS server right now — the same liveness the monitoring uses. We
// never claim a friend is online on a guess: a wrong ONLINE sends the player into a join attempt
// that cannot succeed.
// attributsParUid retient ce que CHAQUE joueur publie sur son propre KeepAlive, pour le
// redistribuer a ses amis.
//
// Mesure du 2026-08-15, capture d'une vraie Switch chez Nintendo (SubscribePresences) : la presence
// d'un ami porte TREIZE attributs, et deux d'entre eux commandent l'affichage de la liste d'amis.
//
//	juste en ligne : GameStatus=1  SessionId=""   MaxParticipants=0   CurrentParticipants=0
//	salon ouvert   : GameStatus=2  SessionId=<uuid de la partie>  Max=10  Current=1
//	un invite entre :                                             Max=10  Current=2
//	il ressort     : GameStatus=1
//
// C'est le passage de GameStatus 1 a 2, avec un SessionId non vide, qui fait apparaitre
// « Rejoindre » — et ce SessionId est exactement l'uuid que le jeu passe ensuite a JoinGameSession.
// Les autres attributs (PlayerName, GameMode, Udemae, UsePassword…) remplissent la ligne.
var attributsParUid = struct {
	sync.Mutex
	m map[string]map[string]*commonpb.Value
}{m: map[string]map[string]*commonpb.Value{}}

// retenirAttributsPresence FUSIONNE la mise a jour dans ce qu'on sait deja du joueur.
//
// ⚠️ Le client envoie des mises a jour PARTIELLES : il ne renvoie que ce qui change, et le message
// porte d'ailleurs un update_mask. Mesure du 2026-08-15, un joueur ouvrant un salon prive :
//
//	13:31:11  13 attributs — GameStatus=1  SessionId=""
//	13:31:32   4 attributs — GameStatus=2  SessionId="9a7f7c6f-…"   <- le salon s'ouvre
//	13:31:34   1 attribut  — CurrentParticipants=1
//
// En remplacant la carte entiere a chaque fois, la mise a jour d'un seul attribut effacait
// GameStatus et SessionId deux secondes apres l'ouverture du salon : ses amis ne le voyaient donc
// jamais « en partie privee », seulement « en ligne ». On fusionne.
func retenirAttributsPresence(uid string, attrs map[string]*commonpb.Value) {
	if len(attrs) == 0 {
		return
	}
	attributsParUid.Lock()
	defer attributsParUid.Unlock()
	courant := attributsParUid.m[uid]
	if courant == nil {
		courant = map[string]*commonpb.Value{}
		attributsParUid.m[uid] = courant
	}
	for k, v := range attrs {
		courant[k] = v
	}
}

// attributsPresenceDe rend une COPIE : la carte retenue est mutee par les mises a jour partielles,
// la partager telle quelle exposerait le message en cours d'envoi a une modification concurrente.
func attributsPresenceDe(uid string) map[string]*commonpb.Value {
	attributsParUid.Lock()
	defer attributsParUid.Unlock()
	src := attributsParUid.m[uid]
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]*commonpb.Value, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// empreintePresence resume l'etat AFFICHABLE d'un ami : sa disponibilite et son salon.
//
// La detection de changement ne regardait que l'etat en ligne. Or un ami deja en ligne qui OUVRE un
// salon ne change pas d'etat — seuls ses attributs bougent. Sans les prendre en compte, la bascule
// vers « Rejoindre » n'etait jamais poussee sur le flux deja ouvert.
func empreintePresence(pr *friendspb.Presence) string {
	a := pr.GetAttributes()
	return fmt.Sprintf("%d|%d|%s|%d|%d",
		pr.GetState(),
		a["GameStatus"].GetIntegerValue(),
		a["SessionId"].GetStringValue(),
		a["CurrentParticipants"].GetIntegerValue(),
		a["MaxParticipants"].GetIntegerValue())
}

func (p *presenceServer) presencesFor(ctx context.Context) []*friendspb.Presence {
	pid, ok := callerPID(ctx)
	if !ok {
		return nil
	}
	me, err := accountFriends(pid)
	if err != nil {
		log.Printf("[NPLN Presence] pid=%d: %v", pid, err)
		return nil
	}
	// ⚠️ N'enumerer QUE les amis que SubscribeFriendUsers vient de DECLARER avec l'objet FriendUser
	// complet — meme drapeau, meme predicat que friends.go.
	//
	// LE CORPUS ENONCE CETTE EGALITE DEUX FOIS, SANS EXCEPTION :
	//
	//	compte VIERGE   SubscribeFriendUsers  99 o, ZERO friend_account
	//	                SubscribePresences   172 o, AUCUN message `presences`
	//	compte ETABLI   SubscribeFriendUsers 605 o, 9 comptes dont DEUX avec l'objet complet
	//	                SubscribePresences   413 o, exactement CES DEUX-LA
	//	                Locker.SelectDocuments (requete) filtre UIDs = exactement CES DEUX-LA
	//
	// La troisieme ligne est la preuve que le client RELIT cet ensemble : il le reinjecte dans sa
	// requete Locker. Nous declarions 0 ami sur 181, puis nous annoncions 181 presences — 181
	// utilisateurs dont le client n'avait jamais entendu parler, juste avant l'appel Locker qui
	// precede CreateSaveRecord de 4,67 s dans la reference.
	//
	// A noter contre une lecture tentante : dans la capture du compte etabli, ces deux presences
	// portent state=2, soit OFFLINE. Le critere de Nintendo n'est donc PAS « en ligne » — c'est
	// l'appartenance a l'ensemble declare. On ne reproduit que ce que le corpus prouve.
	cles := soirFlag("friendskeys")

	out := make([]*friendspb.Presence, 0, len(me.Friends))
	ignores := 0
	for _, f := range me.Friends {
		enLigne := nplnUserOnline(f.UserID)
		if cles && !enLigne {
			ignores++
			continue // jamais declare a ce client : on ne peut pas lui annoncer sa presence
		}
		state := friendspb.State_OFFLINE
		if enLigne {
			state = friendspb.State_ONLINE
		}
		out = append(out, &friendspb.Presence{
			Name:       nplnTenant + "/users/" + f.UserID + "/presence",
			State:      state,
			Attributes: attributsPresenceDe(f.UserID),
		})
	}
	if ignores > 0 {
		log.Printf("[NPLN Presence] pid=%d: %d ami(s) non declare(s) a SubscribeFriendUsers -> non enumere(s)", pid, ignores)
	}

	return out
}

// presenceResumeToken mirrors the capture's token: a base64 blob listing the enumerated users, so
// a resumed subscription starts where this one stopped. The client echoes it back verbatim.
func presenceResumeToken(ps []*friendspb.Presence) string {
	var raw []byte
	for _, pr := range ps {
		uid := userIDFromPath(trimPresenceSuffix(pr.GetName()))
		if uid == "" {
			continue
		}
		entry := append([]byte{0x0a, byte(len(uid))}, uid...)
		entry = append(entry, 0x12, 0x00)
		raw = append(raw, 0x0a, byte(len(entry)))
		raw = append(raw, entry...)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func trimPresenceSuffix(name string) string {
	const suffix = "/presence"
	if len(name) > len(suffix) && name[len(name)-len(suffix):] == suffix {
		return name[:len(name)-len(suffix)]
	}
	return name
}

func (p *presenceServer) KeepAlive(stream friendspb.PresenceService_KeepAliveServer) error {
	ctx := stream.Context()
	uid := uidFromCtx(ctx)
	pid := pidDeLAppelant(ctx)
	if pid == 0 && uid != "" {
		log.Printf("[NPLN Presence] KeepAlive de %s SANS identite : ni jeton exploitable, ni appariement connu", uid)
	}
	return servePresenceHeartbeat(ctx, presenceKeepAliveInterval, func() error {
		req := &friendspb.KeepAliveRequest{}
		if err := stream.RecvMsg(req); err != nil {
			return err
		}
		if uid != "" {
			dashTouch(uid, pid, "")
		}
		if up := req.GetUpdatePresence(); up != nil && uid != "" {
			attrs := up.GetPresence().GetAttributes()
			retenirAttributsPresence(uid, attrs)
			log.Printf("[NPLN Presence] UpdatePresence de %s : %d attribut(s) — GameStatus=%v SessionId=%q Current=%v Max=%v",
				uid, len(attrs),
				attrs["GameStatus"].GetIntegerValue(),
				attrs["SessionId"].GetStringValue(),
				attrs["CurrentParticipants"].GetIntegerValue(),
				attrs["MaxParticipants"].GetIntegerValue())

			if inv := inventaireDesAttributs(attrs); attributsOntChange(uid, inv) {
				log.Printf("[NPLN Presence] %s publie : %s", uid, inv)
			}
		}
		return nil
	}, func() error {
		return stream.Send(&friendspb.KeepAliveResponse{Heartbeat: presenceHeartbeat()})
	})
}

func servePresenceHeartbeat(ctx context.Context, interval time.Duration, receive func() error, send func() error) error {
	if err := send(); err != nil {
		return err
	}
	finished := make(chan error, 1)
	go func() {
		for {
			if err := receive(); err != nil {
				finished <- err
				return
			}
			if err := ctx.Err(); err != nil {
				finished <- err
				return
			}
		}
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-finished:
			if err == io.EOF {
				return nil
			}
			return err
		case <-ticker.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := send(); err != nil {
				return err
			}
		}
	}
}
