package main

// friends — a DYNAMIC nn.npln.friends.v1.Friends service, built from the unified
// Nextendo friend graph (nextendo-account) instead of the frozen capture. Registering
// it takes precedence over the generic replay, so Splatoon 3 shows the player's REAL
// Nextendo friends — the same list as Splatoon 2 / the NEX games.

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	friendspb "npln.nintendo.net/npln-practice/proto/friends/v1"
)

// capturedSubscribeFriendUsers unmarshals the real captured SubscribeFriendUsers response (the
// friend list u-qoahvkaf actually got on console). We serve it as the fallback (unresolved identity
// = the session runs as capturedUser): an EMPTY friends response made S3's plaza resource-path
// parser abort (2162-0001) right after it reached the 2nd wave. Sending the exact captured friend
// structure — semantically what S3 accepted on hardware — lets the plaza render.
func capturedSubscribeFriendUsers() (*friendspb.SubscribeFriendUsersResponse, bool) {
	data, err := capturedBoot.ReadFile("captured_boot/friends.v1.Friends.SubscribeFriendUsers.grpc")
	data = alignCapturedIdentity(data)
	if err != nil || len(data) < 5 {
		return nil, false
	}
	mlen := int(binary.BigEndian.Uint32(data[1:5]))
	if 5+mlen > len(data) {
		return nil, false
	}
	var resp friendspb.SubscribeFriendUsersResponse
	if proto.Unmarshal(data[5:5+mlen], &resp) != nil {
		return nil, false
	}
	return &resp, true
}

// nplnTenant is the Splatoon 3 (1.0.0) tenant path all user resources hang off.
const nplnTenant = "tenants/t-dce9377b-lp1"

// callerPID extracts the caller's Nextendo account PID from the access token that S3
// echoes back in `authorization: bearer nextendo-npln-access.<pid>` (issued by auth).
func callerPID(ctx context.Context) (uint64, bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, a := range md.Get("authorization") {
		a = strings.TrimSpace(a)
		a = strings.TrimPrefix(a, "Bearer ")
		a = strings.TrimPrefix(a, "bearer ")

		// The access token is now the ES256 JWT Nintendo's shape requires (token_jwt.go); we
		// carry the PID in npln.ext_id, so read it back from there.
		if pid, ok := pidFromJWT(a); ok {
			return pid, true
		}

		// Jeton opaque legacy (d'avant la bascule JWT du 2026-07-16) : trivialement forgeable
		// — un simple numéro, sans aucune preuve. On ne l'accepte plus qu'en mode dev explicite ;
		// en prod il est refusé (ces jetons ont de toute façon expiré depuis longtemps, TTL 8 h).
		if allowUnverified() {
			const pfx = "nextendo-npln-access."
			if strings.HasPrefix(a, pfx) {
				seg := strings.SplitN(a[len(pfx):], ".", 2)[0]
				if pid, err := strconv.ParseUint(seg, 10, 64); err == nil {
					return pid, true
				}
			}
		}
	}
	return 0, false
}

// pidFromJWT pulls the PID out of our access token's npln.ext_id claim, APRÈS avoir vérifié la
// signature ES256 du jeton. La vérification est le cœur du modèle de confiance : sans elle,
// n'importe qui forge un jeton portant l'ext_id d'un autre et agit sous son identité sur
// friends/présence (accountFriends(pid) ne prouve que l'EXISTENCE du compte, pas que l'appelant
// EST ce compte). On l'a signé avec notre clé (token_jwt.go) ; on le vérifie avec la même clé.
func pidFromJWT(tok string) (uint64, bool) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "ey") {
		return 0, false
	}
	if !verifyNplnAccessToken(parts[0], parts[1], parts[2]) {
		return 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0, false
	}
	var claims struct {
		Npln struct {
			ExtID string `json:"ext_id"`
		} `json:"npln"`
	}
	if json.Unmarshal(raw, &claims) != nil || claims.Npln.ExtID == "" {
		return 0, false
	}
	pid, err := strconv.ParseUint(claims.Npln.ExtID, 16, 64)
	if err != nil {
		return 0, false
	}
	return pid, true
}

type friendsServer struct {
	friendspb.UnimplementedFriendsServer
}

func (s *friendsServer) ActivateUser(ctx context.Context, req *friendspb.ActivateUserRequest) (*friendspb.ActivateUserResponse, error) {
	log.Printf("[NPLN Friends] ActivateUser %q", req.GetName())
	return &friendspb.ActivateUserResponse{}, nil
}

func (s *friendsServer) ListBlockingUsers(ctx context.Context, req *friendspb.ListBlockingUsersRequest) (*friendspb.ListBlockingUsersResponse, error) {
	log.Printf("[NPLN Friends] ListBlockingUsers parent=%q (returning empty)", req.GetParent())
	return &friendspb.ListBlockingUsersResponse{}, nil
}

// friendUser builds one NPLN FriendUser from a bridge friend (mine = the caller).
func friendUser(myUID string, f nplnFriendData) *friendspb.FriendUser {
	return &friendspb.FriendUser{
		Name:       nplnTenant + "/users/" + myUID + "/friendUsers/" + f.UserID,
		FriendUser: nplnTenant + "/users/" + f.UserID,
		NsaId:      f.AccountHex,
	}
}

func (s *friendsServer) ListFriendUsers(ctx context.Context, req *friendspb.ListFriendUsersRequest) (*friendspb.ListFriendUsersResponse, error) {
	pid, ok := callerPID(ctx)
	if !ok {
		return &friendspb.ListFriendUsersResponse{}, nil
	}
	me, err := accountFriends(pid)
	if err != nil {
		log.Printf("[NPLN Friends] ListFriendUsers pid=%d: %v", pid, err)
		return &friendspb.ListFriendUsersResponse{}, nil
	}
	users := make([]*friendspb.FriendUser, 0, len(me.Friends))
	for _, f := range me.Friends {
		users = append(users, friendUser(me.UserID, f))
	}
	return &friendspb.ListFriendUsersResponse{FriendUsers: users}, nil
}

// SubscribeFriendUsers streams the caller's real Nextendo friend list, then holds the
// stream open (S3 keeps it for the whole session), re-pushing keep-alives. The
// response shape matches the captured SubscribeFriendUsersResponse exactly:
// {FriendAccounts:[{nsa_id, users:[FriendUser]}], KeepAliveInterval}.
func (s *friendsServer) SubscribeFriendUsers(req *friendspb.SubscribeFriendUsersRequest, stream friendspb.Friends_SubscribeFriendUsersServer) error {
	ctx := stream.Context()
	ka := durationpb.New(50 * time.Second)

	pid, ok := callerPID(ctx)
	if !ok {
		log.Printf("[NPLN Friends] SubscribeFriendUsers: NO PID -> captured fallback friend list")
		if resp, cok := capturedSubscribeFriendUsers(); cok {
			resp.KeepAliveInterval = ka
			_ = stream.Send(resp)
		} else {
			_ = stream.Send(&friendspb.SubscribeFriendUsersResponse{KeepAliveInterval: ka})
		}
		<-ctx.Done()
		return nil
	}
	if me, err := accountFriends(pid); err != nil {
		log.Printf("[NPLN Friends] Subscribe pid=%d: %v -> captured fallback friend list", pid, err)
		if resp, cok := capturedSubscribeFriendUsers(); cok {
			resp.KeepAliveInterval = ka
			_ = stream.Send(resp)
		} else {
			_ = stream.Send(&friendspb.SubscribeFriendUsersResponse{KeepAliveInterval: ka})
		}
	} else {
		// FORME DE LA REPONSE, mesuree sur la capture reelle (une capture du hall, 605 octets,
		// 9 amis) : Nintendo ne porte l'objet FriendUser complet que pour DEUX d'entre eux ; les
		// sept autres n'ont que leur cle NSA. Nous le portions pour TOUS — 161 amis, 29 Ko, puis
		// 21 messages une fois decoupes — et le jeu s'arretait juste apres les avoir recus.
		//
		// Le drapeau "friendskeys" reproduit la forme de Nintendo : l'objet complet uniquement pour
		// les amis REELLEMENT en ligne (ceux dont le jeu a besoin tout de suite), la simple cle pour
		// les autres. La reponse retombe a quelques kilo-octets et tient dans UN message, comme chez
		// Nintendo — le decoupage devient inutile.
		cles := soirFlag("friendskeys")
		complets := 0

		// NE JAMAIS TRONQUER LA LISTE — alleger son CONTENU.
		//
		// Mesure du 2026-08-20 sur le compte a 295 amis : en n'envoyant que 40 comptes (objets
		// complets, 7324 octets), le jeu n'affiche AUCUN ami. Il connait ses 295 amis par sa propre
		// sauvegarde ; recevoir une liste amputee la lui fait rejeter en bloc. La troncature est donc
		// a proscrire.
		//
		// En revanche la TAILLE tue la session : 54 Ko -> une seconde en ligne, 6,5 Ko -> trente et
		// une secondes, 1,3 Ko -> stable. On sert donc TOUS les comptes, mais l'objet FriendUser
		// complet seulement pour les « amiscomplets » premiers — les amis EN LIGNE d'abord, ce sont
		// ceux dont le jeu a besoin tout de suite. Les autres n'ont que leur cle NSA, exactement comme
		// dans la capture Nintendo (9 amis, 2 objets complets, 605 octets).
		amis := me.Friends
		if n := amisComplets(); n > 0 && len(amis) > n {
			enLigne := make([]nplnFriendData, 0, n)
			autres := make([]nplnFriendData, 0, len(amis))
			for _, f := range amis {
				if nplnUserOnline(f.UserID) {
					enLigne = append(enLigne, f)
				} else {
					autres = append(autres, f)
				}
			}
			amis = append(enLigne, autres...)
			log.Printf("[NPLN Friends] Subscribe pid=%d : %d ami(s) servis, objet complet pour les %d premiers (%d en ligne en tete)",
				pid, len(amis), n, len(enLigne))
		}

		accounts := make([]*friendspb.SubscribeFriendUsersResponse_FriendAccount, 0, len(amis))
		budget := amisComplets()
		for i, f := range amis {
			compte := &friendspb.SubscribeFriendUsersResponse_FriendAccount{NsaId: f.AccountHex}
			dansLeBudget := budget <= 0 || i < budget
			if dansLeBudget && (!cles || nplnUserOnline(f.UserID)) {
				compte.Users = []*friendspb.FriendUser{friendUser(me.UserID, f)}
				complets++
			}
			accounts = append(accounts, compte)
		}
		if cles {
			log.Printf("[NPLN Friends] Subscribe pid=%d -> %d ami(s), dont %d avec l'objet complet (forme de la capture)",
				pid, len(accounts), complets)
		}
		// Taille du message : la capture Nintendo repond 99 octets a un compte neuf et 605 a un
		// compte etabli. Nous, nous poussons TOUTE la liste Nextendo d'un coup — 146 amis, de l'ordre
		// de 27 Ko. Or l'emulateur lisait justement ~27 Ko juste avant de s'abattre en 2162-0001, et
		// le contexte d'erreur envoye par le jeu nomme SubscribeFriendUsers. On journalise donc la
		// taille reelle, et le drapeau "friendschunk" permet de la decouper en petits messages —
		// legitime pour un flux : le client accumule les messages recus.
		plein := &friendspb.SubscribeFriendUsersResponse{FriendAccounts: accounts, KeepAliveInterval: ka}
		if b, err := proto.Marshal(plein); err == nil {
			log.Printf("[NPLN Friends] Subscribe pid=%d -> %d ami(s), message de %d octets", pid, len(accounts), len(b))
		}

		if n := tailleLotAmis(); n > 0 && !cles && len(accounts) > n {
			for i := 0; i < len(accounts); i += n {
				j := i + n
				if j > len(accounts) {
					j = len(accounts)
				}
				lot := &friendspb.SubscribeFriendUsersResponse{FriendAccounts: accounts[i:j]}
				if i == 0 {
					lot.KeepAliveInterval = ka
				}
				if err := stream.Send(lot); err != nil {
					return err
				}
			}
			log.Printf("[NPLN Friends] Subscribe pid=%d -> envoye en %d lots de %d",
				pid, (len(accounts)+n-1)/n, n)
		} else {
			_ = stream.Send(plein)
		}
		keepalive := time.NewTicker(nplnStreamHeartbeat)
		defer keepalive.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-keepalive.C:
				if err := stream.Send(&friendspb.SubscribeFriendUsersResponse{KeepAliveInterval: ka}); err != nil {
					return nil
				}
			}
		}
	}

	// Hold open with periodic keep-alives (S3 treats a close as "lost friends").
	//
	// [Nextendo 2026-08-25] BATTRE LARGEMENT A L'INTERIEUR DE L'INTERVALLE ANNONCE.
	//
	// On annonce 50 s au client (ka ci-dessus) et on battait toutes les 45 s : cinq secondes de
	// marge, alors que ce flux porte le plus gros message du serveur — 57 100 octets pour un compte
	// a 312 amis, contre moins de 9 000 pour tous les autres. Le moindre retard sur cet envoi
	// depassait le delai annonce, et le jeu declarait le flux perdu : erreur de communication chez
	// un joueur SEUL dans le square, sans le moindre P2P en cause.
	//
	// Les trois autres abonnements longs battent a nplnStreamHeartbeat (20 s). Celui-ci etait le
	// seul a frôler sa propre echeance ; il s'aligne.
	t := time.NewTicker(nplnStreamHeartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := stream.Send(&friendspb.SubscribeFriendUsersResponse{KeepAliveInterval: ka}); err != nil {
				return nil
			}
		}
	}
}

// tailleLotAmis rend le nombre d'amis par message quand le decoupage est demande, 0 sinon.
// Le drapeau "friendschunk" vaut un lot de 8 : assez petit pour rester loin des tailles de la
// capture (99 et 605 octets), assez grand pour ne pas multiplier les messages a l'infini.
// amisComplets rend le nombre d'amis qui recoivent l'objet FriendUser COMPLET (0 = tous).
// Les suivants sont servis quand meme, mais avec leur seule cle NSA : la liste reste entiere.
func amisComplets() int {
	if v := soirFlagValeur("amiscomplets"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

func tailleLotAmis() int {
	if soirFlag("friendschunk") {
		return 8
	}
	return 0
}
