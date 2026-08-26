package main

// ugcstore_absent — repondre a GetDocument comme le VRAI serveur repond pour un document qui
// n'existe pas encore.
//
// ⚠️ CE QUI SUIT A ETE CORRIGE LE 2026-08-24. La capture d'un vrai festival sur console
// (le relais de session, flux 55) donne le
// TRAILER, que Proxide ne transcrivait pas : grpc-status = 5, un grpc-message circonstancie et un
// grpc-status-details-bin. Ce n'est donc PAS un succes vide, c'est une erreur riche. Voir
// ugcstore_erreur_absente.go, qui la reproduit — et qui a fait tomber la boucle de
// WeaponPowerMeasurement en une fois.
//
// LA MESURE D'ORIGINE, qui a induit en erreur pendant des semaines
// Sur les 44 GetDocument de la capture (le corpus de captures NPLN
// session-2026-08-08_000317.bin.json), Proxide affiche TOUJOURS "Succeeded" — mais il ne montre
// aucun trailer, donc ce mot ne venait pas du fil. Les documents absents reviennent a
// `content-length: 0` :
//
//	Hammer/PrivateMatch/Data              Succeeded  0 octet  (x6)
//	Hammer/TournamentMatch/Data           Succeeded  0 octet  (x6)
//	Hammer/CoopMatch/Data                 Succeeded  0 octet  (x6)
//	GameRecord/WeaponPowerMeasurement/16  Succeeded  0 octet  (x7)
//	GameRecord/PointCardRegular/<date>    Succeeded  0 puis 509 octets
//
// POURQUOI LA PREMIERE TENTATIVE A PLANTE (et ce qui manquait)
// Ne rien emettre puis rendre nil ne suffit pas : grpc-go, quand aucun en-tete n'a ete envoye,
// replie toute la reponse dans un UNIQUE cadre HEADERS (« Trailers-Only ») ou grpc-status voisine
// avec content-type. Nintendo, lui, envoie DEUX cadres — HEADERS (content-type: application/grpc,
// npln-grpc-type: Unary, content-length: 0) puis TRAILERS avec grpc-status. Mesure : capture reelle
// de la console contre Nintendo, une capture du hall, 43 echanges tous « istio-envoy », dont
// huit reponses a zero message (7 GetDocument + UserScreening/GetViolation) qui portent TOUTES ces
// en-tetes. Forcer SendHeader avant de sortir reproduit la forme exacte du vrai serveur.
//
// L'ERREUR A NE PAS REFAIRE
// `content-length: 0` veut dire ZERO OCTET DE CORPS, donc AUCUN message gRPC. Renvoyer un
// Document VIDE depuis un handler unaire n'est PAS la meme chose : gRPC l'emet quand meme comme
// un message de longueur nulle, precede de ses 5 octets de cadre. Essaye : S3 le lit, y cherche
// ses champs et deref un pointeur nul — crash du thread nn.npln.Worker a nnSdk:0x17574c, juste
// apres notre reponse sur PointCardRegular. C'est le meme piege que celui documente en tete de
// ugcstore.go pour Mario Party Jamboree, pris par l'autre bout.
//
// COMMENT ON EMET ZERO MESSAGE
// Un handler unaire DOIT rendre un message. On declare donc GetDocument en FLUX dans une
// description de service fabriquee a la main : un handler de flux peut se terminer en OK sans
// jamais appeler SendMsg. Cote fil, un client unaire envoie un message puis ferme sa moitie ; le
// serveur peut legalement n'en renvoyer aucun. C'est exactement ce que fait Nintendo, donc le
// client de S3 sait le lire.
//
// GATE PAR LOCATAIRE : Mario Party Jamboree FONCTIONNE avec le NotFound (il cree un profil neuf
// et poursuit, cf. l'en-tete de ugcstore.go). Lui garde une erreur, S3 seul recoit le succes vide.

import (
	"log"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ugcpb "npln.nintendo.net/npln-practice/proto/ugcstore/v1"
)

// registerUgcstore enregistre le service avec GetDocument bascule en flux. Toutes les autres
// methodes gardent la description generee : on recopie la ServiceDesc et on deplace la seule
// entree qui nous interesse.
func registerUgcstore(s *grpc.Server, impl *ugcstoreServer) {
	desc := ugcpb.Ugcstore_ServiceDesc // copie

	methods := make([]grpc.MethodDesc, 0, len(desc.Methods))
	for _, m := range desc.Methods {
		if m.MethodName == "GetDocument" {
			continue // repris ci-dessous en flux
		}
		methods = append(methods, m)
	}
	desc.Methods = methods

	streams := make([]grpc.StreamDesc, 0, len(desc.Streams)+1)
	streams = append(streams, desc.Streams...)
	streams = append(streams, grpc.StreamDesc{
		StreamName:    "GetDocument",
		Handler:       getDocumentStreamHandler,
		ServerStreams: true, // indispensable pour pouvoir ne rien emettre
	})
	desc.Streams = streams

	s.RegisterService(&desc, impl)
}

// getDocumentStreamHandler lit la requete unaire du client, puis :
//   - document connu  -> l'emet, comme avant ;
//   - document absent -> ne renvoie RIEN et termine en OK (S3), ou NotFound (autres locataires).
func getDocumentStreamHandler(srv any, stream grpc.ServerStream) error {
	impl, ok := srv.(*ugcstoreServer)
	if !ok {
		return status.Error(codes.Internal, "mauvais type de service")
	}

	req := new(ugcpb.GetDocumentRequest)
	if err := stream.RecvMsg(req); err != nil {
		return err
	}

	ctx := stream.Context()
	doc, err := impl.GetDocument(ctx, req)
	if err == nil {
		return stream.SendMsg(doc)
	}

	// L'implementation signale l'absence par NotFound. Pour S3, c'est un SUCCES VIDE qu'il faut :
	// on sort sans emettre. Pour les autres locataires, on laisse l'erreur passer telle quelle.
	if status.Code(err) == codes.NotFound && isS3Tenant(tenantFromCtx(ctx)) && soirFlag("getdoc") {
		// ⚠️ Le « succes sans message » n'est PROUVE par la capture que pour les documents listes en
		// tete de ce fichier. Pour les autres, il tue le jeu : mesure du 2026-08-13, abort 2162-0001
		// sur le fil nn.npln.Worker a Thunder.nss:0x667fe8 — l'analyseur de nom de ressource, qui
		// exige « tenants/<t>/documents/<rest> » et s'arrete sur un nom VIDE. Or sans message, le
		// client construit un Document par defaut, dont le nom est justement vide. Le dernier
		// document lu avant l'abort etait GameRecord/BankaraChallenges/<id>, absent de la capture.
		//
		// On rend donc, pour ces documents-la, un Document qui porte SON NOM et rien d'autre :
		// l'analyseur est satisfait, et le jeu lit un contenu vide, ce qui est la verite.
		// Trois formes ont ete essayees pour un document absent, et MESUREES sur ce client :
		//
		//	OK + AUCUN message  -> le SDK construit un Document par defaut, au nom VIDE, et
		//	                       l'analyseur de nom de ressource s'abat : abort 2162-0001 a
		//	                       Thunder.nss:0x667fe8, sur nn.npln.Worker. Vu deux fois, sur deux
		//	                       documents differents : GameRecord/BankaraChallenges/<id> le
		//	                       2026-08-12, puis Hammer/UserAttribute/Data le 2026-08-13 a 15:12:03.
		//	OK + Document nomme -> plus d'abort, mais le hall s'arrete sur « Une erreur de
		//	                       communication est survenue ». Le jeu recoit un document qui existe
		//	                       et qui est vide, la ou il n'en existe aucun.
		//	NOT_FOUND, DEUX cadres -> ce qu'on essaie ici.
		//
		// POURQUOI CETTE TROISIEME FORME. La capture montre « Succeeded, content-length: 0 » sur les
		// quatre documents Hammer du compte vierge (48, 53, 54, 55 : 0 octet chacun) — mais Proxide
		// NE TRANSCRIT AUCUN TRAILER : sur 102 echanges, le champ trailers est vide 102 fois et
		// « grpc-status » n'apparait nulle part. Le statut de ces reponses n'a donc JAMAIS ete
		// mesure ; « Succeeded » vient de l'affichage de Proxide, pas du fil.
		//
		// Le meme piege a coute deux jours sur CloudSave/GetSaveRecord, et la reponse s'est revelee
		// etre exactement celle-ci : NOT_FOUND en DEUX cadres (en-tetes d'abord, statut ensuite). Le
		// client a alors cree son record. L'ancien essai « NotFound » consigne comme elimine partait,
		// lui, en Trailers-Only — un seul cadre, la forme dont on sait qu'elle fait echouer S3.
		//
		// Le drapeau « docnomme » permet de revenir au Document nomme sans redeployer.
		switch forme := formeDAbsence(req.GetName()); forme {
		case "vide":
			log.Printf("[NPLN ugcstore] GetDocument name=%q -> Document nomme sans contenu (docvide)", req.GetName())
			return stream.SendMsg(&ugcpb.Document{Name: req.GetName()})
		case "muet":
			// ⚠️ Les en-tetes D'ABORD. Rendre nil sans les avoir envoyes fait replier grpc-go en un
			// UNIQUE cadre (Trailers-Only), la forme dont on sait qu'elle fait echouer S3. Nintendo
			// en envoie deux : HEADERS avec content-length: 0, puis TRAILERS avec grpc-status. C'est
			// cette confusion qui avait fait eliminer « OK sans message » a tort.
			if err := enteteVideCommeNintendo(stream); err != nil {
				return err
			}
			log.Printf("[NPLN ugcstore] GetDocument name=%q -> OK sans message, DEUX cadres (docmuet)", req.GetName())
			return nil
		}

		if soirFlag("docnomme") {
			log.Printf("[NPLN ugcstore] GetDocument name=%q -> Document nomme sans contenu", req.GetName())
			return stream.SendMsg(&ugcpb.Document{Name: req.GetName()})
		}

		// L'erreur exacte de Nintendo, details compris : voir ugcstore_erreur_absente.go. La forme a
		// deux cadres etait juste depuis longtemps ; c'est le contenu qui manquait.
		if err := enteteVideCommeNintendo(stream); err != nil {
			return err
		}
		log.Printf("[NPLN ugcstore] GetDocument name=%q -> NotFound comme Nintendo (details NError)",
			req.GetName())
		return erreurDocumentAbsent(req.GetName())
	}

	return err
}

// absenceVideProuveeParLaCapture dit si, POUR CE DOCUMENT, on a la preuve dans la capture Nintendo
// qu'un document absent revient en « Succeeded, content-length: 0 ». La liste est volontairement
// fermee : hors de ces cas, on ne devine pas, on renvoie un Document nomme et vide.
// ⚠️ CES MOTIFS ETAIENT DU CODE MORT. Un nom reel a la forme
//
//	tenants/current/documents/services/Hammer/users/<uid>/UserAttribute/Data
//
// — il y a « users/<uid>/ » ENTRE le service et le document. Les anciens motifs
// (« /Hammer/UserAttribute/ », « /GameRecord/PointCardRegular/ ») ne matchaient donc JAMAIS, et
// tous les documents absents partaient par la branche « Document nomme sans contenu ».
//
// Ce que ca coutait, mesure du 2026-08-13 a 15:07:18 : a l'entree du hall, le jeu lit
// Hammer/users/<uid>/UserAttribute/Data ; nous lui rendions un Document nomme, la capture prouve un
// SUCCES SANS AUCUN MESSAGE — et le hall s'arretait sur « Une erreur de communication est survenue ».
//
// ON NE CORRIGE QUE LA FAMILLE HAMMER. Les autres documents (PointCard*, BankaraChallenges,
// WeaponPowerMeasurement) recoivent aujourd'hui un Document nomme et le demarrage FONCTIONNE avec :
// les repasser au message vide serait changer deux variables a la fois, et c'est exactement ce qui
// a coute deux jours sur GetSaveRecord. Ils resteront tels quels tant qu'une mesure ne les accuse
// pas.
// RESULTAT : AUCUN document ne supporte le message vide. Toujours rendre le Document nomme.
//
// La capture montre « Succeeded, content-length: 0 » sur Hammer/* et les cartes de points, et j'ai
// essaye de le reproduire fidelement. Mesure du 2026-08-13 a 15:12:03 : le hall demande
// Hammer/users/<uid>/UserAttribute/Data, nous terminons l'appel sans message, et le jeu s'abat
// aussitot sur Thunder.nss:0x667fe8 — l'analyseur de nom de ressource, sur le fil nn.npln.Worker.
// Exactement le meme abort que la veille sur GameRecord/BankaraChallenges/<id>.
//
// DEUX documents, DEUX fois le meme verdict : pour CE client, un appel unaire termine sans message
// construit un Document par defaut au nom VIDE, et le parseur s'arrete dessus. « content-length: 0 »
// cote Nintendo n'est donc pas reproductible en « aucun message » chez nous — il faut lui rendre
// l'objet, portant son nom et sans contenu.
//
// A ne pas confondre avec CloudSave/GetSaveRecord, ou c'est l'inverse : la, le meme objet par defaut
// fait croire au client qu'un record EXISTE, et il faut au contraire un NotFound en deux cadres pour
// qu'il cree le sien. Deux services, deux regles ; ne jamais generaliser de l'un a l'autre.
func absenceVideProuveeParLaCapture(name string) bool {
	return false
}

// LA REGLE N'EST PAS LA MEME POUR TOUS LES DOCUMENTS, et c'est ce que les essais precedents
// manquaient : chaque forme a ete essayee GLOBALEMENT, puis eliminee sur la foi d'UN document.
// Le NotFound en deux cadres, retenu pour tous, fait pourtant boucler GameRecord/
// WeaponPowerMeasurement/16 — mesure du 2026-08-23 : 510 lectures et 239 creations de mesure de
// fete en quatre-vingt-dix secondes, ecran « Connexion a Internet » sans fin, un seul joueur.
//
// On rend donc la forme choisissable PAR FAMILLE DE DOCUMENT, a chaud. « docvide=<motifs> » rend
// l'objet nomme et sans contenu, « docmuet=<motifs> » termine en OK sans aucun message ; les
// motifs sont des fragments de nom separes par des virgules, compares en minuscules. Tout ce qui
// n'est nomme nulle part garde le NotFound en deux cadres, qui fonctionne pour le reste.
func formeDAbsence(name string) string {
	bas := strings.ToLower(name)
	for forme, drapeau := range map[string]string{"vide": "docvide", "muet": "docmuet"} {
		for _, motif := range strings.Split(soirFlagValeur(drapeau), ",") {
			motif = strings.TrimSpace(motif)
			// Les deux cotes en minuscules : « bas » l'est deja, le motif ne l'etait plus depuis
			// que le fichier de drapeaux rend les valeurs dans leur casse d'origine. Un motif ecrit
			// avec une capitale ne trouvait donc plus rien, en silence — et ce drapeau est
			// justement le bouton d'urgence contre un bouclage de document.
			if motif != "" && motif != "1" && strings.Contains(bas, strings.ToLower(motif)) {
				return forme
			}
		}
	}
	return "notfound"
}
