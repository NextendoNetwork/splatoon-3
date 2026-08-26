package main

// Inscription a un Splatfest : GetFestEntry + CreateFestEntry.
//
// Releve sur la capture Nintendo du 2026-08-20 16:09 (relais-session, fete JUEA-00107, equipe
// Bravo, region EU) — la premiere capture d'un vrai Splatfest :
//
//	CreateFestEntry
//	  requete : parent     = "tenants/current/fests/JUEA-00107"
//	            fest_entry = { fest_team = "Bravo", fest_region = "EU" }
//	  reponse : name        = "tenants/t-dce9377b-lp1/fests/JUEA-00107/entries/u-exemple7000000000000"
//	            fest_team   = "Bravo"
//	            fest_region = "EU"
//
//	GetFestEntry
//	  requete : name = "tenants/current/fests/JUEA-00107/entries/current"
//	  reponse : AUCUNE tant que le joueur n'a pas choisi son equipe
//
// Deux lois s'y retrouvent, les memes que partout dans NPLN : la requete porte l'alias
// « tenants/current » et la reponse le rend RESOLU vers le locataire concret ; et « current »
// designe l'appelant, qu'il faut remplacer par son uid reel.
//
// ⚠️ En jeu, le choix d'equipe est IRREVERSIBLE : une entree deja posee ne se remplace pas, on la
// rend telle quelle. Sans cela un joueur pourrait changer de camp en cours de fete.

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	toyohrpb "npln.nintendo.net/npln-practice/proto/toyohr/v1"
)

// festEntriesActives dit si l'inscription aux fetes est servie. Defaut : NON — le drapeau
// « festequipes » l'allume. Eteinte, la methode rend exactement ce qu'elle rendait avant d'exister
// (Unimplemented), pour qu'allumer ou eteindre ne change rien d'autre.
func festEntriesActives() bool { return soirFlag("festequipes") }

// cheminEntreesFest : une VARIABLE pour que les tests pointent un fichier temporaire.
var cheminEntreesFest = "/data/fest_entries.json"

// entreeFest est ce qu'on retient d'un joueur pour une fete donnee.
type entreeFest struct {
	Equipe string `json:"equipe"`
	Region string `json:"region"`

	// Qui, et quand. Ces deux champs ne servent PAS au jeu — il ne demande jamais que l'equipe et
	// la region — mais sans eux le tableau de bord ne saurait montrer qu'une liste d'identifiants
	// NPLN, illisibles pour un humain. Le PID est la seule cle qui permette de retrouver le pseudo
	// Nextendo du votant ; l'heure du vote raconte comment la fete s'est remplie.
	//
	// Les deux sont facultatifs a la relecture : une inscription posee avant ce changement les
	// laisse a zero, et tout continue de fonctionner.
	Pid  uint64 `json:"pid,omitempty"`
	Vote int64  `json:"vote,omitempty"` // horodatage unix du choix du camp
}

var entreesFest = struct {
	sync.Mutex
	m      map[string]entreeFest // "<festID>/<uid>" -> entree
	chargé bool
}{m: map[string]entreeFest{}}

func chargerEntreesFestLocked() {
	if entreesFest.chargé {
		return
	}
	entreesFest.chargé = true
	b, err := os.ReadFile(cheminEntreesFest)
	if err != nil {
		return
	}
	m := map[string]entreeFest{}
	if json.Unmarshal(b, &m) == nil {
		entreesFest.m = m
		log.Printf("[NPLN fest] %d inscription(s) relue(s) depuis %s", len(m), cheminEntreesFest)
	}
}

func enregistrerEntreesFestLocked() {
	b, err := json.MarshalIndent(entreesFest.m, "", "  ")
	if err != nil {
		return
	}
	tmp := cheminEntreesFest + ".tmp"
	if os.WriteFile(tmp, b, 0o644) != nil {
		return
	}
	// Remplacement atomique : une coupure au milieu d'une ecriture ne doit pas laisser un fichier
	// tronque, sinon toutes les inscriptions de la fete seraient perdues.
	_ = os.MkdirAll(filepath.Dir(cheminEntreesFest), 0o755)
	_ = os.Rename(tmp, cheminEntreesFest)
}

// festIDDuChemin extrait l'identifiant de fete d'un nom de ressource
// (« tenants/…/fests/JUEA-00107/entries/… » -> « JUEA-00107 »).
// collectionsDeFete : les segments de chemin sous lesquels un identifiant de fete peut vivre.
//
// ⚠️ ON N'EN CONNAISSAIT QU'UN. Le decoupage ne cherchait que « fests », celui des inscriptions.
// Or GetFestResult demande son verdict sous « festResults », et SelectFestSchedule sous
// « festSchedules » :
//
//	tenants/current/fests/JUEA-00015/entries/<uid>      inscription
//	tenants/current/festResults/JUEA-00015              verdict
//	tenants/current/festSchedules/JUEA-00015            calendrier
//
// MESURE DU 2026-08-23. Fete maison en cours : 144 appels a GetFestResult, et 144 fois
// « nom non decoupe -> objet vide ». Le jeu recevait donc un verdict VIDE pour une fete qui court,
// ce qui est exactement le mur releve le 2026-08-20 — onze consoles l'avaient recue, aucune n'a pu
// jouer, et la seule qui ne l'avait pas recue continuait. Le calendrier maison n'y etait pour rien :
// la comparaison champ par champ avec la capture Nintendo donne vingt champs de chaque cote et
// aucun manquant. C'etait ce decoupage.
var collectionsDeFete = []string{"fests", "festResults", "festSchedules"}

func festIDDuChemin(nom string) string {
	seg := strings.Split(nom, "/")
	for i, s := range seg {
		for _, c := range collectionsDeFete {
			if s == c && i+1 < len(seg) {
				return seg[i+1]
			}
		}
	}
	return ""
}

// nomEntreeFest construit le nom RESOLU d'une inscription, comme Nintendo le rend.
func nomEntreeFest(tenant, festID, uid string) string {
	return tenant + "/fests/" + festID + "/entries/" + uid
}

func (s *festServer) GetFestEntry(ctx context.Context, req *toyohrpb.GetFestEntryRequest) (*toyohrpb.FestEntry, error) {
	if !festEntriesActives() {
		return nil, status.Error(codes.Unimplemented, "method GetFestEntry not implemented")
	}

	festID := festIDDuChemin(req.GetName())
	uid := uidFromCtx(ctx)
	if festID == "" || uid == "" {
		return nil, status.Error(codes.InvalidArgument, "fest entry name without a fest or a caller")
	}

	entreesFest.Lock()
	chargerEntreesFestLocked()
	e, ok := entreesFest.m[festID+"/"+uid]
	entreesFest.Unlock()

	if !ok {
		// Etat « pas encore inscrit », releve sur la capture : Nintendo ne rend RIEN. C'est ce qui
		// autorise le jeu a proposer l'ecran de choix d'equipe.
		log.Printf("[NPLN fest] GetFestEntry %s / %s -> aucune inscription (le joueur n'a pas choisi)", festID, uid)
		return nil, status.Error(codes.NotFound, "fest entry not found")
	}

	tenant := tenantFromCtx(ctx)
	if tenant == "" {
		tenant = npnTenant
	}
	log.Printf("[NPLN fest] GetFestEntry %s / %s -> equipe %s (%s)", festID, uid, e.Equipe, e.Region)
	return &toyohrpb.FestEntry{
		Name:       nomEntreeFest(tenant, festID, uid),
		FestTeam:   e.Equipe,
		FestRegion: e.Region,
	}, nil
}

func (s *festServer) CreateFestEntry(ctx context.Context, req *toyohrpb.CreateFestEntryRequest) (*toyohrpb.FestEntry, error) {
	if !festEntriesActives() {
		return nil, status.Error(codes.Unimplemented, "method CreateFestEntry not implemented")
	}

	festID := festIDDuChemin(req.GetParent())
	uid := uidFromCtx(ctx)
	if festID == "" || uid == "" {
		return nil, status.Error(codes.InvalidArgument, "fest entry without a fest or a caller")
	}

	demande := req.GetFestEntry()
	equipe, region := demande.GetFestTeam(), demande.GetFestRegion()
	if equipe == "" {
		return nil, status.Error(codes.InvalidArgument, "fest entry without a team")
	}

	entreesFest.Lock()
	chargerEntreesFestLocked()
	cle := festID + "/" + uid
	deja, existe := entreesFest.m[cle]
	if existe {
		// ⚠️ IRREVERSIBLE, comme en jeu. Le joueur ne choisit qu'une fois ; une seconde demande rend
		// l'inscription DEJA posee, jamais la nouvelle.
		entreesFest.Unlock()
		log.Printf("[NPLN fest] CreateFestEntry %s / %s : deja inscrit en %s — demande de %s ignoree",
			festID, uid, deja.Equipe, equipe)
		equipe, region = deja.Equipe, deja.Region
	} else {
		entreesFest.m[cle] = entreeFest{
			Equipe: equipe, Region: region,
			// pidDeLAppelant et NON dashPIDFromCtx : le second ne lit que le jeton et rend 0
			// des qu'il ne peut pas le verifier, ce qui laisse le votant anonyme pour toute
			// la duree de la fete — une inscription ne se rejoue pas, la branche qui ecrit ce
			// champ n'est atteinte qu'une fois par joueur et par fete.
			Pid: pidDeLAppelant(ctx), Vote: time.Now().Unix(),
		}
		enregistrerEntreesFestLocked()
		n := len(entreesFest.m)
		entreesFest.Unlock()
		log.Printf("[NPLN fest] CreateFestEntry %s / %s -> equipe %s (%s), %d inscrit(s) au total",
			festID, uid, equipe, region, n)
	}

	tenant := tenantFromCtx(ctx)
	if tenant == "" {
		tenant = npnTenant
	}
	return &toyohrpb.FestEntry{
		Name:       nomEntreeFest(tenant, festID, uid),
		FestTeam:   equipe,
		FestRegion: region,
	}, nil
}
