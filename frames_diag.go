package main

// frames_diag — nommer les trames HTTP/2, dans les deux sens, sur le flux DECHIFFRE.
//
// POURQUOI. Le hall de Splatoon 3 rend « Une erreur de communication est survenue » alors que le
// serveur ne recoit AUCUN appel applicatif pendant des minutes, tout en echangeant des megaoctets.
// Les journaux de l'emulateur donnent les TAILLES des envois (un triplet 59 / 89 / 102 octets repete
// des milliers de fois) mais jamais les octets : impossible de savoir de quelles trames il s'agit.
// Deviner le type d'une trame HTTP/2 a partir d'une arithmetique sur des tailles d'enregistrements
// TLS ne vaut rien — on mesure.
//
// COMMENT. Le serveur termine le TLS lui-meme (credentials.NewTLS), donc la connexion rendue par
// ServerHandshake porte deja le flux EN CLAIR. On l'enveloppe pour compter les trames au passage.
// grpc-go a sa propre pile HTTP/2 : GODEBUG=http2debug n'y produit rien, d'ou ce compteur maison.
//
// L'en-tete d'une trame HTTP/2 fait 9 octets : longueur sur 3, type sur 1, drapeaux sur 1, puis
// l'identifiant de flux sur 4 (bit de poids fort reserve). On ne lit que les en-tetes : aucune
// charge utile n'est copiee, aucun contenu de joueur ne passe dans le journal.
//
// Le comptage est resume toutes les 5 s pour ne pas noyer le journal (a 5 ms d'aller-retour, il y a
// des centaines de trames par seconde). Drapeau a chaud « frames ».

import (
	"encoding/binary"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

var nomTrame = map[byte]string{
	0x0: "DATA", 0x1: "HEADERS", 0x2: "PRIORITY", 0x3: "RST_STREAM",
	0x4: "SETTINGS", 0x5: "PUSH_PROMISE", 0x6: "PING", 0x7: "GOAWAY",
	0x8: "WINDOW_UPDATE", 0x9: "CONTINUATION",
}

func nommerTrame(t byte) string {
	if n, ok := nomTrame[t]; ok {
		return n
	}
	return "type" + string(rune('0'+t%10))
}

// compteurTrames agrege ce qui passe sur UNE connexion, par sens et par type.
type compteurTrames struct {
	mu        sync.Mutex
	tag       string
	parType   map[string]int // "recu DATA" -> n
	octets    map[string]int // idem, en octets de charge utile
	flux      map[uint32]int // trames par identifiant de flux
	dataRecu  map[uint32]int // octets de DATA recus, par flux
	dataEmis  map[uint32]int // octets de DATA emis, par flux
	dernier   time.Time
	demarrage time.Time
}

func (c *compteurTrames) noter(sens string, typ byte, flags byte, streamID uint32, taille int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cle := sens + " " + nommerTrame(typ)
	c.parType[cle]++
	c.octets[cle] += taille
	c.flux[streamID]++
	if typ == 0x0 { // DATA
		if sens == "recu" {
			c.dataRecu[streamID] += taille
		} else {
			c.dataEmis[streamID] += taille
		}
	}

	// RST_STREAM et GOAWAY sont rares et decisifs : on les nomme un par un, jamais en resume.
	if typ == 0x3 || typ == 0x7 {
		log.Printf("[NPLN trames] %s %s %s flux=%d taille=%d drapeaux=0x%02x",
			c.tag, sens, nommerTrame(typ), streamID, taille, flags)
	}

	if time.Since(c.dernier) < 5*time.Second {
		return
	}
	c.dernier = time.Now()

	total := 0
	for _, n := range c.parType {
		total += n
	}
	log.Printf("[NPLN trames] %s resume apres %.0f s : %d trames sur %d flux",
		c.tag, time.Since(c.demarrage).Seconds(), total, len(c.flux))
	for cle, n := range c.parType {
		log.Printf("[NPLN trames]   %s : %d trames, %d o", cle, n, c.octets[cle])
	}

	// PAR FLUX : c'est la seule facon de savoir LEQUEL des flux longs porte le va-et-vient. Le
	// total ne le dit pas, et l'identifiant de flux se rattache ensuite a sa methode par l'ordre
	// d'ouverture (les HEADERS du client, flux impairs croissants, dans l'ordre des RPC servis).
	type ligne struct {
		id uint32
		n  int
	}
	classe := make([]ligne, 0, len(c.flux))
	for id, n := range c.flux {
		classe = append(classe, ligne{id, n})
	}
	sort.Slice(classe, func(i, j int) bool { return classe[i].n > classe[j].n })

	for i, l := range classe {
		if i >= 6 {
			break
		}
		log.Printf("[NPLN trames]   flux %d : %d trames, DATA recu %d o / emis %d o",
			l.id, l.n, c.dataRecu[l.id], c.dataEmis[l.id])
	}
}

// connTracee lit et ecrit normalement, en decodant au passage les en-tetes de trame.
type connTracee struct {
	net.Conn
	cpt *compteurTrames

	// Etat du decodeur, un par sens : une trame peut etre coupee en plusieurs lectures.
	resteRecu []byte
	resteEmis []byte

	// Le client envoie d'abord le preambule HTTP/2 « PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n », 24 octets qui
	// ne sont PAS une trame. Sans les sauter, le decodeur les lit comme un en-tete et tout le flux
	// recu est decale d'un cran.
	prefaceRestant int
}

func decouper(reste []byte, buf []byte, noter func(typ, flags byte, id uint32, n int)) []byte {
	flux := append(reste, buf...)

	for len(flux) >= 9 {
		taille := int(flux[0])<<16 | int(flux[1])<<8 | int(flux[2])
		typ := flux[3]
		flags := flux[4]
		id := binary.BigEndian.Uint32(flux[5:9]) & 0x7fffffff

		if 9+taille > len(flux) {
			break // trame incomplete : on garde ce qu'on a
		}
		noter(typ, flags, id, taille)
		flux = flux[9+taille:]
	}

	// Ne jamais laisser le tampon d'attente grossir sans borne (charge utile max = 16 Mo).
	if len(flux) > 1<<20 {
		return nil
	}

	return append([]byte(nil), flux...)
}

func (c *connTracee) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n <= 0 || !soirFlag("frames") {
		return n, err
	}

	vu := b[:n]
	if c.prefaceRestant > 0 {
		saute := min(c.prefaceRestant, len(vu))
		c.prefaceRestant -= saute
		vu = vu[saute:]
	}

	if len(vu) > 0 {
		c.resteRecu = decouper(c.resteRecu, vu, func(t, f byte, id uint32, taille int) {
			c.cpt.noter("recu", t, f, id, taille)
		})
	}

	return n, err
}

func (c *connTracee) Write(b []byte) (int, error) {
	if soirFlag("frames") {
		c.resteEmis = decouper(c.resteEmis, b, func(t, f byte, id uint32, taille int) {
			c.cpt.noter("emis", t, f, id, taille)
		})
	}
	return c.Conn.Write(b)
}

// credsTracees enveloppe les identifiants TLS pour tracer le flux DECHIFFRE.
type credsTracees struct {
	credentials.TransportCredentials
}

func (c credsTracees) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, info, err := c.TransportCredentials.ServerHandshake(raw)
	if err != nil || conn == nil {
		return conn, info, err
	}

	// Le preambule HTTP/2 du client (« PRI * HTTP/2.0 ... », 24 octets) precede la premiere trame ;
	// on le saute en amorcant le decodeur avec, sinon tout le flux recu est decale.
	tracee := &connTracee{
		Conn:           conn,
		prefaceRestant: len("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"),
		cpt: &compteurTrames{
			tag:       raw.RemoteAddr().String(),
			parType:   map[string]int{},
			octets:    map[string]int{},
			flux:      map[uint32]int{},
			dataRecu:  map[uint32]int{},
			dataEmis:  map[uint32]int{},
			dernier:   time.Now(),
			demarrage: time.Now(),
		},
	}

	return tracee, info, err
}

// tracerLesTrames enveloppe des identifiants TLS pour compter les trames HTTP/2 qui les traversent.
func tracerLesTrames(creds credentials.TransportCredentials) credentials.TransportCredentials {
	return credsTracees{TransportCredentials: creds}
}
