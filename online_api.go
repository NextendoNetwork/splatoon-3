package main

// online_api — qui joue en ce moment, pour la page publique « Salons en direct ».
//
// Le monitoring interne (/api/stats) porte des adresses IP, des villes, des fournisseurs d'acces
// et des identifiants de compte. Rien de tout cela n'a sa place sur une page publique, et le
// jeton qui protege /api/stats existe precisement pour ca. Cette vue-ci est l'inverse : aucun
// jeton, mais elle ne rend que ce qu'un joueur voit deja depuis son propre hall — combien de
// salons tournent, dans quel mode, et quels PID s'y trouvent.
//
// Les pseudos ne sortent PAS d'ici : le monitoring ne les connait pas (voir dashSnapshot), c'est
// le site qui les resout depuis le PID. On garde ce decoupage — un serveur de jeu n'a pas a
// republier l'annuaire des comptes.
//
// Les salons prives sont comptes mais jamais detailles : leur interet est justement de ne pas
// figurer sur une place publique.

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"
)

type joueurEnSalon struct {
	PID  uint64 `json:"pid"`
	Host bool   `json:"host,omitempty"`
}

type salonPublic struct {
	ID      uint32          `json:"id"`
	Mode    string          `json:"mode"`
	Label   string          `json:"label"`
	Count   int             `json:"count"`
	Max     uint16          `json:"max"`
	Players []joueurEnSalon `json:"players"`
}

type resumeDuMode struct {
	Mode      string `json:"mode"`
	Lobbies   int    `json:"lobbies"`
	InLobby   int    `json:"inLobby"`
	Searching int    `json:"searching"`
}

// modePrive dit si un mode ne doit pas etre detaille publiquement.
func modePrive(cle string) bool {
	return cle == "private" || cle == "coop_private"
}

// onlineHandler : GET /api/online
// Une photographie de l'activite en cours, sans rien qui identifie un joueur hors du jeu.
func onlineHandler(w http.ResponseWriter, r *http.Request) {
	s := dashSnapshot()

	salons := []salonPublic{}
	prives := 0
	parMode := map[string]*resumeDuMode{}

	resume := func(cle string) *resumeDuMode {
		if parMode[cle] == nil {
			parMode[cle] = &resumeDuMode{Mode: cle}
		}
		return parMode[cle]
	}

	for _, g := range s.Gatherings {
		cle := g.Mode
		if cle == "" {
			// Un salon cree par un hote, avant toute mise en file : le matchmaker ne lui a pas
			// encore attribue de mode. On ne devine pas, on le range a part.
			cle = "lobby"
		}
		if modePrive(cle) {
			prives++
			continue
		}
		m := resume(cle)
		m.Lobbies++
		m.InLobby += len(g.Players)

		joueurs := make([]joueurEnSalon, 0, len(g.Players))
		for _, p := range g.Players {
			joueurs = append(joueurs, joueurEnSalon{PID: p.PID, Host: p.Host})
		}
		salons = append(salons, salonPublic{
			ID: g.ID, Mode: cle, Label: g.Type,
			Count: len(g.Players), Max: g.Max, Players: joueurs,
		})
	}

	// Les joueurs en recherche : ils ne sont dans aucun salon, mais ils font l'attente que la
	// page doit montrer — c'est la moitie interessante de l'information.
	recherche := []joueurEnSalon{}
	for _, p := range s.Players {
		if p.Gathering != 0 || p.Mode == "" {
			continue
		}
		cle := p.ModeKey
		if cle == "" || modePrive(cle) {
			continue
		}
		resume(cle).Searching++
		recherche = append(recherche, joueurEnSalon{PID: p.PID})
	}

	modes := make([]resumeDuMode, 0, len(parMode))
	for _, m := range parMode {
		modes = append(modes, *m)
	}
	sort.Slice(modes, func(i, j int) bool { return modes[i].Mode < modes[j].Mode })
	sort.Slice(salons, func(i, j int) bool {
		if salons[i].Mode != salons[j].Mode {
			return salons[i].Mode < salons[j].Mode
		}
		return salons[i].ID < salons[j].ID
	})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"now":            time.Now().UTC().Format(time.RFC3339),
		"connected":      s.Connected,
		"inLobby":        s.InLobby,
		"activeLobbies":  s.ActiveLobbies,
		"privateLobbies": prives,
		"modes":          modes,
		"lobbies":        salons,
		"searching":      recherche,
	})
}
