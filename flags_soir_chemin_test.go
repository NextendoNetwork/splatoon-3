package main

// Verrou sur la RESOLUTION du fichier de drapeaux.
//
// MESURE DU 2026-08-22. Le drapeau « clestls » ouvrait le journal des secrets TLS sur le port 443
// et restait sans effet sur le 7575. Cause : cheminFlags vaut « /data/soir.flags », un chemin de
// CONTENEUR, alors que le meme binaire tourne AUSSI sur l'hote en systemd pour l'arbitre de
// session (NPLN_GAMESYNC_ONLY=1) — ou « /data » existe mais ne contient pas le fichier.
//
// La portee depasse de loin le TLS : depuis qu'il existe, l'arbitre n'avait JAMAIS lu un seul
// drapeau, et il le faisait EN SILENCE. Ces tests fixent les deux corrections : chercher aux
// endroits de secours, et dire d'ou viennent les drapeaux.

import (
	"os"
	"path/filepath"
	"testing"
)

// sansAucunFichierDeDrapeaux met la resolution a nu : aucun chemin ne pointe sur un fichier.
func sansAucunFichierDeDrapeaux(t *testing.T) {
	t.Helper()
	ancienChemin, ancienSecours, ancienneSource := cheminFlags, cheminsFlagsDeSecours, sourceDesDrapeaux
	cheminFlags = filepath.Join(t.TempDir(), "absent.flags")
	cheminsFlagsDeSecours = nil
	os.Unsetenv("NPLN_FLAGS_FILE")
	invaliderCacheFlags()
	t.Cleanup(func() {
		cheminFlags, cheminsFlagsDeSecours, sourceDesDrapeaux = ancienChemin, ancienSecours, ancienneSource
		invaliderCacheFlags()
	})
}

func ecrireDrapeaux(t *testing.T, contenu string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "soir.flags")
	if err := os.WriteFile(f, []byte(contenu), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestDrapeauxLusDepuisLeCheminDeSecours(t *testing.T) {
	// Le cas exact de l'arbitre : cheminFlags pointe dans le vide, le fichier est ailleurs.
	sansAucunFichierDeDrapeaux(t)
	cheminsFlagsDeSecours = []string{ecrireDrapeaux(t, "clestls"+"\n")}
	invaliderCacheFlags()

	if !soirFlag("clestls") {
		t.Error("drapeau non vu : c'est exactement la panne du port 7575, l'arbitre lisait un fichier absent")
	}
	if sourceDesDrapeaux != cheminsFlagsDeSecours[0] {
		t.Errorf("source annoncee = %q, attendu %q — un serveur qui n'obeit pas doit dire d'ou il lit",
			sourceDesDrapeaux, cheminsFlagsDeSecours[0])
	}
}

func TestLeCheminPrincipalPrimeSurLeSecours(t *testing.T) {
	sansAucunFichierDeDrapeaux(t)
	cheminFlags = ecrireDrapeaux(t, "principal"+"\n")
	cheminsFlagsDeSecours = []string{ecrireDrapeaux(t, "secours"+"\n")}
	invaliderCacheFlags()

	if !soirFlag("principal") {
		t.Error("le chemin principal doit gagner quand il existe")
	}
	if soirFlag("secours") {
		t.Error("le secours ne doit servir QUE si le principal est illisible")
	}
}

func TestVariableEnvironnementTranche(t *testing.T) {
	sansAucunFichierDeDrapeaux(t)
	cheminFlags = ecrireDrapeaux(t, "principal"+"\n")
	cheminsFlagsDeSecours = []string{ecrireDrapeaux(t, "secours"+"\n")}
	t.Setenv("NPLN_FLAGS_FILE", ecrireDrapeaux(t, "impose"+"\n"))
	invaliderCacheFlags()

	if !soirFlag("impose") {
		t.Error("NPLN_FLAGS_FILE doit trancher : c'est le moyen de forcer un chemin sans redeployer")
	}
	if soirFlag("principal") {
		t.Error("la variable posee doit court-circuiter cheminFlags")
	}
}

func TestAucunFichierNeLaisseToutParDefaut(t *testing.T) {
	sansAucunFichierDeDrapeaux(t)

	if soirFlag("clestls") {
		t.Error("sans fichier, aucun drapeau ne doit etre actif")
	}
	if sourceDesDrapeaux != "" {
		t.Errorf("source = %q, attendu vide quand rien n'est lisible", sourceDesDrapeaux)
	}
}
