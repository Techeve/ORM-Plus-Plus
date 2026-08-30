// githubsync holt die Issues des öffentlichen GitHub-Spiegels
// (Techeve/ORM-Plus-Plus) in das GitLab-Projekt (techeve/orm-plus-plus).
// GitHub ist für seine
// Issues die Quelle: Neue werden in GitLab angelegt, auf GitHub geschlossene
// in GitLab geschlossen, auf GitHub wieder geöffnete wieder geöffnet.
//
// Die Zuordnung läuft über eine Markierungszeile am Ende der GitLab-
// Beschreibung ("GitHub-Issue: <URL>"). Sie ist zugleich der Rückverweis für
// Leser - eine unsichtbare Kennung gäbe es nicht umsonst dazu.
//
// Kommentare werden bewusst NICHT synchronisiert: Die Diskussion gehört auf
// genau eine Plattform, und der Rückverweis führt dorthin.
//
// Konfiguration über die Umgebung (siehe Job github-issue-sync):
//
//	GITHUB_REPO            z.B. Techeve/ORM-Plus-Plus
//	GITLAB_MIRROR_PROJECT  z.B. techeve/orm-plus-plus
//	CI_API_V4_URL          von GitLab CI gesetzt
//	ISSUE_SYNC_TOKEN       Projekt-Access-Token des Ziels, Scope api
package main

import (
	"fmt"
	"os"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "FEHLER:", err)
		os.Exit(1)
	}
	summary, err := sync(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "FEHLER:", err)
		os.Exit(1)
	}
	fmt.Printf("GitHub-Issues: %d gesehen | %d angelegt | %d geschlossen | %d wieder geöffnet | %d unverändert\n",
		summary.Seen, summary.Created, summary.Closed, summary.Reopened, summary.Unchanged)
}
