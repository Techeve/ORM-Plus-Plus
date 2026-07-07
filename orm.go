// Package orm ist ORM++ — ein model-first Persistenz-Modul für Go mit
// Event Sourcing, Projektionen, Snapshots und Expand/Contract-Migrationen,
// optimiert für SQLite, PostgreSQL und YugabyteDB.
//
// Die konsumierende Anwendung deklariert Modelle und arbeitet mit Commands,
// Events und Query-Schnittstellen — sie schreibt kein SQL und kennt keine
// Datenbank-Details. Siehe ROADMAP.md für den Entwicklungsplan.
package orm

// Version ist die aktuelle Modulversion. Sie wird bei Releases über
// Git-Tags (v*) gepflegt; der Changelog wird mit git-cliff aus den
// Conventional Commits generiert.
const Version = "0.1.0-dev"
