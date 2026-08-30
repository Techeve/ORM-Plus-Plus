---
title: Changelog
description: Versionierung, Conventional Commits und wo die Release-Notes stehen.
sidebar:
  order: 5
---

ORM++ folgt [Semantic Versioning](https://semver.org/lang/de/) und
[Conventional Commits](https://www.conventionalcommits.org/). Jeder
Versions-Tag `vX.Y.Z` löst in der CI-Pipeline einen Job aus, der aus den
Commit-Nachrichten seit dem letzten Tag automatisch einen Changelog erzeugt
([git-cliff](https://git-cliff.org)) und daraus ein GitLab-Release anlegt.

Die jeweils aktuelle, vollständige Historie steht deshalb nicht als
statische Kopie hier in der Doku, sondern an der Quelle, wo sie automatisch
gepflegt wird:

**[Releases auf GitLab ansehen →](https://gitlab.techeve.de/techeve/orm-plus-plus/-/releases)**

## Versionsschema

| Präfix | Bedeutung |
|---|---|
| `feat:` | neues Feature — erhöht die Minor-Version |
| `fix:` | Fehlerbehebung — erhöht die Patch-Version |
| `refactor:`, `docs:`, `test:`, `ci:`, `chore:` | keine funktionale Änderung an der öffentlichen API |

Ein Bruch mit der öffentlichen API (siehe [API-Referenz](/reference/api/))
erhöht die Major-Version und wird im Commit als `BREAKING CHANGE` markiert.
Vor `v1.0.0` galt das Projekt als nicht stabil; seit Phase 5 (siehe
[Architektur](/reference/architecture/)) ist die gesamte v1-API implementiert
und die Testsuite läuft identisch auf allen drei Backends.
