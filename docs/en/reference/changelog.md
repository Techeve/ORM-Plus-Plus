---
title: Changelog
description: Versioning, Conventional Commits, and where the release notes live.
sidebar:
  order: 5
---

ORM++ follows [Semantic Versioning](https://semver.org/) and
[Conventional Commits](https://www.conventionalcommits.org/). Every version
tag `vX.Y.Z` triggers a CI job that generates a changelog from the commit
messages since the last tag ([git-cliff](https://git-cliff.org)) and turns it
into a GitLab release.

The current, complete history therefore doesn't live as a static copy in
these docs — it lives at the source, where it's kept up to date
automatically:

**[View releases on GitLab →](https://gitlab.techeve.de/orm-plus-plus/orm-plus-plus/-/releases)**

## Versioning scheme

| Prefix | Meaning |
|---|---|
| `feat:` | new feature — bumps the minor version |
| `fix:` | bug fix — bumps the patch version |
| `refactor:`, `docs:`, `test:`, `ci:`, `chore:` | no functional change to the public API |

A break in the public API (see the [API reference](/en/reference/api/))
bumps the major version and is marked `BREAKING CHANGE` in the commit. Before
`v1.0.0` the project wasn't considered stable; since phase 5 (see
[Architecture](/en/reference/architecture/)) the entire v1 API is implemented
and the test suite runs identically across all three backends.
