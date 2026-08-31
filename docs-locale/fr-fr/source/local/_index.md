---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Orbit Local - créez et interrogez un graphe de code sur votre propre machine, sans instance GitLab requise."
title: GitLab Orbit Local
---

{{< details >}}

- Édition : Gratuite, GitLab Premium, GitLab Ultimate
- Offre : GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Statut : version bêta

{{< /details >}}

{{< history >}}

- [Introduit](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) dans GitLab 19.0 en tant que [version expérimentale](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Passage](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) en [version bêta](https://docs.gitlab.com/policy/development_stages_support/#beta) dans GitLab 19.1.

{{< /history >}}

GitLab Orbit Local s'exécute entièrement sur votre machine. Créez un graphe de code pour n'importe quel dépôt local et interrogez-le en utilisant le même langage de requête que GitLab Orbit Remote. Aucun compte GitLab ni connexion réseau requis.

- Index : Code uniquement, y compris les fichiers, les définitions et les références inter-fichiers.
- Stockage : DuckDB (fichier local à `~/.orbit/graph.duckdb`)

[Premiers pas avec GitLab Orbit Local](getting-started.md)

## Dans cette section {#in-this-section}

| Page | Description |
|---|---|
| [Commencer](getting-started.md) | Choisissez une méthode d'accès et exécutez votre première requête |
| [Fonctionnement](how-it-works.md) | Pipeline d'indexation, modèle de graphe, exécution des requêtes |
| [Ce que GitLab Orbit Local indexe](indexing.md) | Couverture du code, prise en charge des langages, portée |
| [Référence de schéma](schema.md) | Les quatre types de nœuds dans le graphe de code local |

## Méthodes d'accès {#access-methods}

| Méthode | Description |
|---|---|
| [Le CLI GitLab Orbit (`orbit`)](access/cli.md) | Exécutez directement le binaire `orbit` pour indexer et interroger |
| [Le CLI GitLab (`glab`)](access/glab.md) | Pilotez GitLab Orbit Local via `glab orbit local` |
| [MCP](access/mcp.md) | Exposez le graphe local à Claude Code, Codex et d'autres agents |

## Facturation {#billing}

GitLab Orbit Local ne consomme pas de GitLab Credits. Tout le traitement est local.
