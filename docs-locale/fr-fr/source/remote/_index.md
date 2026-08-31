---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Orbit s'exécutant sur une infrastructure hébergée par GitLab"
title: GitLab Orbit Remote
---

{{< details >}}

- Édition : GitLab Premium, GitLab Ultimate
- Offre : GitLab.com
- Statut : version bêta

{{< /details >}}

{{< history >}}

- [Introduite](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) dans GitLab 18.10 [avec un feature flag](https://docs.gitlab.com/administration/feature_flags/) nommé `knowledge_graph`. Désactivé par défaut. Cette fonctionnalité est une [version expérimentale](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Passage](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) en [version bêta](https://docs.gitlab.com/policy/development_stages_support/#beta) dans GitLab 19.1.

{{< /history >}}

> [!flag]
Un feature flag contrôle la disponibilité de cette fonctionnalité. Pour plus d'informations, consultez l'historique. Cette fonctionnalité est disponible pour être testée, mais elle n'est pas prête pour une utilisation en production.

GitLab Orbit Remote s'exécute sur une infrastructure hébergée par GitLab. Activez-le sur un groupe principal et il indexe automatiquement l'ensemble de votre SDLC et de votre code - groupes, projets, utilisateurs, merge requests, pipelines, vulnérabilités et code source - dans un graphe de propriétés ClickHouse.

- Index : graphe SDLC complet + code
- Stockage : ClickHouse (géré, aucune configuration requise)

[Premiers pas avec GitLab Orbit Remote](getting-started.md)

## Dans cette section {#in-this-section}

| Page | Description |
|---|---|
| [Commencer](getting-started.md) | Activer GitLab Orbit et exécuter votre première requête |
| [Fonctionnement](how-it-works.md) | Pipeline d'indexation, modèle de graphe, exécution des requêtes |
| [Ce que GitLab Orbit indexe](indexing.md) | Couverture SDLC, prise en charge des langages, portée de l'indexation |
| [Sécurité](security.md) | Rôles requis pour les requêtes, le modèle d'autorisation et l'accès programmatique |
| [Référence de schéma](schema.md) | Les 28 types de nœuds répartis sur 6 domaines |
| [Cookbook](cookbook.md) | Requêtes à copier-coller pour les cas d'utilisation courants |
| [Langage de requête](queries/) | Référence complète du DSL de requête |

## Méthodes d'accès {#access-methods}

| Méthode | Description |
|---|---|
| [GitLab Duo Agent Platform](access/duo.md) | Questions en langage naturel via l'interface GitLab |
| [MCP](access/mcp.md) | Connecter Claude Code, Codex et d'autres agents |
| [Le CLI GitLab (`glab`)](access/glab.md) | `glab orbit remote` pour les scripts et la découverte (disponible dans `glab` 1.94 ou version ultérieure) |
| [API REST](access/api.md) | Interrogation depuis des scripts, des pipelines CI ou des outils personnalisés |

## Facturation {#billing}

Les requêtes MCP et API REST consomment des GitLab Credits. Les requêtes GitLab Duo Agent Platform sont sans frais.
