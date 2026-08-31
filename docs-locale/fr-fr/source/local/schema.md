---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Référence pour les quatre types de nœuds du graphe de code GitLab Orbit Local et leur connexion.
title: Référence de schéma
---

{{< details >}}

- Édition : Gratuite, GitLab Premium, GitLab Ultimate
- Offre : GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Statut : version bêta

{{< /details >}}

{{< history >}}

- [Introduit](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) dans GitLab 19.0 en tant que [version expérimentale](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Modifié](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) en [version bêta](https://docs.gitlab.com/policy/development_stages_support/#beta) dans GitLab 19.1.

{{< /history >}}

> [!note]
GitLab Orbit Local est expérimental. Les fonctionnalités et la forme des commandes sont susceptibles de changer avant la disponibilité générale.

GitLab Orbit Local indexe 4 types de nœuds, tous dans le domaine du code source. Il n'existe pas de couche SDLC, car GitLab Orbit Local ne se connecte pas à GitLab.

Pour inspecter le schéma DuckDB en temps réel à tout moment :

```shell
orbit schema
```

## Code source {#source-code}

| Type de nœud | Description | Propriétés clés |
|-----------|-------------|----------------|
| `Directory` | Répertoire dans le dépôt indexé | `id`, `path`, `name` |
| `File` | Fichier de code source | `id`, `path`, `name`, `extension`, `language`, `content` |
| `Definition` | Définition de fonction, de classe, de méthode ou de module | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `ImportedSymbol` | Référence d'importation ou de symbole inter-fichiers | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## Relations {#relationships}

Les arêtes du graphe local relient :

- Les répertoires aux fichiers et sous-répertoires qu'ils contiennent
- Les fichiers aux définitions qu'ils déclarent
- Les fichiers aux symboles qu'ils importent
- Les symboles importés aux définitions vers lesquelles ils se résolvent dans d'autres fichiers

## Différences avec GitLab Orbit Remote {#differences-from-gitlab-orbit-remote}

[GitLab Orbit Remote](../remote/schema.md) indexe 28 types de nœuds répartis sur 6 domaines. GitLab Orbit Local couvre uniquement le domaine du code source. Tout ce qui nécessite des données GitLab (merge requests, pipelines, utilisateurs, vulnérabilités, éléments de travail) n'est pas disponible.

## Notes {#notes}

- Les ID de définition sont des entiers hachés par contenu, dont la portée est définie par chemin de fichier. La même fonction dans deux dépôts indexés aura des ID différents.
- Les champs `content` des nœuds `Definition` et `File` contiennent le texte source complet. Ces champs sont renseignés afin que les outils d'agent puissent hydrater le code sans effectuer de lectures de fichiers séparées.
- Il n'existe pas de couche d'autorisation. GitLab Orbit Local n'applique pas de contrôle d'accès par utilisateur. Le fichier de graphe situé à l'emplacement `~/.orbit/graph.duckdb` est protégé uniquement par les permissions du système de fichiers.
