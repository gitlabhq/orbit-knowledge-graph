---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Comment GitLab Orbit Local crée et interroge un graphe de code sur votre machine à l'aide de la CLI GitLab Orbit et de DuckDB."
title: Fonctionnement de GitLab Orbit Local
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

> [!note]
GitLab Orbit Local est expérimental. Les fonctionnalités et la forme des commandes sont susceptibles de changer avant la disponibilité générale.

## Pipeline d'indexation {#indexing-pipeline}

Lorsque vous exécutez `orbit index`, GitLab Orbit Local :

1. Parcourt l'arborescence du répertoire du dépôt en respectant `.gitignore`.
1. Transmet chaque fichier source à un parseur spécifique au langage (rust-analyzer, tree-sitter ou un parseur personnalisé selon le langage).
1. Extrait les définitions (fonctions, classes, modules), les déclarations d'import et les références de symboles inter-fichiers.
1. Écrit les résultats sous forme de nœuds et d'arêtes dans un fichier DuckDB local à l'emplacement `~/.orbit/graph.duckdb`.

Le pipeline v2 exécute tous les parseurs de langages en parallèle. L'indexation d'un dépôt de taille moyenne se termine généralement en quelques secondes.

## Le modèle de graphe {#the-graph-model}

GitLab Orbit Local crée un graphe exclusivement dédié au code. Il n'a pas accès aux données SDLC (merge requests, pipelines, utilisateurs) car il n'y a pas de connexion GitLab.

Nœuds dans le graphe local :

- **Fichier** \- un fichier source dans le dépôt
- **Répertoire** \- un répertoire dans le dépôt
- **Définition** \- une fonction, une classe, un module ou un autre symbole nommé
- **Symbole importé** \- un symbole importé depuis un autre fichier ou package

Les arêtes relient les fichiers à leurs définitions, les fichiers à leurs imports et les définitions aux symboles qu'elles référencent d'un fichier à l'autre.

## Exécution des requêtes {#query-execution}

GitLab Orbit Local expose le graphe sous la forme d'une base de données DuckDB. Exécutez n'importe quelle requête SQL en lecture seule avec `orbit sql` :

1. `orbit sql` ouvre `~/.orbit/graph.duckdb` en lecture seule.
1. Votre SQL s'exécute directement sur les tables du graphe — sans compilation DSL ni couche d'autorisation.
1. Les résultats sont renvoyés en flux sous forme de tableau, JSON, NDJSON ou CSV.

Toutes les données du graphe sont accessibles à quiconque exécute la CLI.

## Stockage {#storage}

Le graphe est stocké dans un seul fichier DuckDB à l'emplacement `~/.orbit/graph.duckdb`. Plusieurs dépôts partagent la même base de données. Chaque dépôt est délimité par son ID de projet et sa branche dans la table du manifeste.

## Langages pris en charge {#supported-languages}

Les 13 langages pris en charge par GitLab Orbit Remote sont également pris en charge localement : Ruby, Java, Kotlin, Python, TypeScript, JavaScript, Rust, Go, C#, C, C++, PHP et Bash/Shell.

Consultez [l'indexation des données avec GitLab Orbit](../indexed-data.md#supported-languages) pour accéder au tableau complet de prise en charge des langages.

## Facturation {#billing}

GitLab Orbit Local ne consomme pas de GitLab Credits. Tout le traitement est local.
