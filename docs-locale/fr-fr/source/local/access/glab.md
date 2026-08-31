---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Installez, indexez et interrogez GitLab Orbit Local via la CLI GitLab avec glab orbit local et glab orbit setup."
title: Utiliser GitLab Orbit Local avec la CLI GitLab (`glab`)
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

> [!disclaimer]

La [CLI GitLab (`glab`)](https://docs.gitlab.com/cli/) est la méthode canonique pour installer, exécuter et intégrer GitLab Orbit Local avec votre agent d'IA. `glab orbit local` reproduit le comportement de `glab orbit remote`, de sorte que les mêmes modèles fonctionnent, que vous interrogiez l'instance GitLab ou votre machine locale.

> [!note]
`glab orbit local` et `glab orbit setup` sont disponibles dès aujourd'hui, dans `glab` 1.94 ou version ultérieure.

Deux commandes de niveau supérieur :

- `glab orbit local` : encapsule le binaire `orbit` géré pour indexer et interroger le graphe local.
- `glab orbit setup` : intégration guidée qui vérifie l'accès, installe le skill GitLab Orbit et installe le binaire local.

## Prérequis {#prerequisites}

- `glab` 1.94 ou version ultérieure est installé.
- Un dépôt Git local à indexer.

Aucun compte GitLab ni connexion réseau n'est nécessaire pour utiliser `glab orbit local` une fois le binaire installé.

## Installation {#install}

Installez le binaire `orbit` géré :

```shell
glab orbit local --install
```

`glab` télécharge le binaire, vérifie sa somme de contrôle et le maintient à jour. Vérifiez l'installation :

```shell
glab orbit version
```

## Configurer votre agent d'IA {#set-up-your-ai-agent}

`glab orbit setup` configure les agents de codage IA pour consulter le graphe : il écrit une section gérée dans le fichier d'instructions de chaque agent et installe le skill GitLab Orbit.

```shell
glab orbit setup
```

Exécutez `glab orbit setup --help` pour obtenir la liste complète des options : quels agents configurer, la portée projet ou utilisateur, le graphe local ou distant, et `--remove` pour désinstaller.

Le skill pilote le binaire `orbit` directement. Pour connecter un client MCP au graphe local à la place, consultez [Connect via MCP](mcp.md).

Vous pouvez également [installer le skill GitLab Orbit manuellement](../../ai_coding_agents.md) avec `glab skills install --global orbit`.

## Indexer un dépôt {#index-a-repository}

```shell
glab orbit local index /path/to/your/repo
```

| Indicateur | Objectif |
|------|---------|
| `--threads` | Nombre de fils de discussion de workers. `0` (par défaut) détecte automatiquement le nombre à partir des cœurs CPU. |
| `--stats` | Inclure des statistiques détaillées dans la sortie JSON. |
| `--verbose` | Journalisation détaillée vers stderr. |

## Exécuter du SQL sur le graphe {#run-sql-against-the-graph}

```shell
glab orbit local sql 'SELECT count(*) FROM gl_definition'
echo 'SELECT name FROM gl_definition LIMIT 3' | glab orbit local sql -
```

## Inspecter le schéma {#inspect-the-schema}

`glab orbit local schema` liste toutes les tables et colonnes du graphe DuckDB local :

```shell
glab orbit local schema
```

Transmettez les noms de tables comme arguments positionnels pour limiter la portée de la sortie :

```shell
glab orbit local schema gl_definition              # scoped to one table
glab orbit local schema gl_definition gl_edge      # scoped to two tables
```

| Indicateur | Objectif |
|------|---------|
| `--raw` | Émettre du JSON au lieu de la vue de table par défaut. |
| `--db` | Remplacer le chemin DuckDB. Par défaut : `~/.orbit/graph.duckdb`. |

## Exécuter en tant que serveur MCP {#run-as-an-mcp-server}

Exposer le graphe local à tout agent d'IA compatible MCP :

```shell
glab orbit local mcp serve
```

Il sert `run_sql`, `get_graph_schema` et `index` via le protocole MCP sur `~/.orbit/graph.duckdb`. Consultez [Connect via MCP](mcp.md) pour le guide complet d'intégration des agents.

## Codes de sortie {#exit-codes}

`glab orbit local` renvoie `0` en cas de succès et un code de sortie non nul en cas d'échec, avec les détails sur stderr. Les scripts et les agents peuvent se ramifier selon le succès ou l'échec.

## Facturation {#billing}

GitLab Orbit Local ne consomme pas de GitLab Credits. Tout le traitement est local.
