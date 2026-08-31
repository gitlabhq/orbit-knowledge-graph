---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Construisez et interrogez un graphe de code local avec le binaire GitLab Orbit CLI (orbit). Aucun compte GitLab ni connexion réseau requis.
title: Utiliser GitLab Orbit Local avec le GitLab Orbit CLI (`orbit`)
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

Le GitLab Orbit CLI (`orbit`) construit un graphe de code pour n'importe quel dépôt local et l'interroge contre un fichier DuckDB local. Aucune connexion GitLab requise.

## Installation {#install}

Installez le binaire `orbit` autonome avec le programme d'installation en une ligne :

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

Cela ajoute `orbit` à votre `PATH`. Ouvrez un nouveau terminal, puis vérifiez l'installation :

```shell
orbit help
```

Vous pouvez également installer depuis npm avec `npm install -g @gitlab/orbit`.

Si vous utilisez déjà le GitLab CLI (`glab`), vous pouvez à la place installer un binaire géré avec `glab orbit local --install`. Ce binaire est invoqué avec `glab orbit local <command>` plutôt que `orbit` directement — voir [Utiliser GitLab Orbit Local avec glab](glab.md).

### Compiler depuis les sources {#build-from-source}

Pour contribuer à GitLab Orbit ou exécuter une version non publiée, compilez le binaire vous-même.

Prérequis :

- [Chaîne d'outils Rust](https://rustup.rs/) (stable)
- [`mise`](https://mise.jdx.dev/) pour la gestion des outils

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

Le binaire compilé se trouve à `target/release/orbit`. Ajoutez-le à votre `PATH` ou invoquez-le directement.

## Indexer un dépôt {#index-a-repository}

```shell
orbit index /path/to/your/repo
```

GitLab Orbit analyse le dépôt et écrit un graphe DuckDB dans `~/.orbit/graph.duckdb`. Vous pouvez indexer plusieurs dépôts. Chacun est délimité par l'ID de projet et la branche dans la table de manifeste.

| Indicateur | Objectif |
|------|---------|
| `--threads` | Nombre de fils de discussion de workers. `0` (par défaut) détecte automatiquement le nombre à partir des cœurs CPU. |
| `--stats` | Inclure des statistiques détaillées dans la sortie JSON. |
| `--verbose` | Journalisation détaillée vers stderr. |
| `--db` | Remplace le chemin du fichier DuckDB (par défaut : `~/.orbit/graph.duckdb`). |

## Inspecter le schéma {#inspect-the-schema}

`orbit schema` liste toutes les tables et colonnes du graphe DuckDB local :

```shell
orbit schema
```

Transmettez les noms de tables comme arguments positionnels pour limiter la portée de la sortie :

```shell
orbit schema gl_definition              # scoped to one table
orbit schema gl_definition gl_edge      # scoped to two tables
```

| Indicateur | Objectif |
|------|---------|
| `--raw` | Émettre du JSON au lieu de la vue de table par défaut. |
| `--db` | Remplacer le chemin DuckDB. Par défaut : `~/.orbit/graph.duckdb`. |

## Exécuter du SQL contre le graphe local {#run-sql-against-the-local-graph}

```shell
orbit sql 'SELECT count(*) FROM gl_definition'
orbit sql -F json 'SELECT name FROM gl_definition LIMIT 3'
echo 'SELECT 1+1' | orbit sql -
orbit sql --file query.sql
```

| Indicateur | Objectif |
|------|---------|
| `-F`, `--format` | `table` (par défaut), `json`, `ndjson`, ou `csv`. |
| `-f`, `--file` | Lit le SQL depuis un fichier. |
| `--db` | Remplacer le chemin DuckDB. Par défaut : `~/.orbit/graph.duckdb`. |

## Lister les dépôts indexés {#list-indexed-repositories}

Le graphe peut contenir plus d'un dépôt. Pour voir ce qu'il contient, exécutez :

```shell
orbit list
orbit list -F json
```

Chaque ligne indique le chemin du dépôt, la branche, le commit, le statut d'indexation, la dernière date d'indexation, et un message d'erreur lorsque le statut est `error` :

```plaintext
+------------------------+--------+------------+---------+---------------------+---------------+
| repo_path              | branch | commit_sha | status  | last_indexed_at     | error_message |
+------------------------+--------+------------+---------+---------------------+---------------+
| /home/dev/workspace/kg | main   | 9606ae8... | indexed | 2026-05-18 10:14:02 |               |
| /tmp/cli-test          | main   | 654f3a6... | indexed | 2026-05-18 10:13:55 |               |
+------------------------+--------+------------+---------+---------------------+---------------+
```

Un dépôt dont l'indexation échoue est enregistré avec `status = error` et une raison dans `error_message`, de sorte qu'un dépôt en échec ou non indexable reste visible ici au lieu de disparaître silencieusement.

| Indicateur | Objectif |
|------|---------|
| `-F`, `--format` | `table` (par défaut), `json`, `ndjson`, ou `csv`. |
| `--db` | Remplacer le chemin DuckDB. Par défaut : `~/.orbit/graph.duckdb`. |

Si rien n'a encore été indexé, `orbit list` se termine avec `0`. La vue tableau n'affiche rien ; les formats structurés émettent une sortie vide valide (`[]` pour `json`, aucun enregistrement pour `ndjson`) afin que les pipelines tels que `orbit list -F json | jq` continuent de fonctionner.

## Exécuter en tant que serveur MCP {#run-as-an-mcp-server}

Exposez le graphe local à n'importe quel agent d'IA compatible MCP via stdio :

```shell
orbit mcp serve
```

Il sert `run_sql`, `get_graph_schema`, et `index` contre `~/.orbit/graph.duckdb`. Consultez [Se connecter via MCP](mcp.md) pour la configuration par client.

## Configurer votre assistant IA {#set-up-your-ai-assistant}

`orbit setup` configure un assistant de codage IA pour consulter le graphe avant d'utiliser grep. Indiquez les assistants que vous souhaitez configurer :

```shell
orbit setup claude
```

Les assistants pris en charge sont `claude`, `codex`, `opencode`, et `pi`. Par défaut, les instructions pointent vers le graphe GitLab Orbit distant. Pour le faire pointer vers votre graphe local, passez `--local` :

```shell
orbit setup claude --local
```

### Ce que cette commande modifie {#what-it-changes}

Cette commande modifie les fichiers qui vous appartiennent. Elle ne s'exécute jamais seule, uniquement lorsque vous l'invoquez.

Pour chaque assistant que vous nommez, `orbit setup` :

- Ajoute un bloc au fichier d'instructions de cet assistant, tel que `CLAUDE.md` ou `AGENTS.md`. Le bloc se trouve entre les marqueurs `<!-- orbit:setup:begin -->` et `<!-- orbit:setup:end -->`, et tout ce qui se trouve en dehors de ces marqueurs est laissé intact. Exécuter la commande à nouveau remplace le bloc en place au lieu d'en ajouter une seconde copie.
- Ajoute des entrées à la configuration JSON de cet assistant, lorsque l'assistant le prend en charge. Pour Claude Code, il s'agit d'un hook `PreToolUse` dans `settings.json` ; pour OpenCode, il s'agit d'un fichier de plugin et de son enregistrement. Les entrées portent un marqueur `orbit`, et seules les entrées marquées sont jamais remplacées ou supprimées.

Par défaut, il écrit dans votre configuration globale utilisateur, comme `~/.claude/CLAUDE.md`. Passez `--project` pour écrire dans le projet actuel, ou `--dir <path>` pour cibler un répertoire de projet spécifique.

Avant que `orbit setup` ne modifie un fichier existant pour la première fois, il en copie l'original vers `<name>.orbit-backup` à côté de lui. Restaurez cette copie manuellement si vous souhaitez récupérer l'original. Les sauvegardes existantes ne sont jamais écrasées, de sorte que la copie conserve toujours la version antérieure à `orbit`.

Utilisez `--project` avec précaution : les fichiers d'instructions de projet sont généralement validés dans le système de contrôle de version, de sorte que la modification apparaît dans `git status` et peut atteindre vos coéquipiers. La portée globale utilisateur, la valeur par défaut, ne vous affecte que vous.

### Supprimer la configuration {#remove-it}

Pour annuler les modifications, exécutez :

```shell
orbit setup claude --remove
```

Cette opération supprime le bloc délimité par des marqueurs et les entrées JSON marquées, et laisse le reste de chaque fichier intact. Si un fichier ne contenait que des entrées `orbit`, il est supprimé. Omettez les noms d'assistants pour supprimer la configuration pour tous. Les fichiers de sauvegarde ne sont pas supprimés.

Si vous préférez que `orbit setup` ne touche pas à vos fichiers, ignorez-le et ajoutez manuellement le même bloc d'instructions et les mêmes hooks.

## Stockage {#storage}

Le graphe est stocké à `~/.orbit/graph.duckdb`. Plusieurs dépôts partagent la même base de données. Supprimez le fichier pour recommencer.

## Configurer le CLI {#configure-the-cli}

`orbit config` lit et écrit les paramètres persistés dans `~/.orbit/settings.json`. Un paramètre sauvegardé s'applique à toutes les exécutions ultérieures.

```shell
orbit config list                          # all settings and their saved values
orbit config get telemetry.enabled         # one setting
orbit config set telemetry.enabled false   # save a setting
```

| Paramètre | Valeurs | Valeur par défaut | Objectif |
|---------|--------|---------|---------|
| `telemetry.enabled` | `true`, `false` | `true` | Indique si le CLI envoie des données de télémétrie d'utilisation. |

## Télémétrie {#telemetry}

Le CLI envoie des événements d'utilisation au service d'analyse produit de GitLab afin que l'équipe puisse voir comment GitLab Orbit est utilisé. Chaque événement enregistre uniquement la commande exécutée, rien de plus : aucun contenu de dépôt, chemin de fichier ou texte de requête n'est envoyé. La télémétrie est activée par défaut.

Désactivez-la avec un paramètre sauvegardé, ou avec la variable d'environnement en CI :

```shell
orbit config set telemetry.enabled false   # persists for every run
export ORBIT_TELEMETRY_ENABLED=false        # for CI or one shell
```

La variable d'environnement remplace le paramètre sauvegardé.

| Variable | Objectif |
|----------|---------|
| `ORBIT_TELEMETRY_ENABLED` | `false` désactive la télémétrie, `true` l'active. Remplace le paramètre sauvegardé. |
| `ORBIT_TELEMETRY_COLLECTOR_URL` | Envoie des événements à un collecteur différent, à des fins de test. Par défaut, utilise le collecteur GitLab. |

## Facturation {#billing}

GitLab Orbit Local ne consomme pas de GitLab Credits. Tout le traitement est local.

## Que faire ensuite {#what-to-try-next}

- [Se connecter via MCP](mcp.md) \- connectez Claude Code, Codex et d'autres agents au graphe local.
- [Utiliser GitLab Orbit Local avec glab](glab.md) \- appelez le CLI via `glab orbit local`.
- [Référence du schéma](../../remote/schema.md) \- types de nœuds et propriétés disponibles.
- [Cookbook](../../remote/cookbook.md) \- requêtes prêtes à l'emploi pour les cas d'usage courants.
