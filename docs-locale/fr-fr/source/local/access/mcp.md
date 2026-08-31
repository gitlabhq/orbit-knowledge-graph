---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Connectez des agents d'IA à votre graphe local avec le serveur MCP local GitLab Orbit."
title: Serveur MCP local GitLab Orbit
---

{{< details >}}

- Édition : Gratuite, GitLab Premium, GitLab Ultimate
- Offre : GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Statut : version expérimentale

{{< /details >}}

{{< history >}}

- [Introduit](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643) dans GitLab 19.2 en tant que [version expérimentale](https://docs.gitlab.com/policy/development_stages_support/#experiment).

{{< /history >}}

Avec le serveur [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) local GitLab Orbit, vous pouvez connecter de manière sécurisée des outils et applications d'IA à votre graphe local. Les assistants d'IA tels que Claude Code, Codex, Cursor et OpenCode peuvent alors accéder à votre graphe et y écrire des requêtes SQL.

Le serveur MCP local GitLab Orbit est sans état (stateless). Cela signifie que le serveur présente les caractéristiques suivantes :

- Il interroge votre graphe local, et non une instance GitLab.
- Il ne met pas en cache les résultats ni ne conserve l'historique des requêtes.
- Il peut répondre à plusieurs clients de manière indépendante.

## Prérequis {#prerequisites}

- Installez l'un des outils suivants :
  - L'[interface de ligne de commande GitLab Orbit](./cli.md) (`orbit`)
  - L'[interface de ligne de commande GitLab](./glab.md) (`glab orbit`)

## Connecter un client au serveur MCP local GitLab Orbit {#connect-a-client-to-the-gitlab-orbit-local-mcp-server}

Le serveur MCP local GitLab Orbit prend en charge le transport stdio. Les arguments et les commandes varient selon le client MCP et votre environnement local.

### Connecter Claude Code {#connect-claude-code}

Claude Code stocke la configuration du serveur MCP dans l'une des trois portées suivantes. Choisissez la portée qui correspond à votre cas d'utilisation.

| Portée | Disponibilité | Stocké dans |
| ----- | ------------ | --------- |
| `local` (par défaut) | Vous uniquement, dans le projet actuel | `~/.claude.json` |
| `user` | Vous uniquement, dans tous les projets | `~/.claude.json` |
| `project` | Toute personne qui extrait le dépôt | `.mcp.json` à la racine du dépôt |

Pour ajouter le serveur MCP au projet actuel, exécutez :

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

Pour ajouter le serveur à l'ensemble de vos projets, exécutez :

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local --scope user -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local --scope user -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

Pour ajouter le serveur pour toute personne qui extrait le dépôt, exécutez :

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local --scope project -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local --scope project -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

Vous pouvez également modifier directement le fichier `.mcp.json` :

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

Pour confirmer que le serveur MCP est connecté, exécutez :

```shell
claude mcp list
```

Une fois connecté, [configurez votre assistant d'IA](./cli.md).

> [!note]
Pour les commandes dont la portée est définie au niveau du projet, Claude Code demande une approbation avant de créer le fichier `mcp.json`. Jusqu'à ce que vous approuviez, `claude mcp list` affiche le serveur comme `Pending approval`.

### Connecter Codex {#connect-codex}

Codex stocke la configuration du serveur MCP dans `~/.codex/config.toml`. Codex configure toujours le serveur pour l'ensemble de vos projets.

Pour vous connecter à Codex, exécutez :

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
codex mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
codex mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

Pour confirmer que le serveur est enregistré, exécutez :

```shell
codex mcp list
```

Une fois connecté, [configurez votre assistant d'IA](./cli.md).

### Connecter Cursor {#connect-cursor}

Cursor lit la configuration du serveur MCP depuis un fichier `mcp.json`. Choisissez la portée qui correspond à l'étendue souhaitée pour la disponibilité du serveur.

| Portée | Disponibilité | Stocké dans |
| ----- | ------------ | --------- |
| Projet | Vous uniquement, dans le projet actuel | `.cursor/mcp.json` à la racine du dépôt |
| Global | Vous uniquement, dans tous les projets | `~/.cursor/mcp.json` |

Pour vous connecter à Cursor, créez ou modifiez le fichier `mcp.json` correspondant à la portée souhaitée :

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

Pour confirmer que le serveur est connecté, exécutez :

```shell
agent mcp list
```

### Connecter OpenCode {#connect-opencode}

Ajoutez la configuration du serveur MCP dans `opencode.json` à la racine du dépôt, ou dans `~/.config/opencode/opencode.json` pour l'utiliser dans l'ensemble de vos projets :

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["orbit", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["glab", "orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

Pour confirmer que le serveur est connecté, exécutez :

```shell
opencode mcp list
```

Une fois connecté, [configurez votre assistant d'IA](./cli.md).

### Connecter d'autres clients MCP {#connect-other-mcp-clients}

Le serveur MCP local GitLab Orbit n'expose pas d'URL de connexion. Les clients qui nécessitent une URL ne peuvent pas s'y connecter.

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

Pour vous connecter avec l'interface de ligne de commande GitLab Orbit, modifiez le fichier de configuration MCP de votre client :

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

Pour vous connecter avec l'interface de ligne de commande GitLab :

1. Exécutez `glab orbit local --install`. Cette commande télécharge le binaire `orbit`.
1. Ensuite, modifiez le fichier de configuration MCP de votre client :

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

Pour confirmer que le serveur est connecté, vérifiez que votre client répertorie les outils `run_sql`, `get_graph_schema` et `index`.

## Outils MCP {#mcp-tools}

Le serveur MCP local GitLab Orbit fournit un ensemble d'outils permettant d'interagir avec votre graphe local.

### `index` {#index}

Indexe un dépôt, ou un répertoire de dépôts, dans le graphe local.

Exemple :

```plaintext
Index my checked out project.
```

### `get_graph_schema` {#get_graph_schema}

Récupère le schéma. Inclut les noms de tables, les colonnes et les types de données présents dans le graphe local.

Exemple :

```plaintext
Use the `get_graph_schema` tool to show me what tables are in my local graph.
```

### `run_sql` {#run_sql}

Exécute des requêtes SQL en lecture seule sur le graphe local. Prend un tableau d'instructions et retourne un tableau de lignes JSON par instruction, au même index.

Un seul appel `run_sql` retourne au maximum environ 1 Mo, en comptant l'ensemble des instructions de l'appel. Les résultats plus volumineux échouent et l'agent d'IA réessaie avec une requête plus restreinte. Si l'agent d'IA ne parvient pas à récupérer les données, demandez-lui de retourner moins de résultats.

Exemple :

```plaintext
Show me the most used imports in this repository.
```
