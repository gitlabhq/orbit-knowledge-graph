---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Résoudre les erreurs courantes dans GitLab Orbit Local et GitLab Orbit Remote.
title: Résoudre les problèmes de GitLab Orbit
---

{{< details >}}

- Édition : Gratuite, GitLab Premium, GitLab Ultimate
- Offre : GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Statut : version bêta

{{< /details >}}

{{< history >}}

- [Introduction](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/661) dans GitLab 19.1.

{{< /history >}}

Utilisez cette page pour résoudre les erreurs que vous pourriez rencontrer avec [GitLab Orbit Local](local/_index.md) ou [GitLab Orbit Remote](remote/_index.md).

## GitLab Orbit Local {#gitlab-orbit-local}

Les erreurs de GitLab Orbit Local se produisent lors de l'exécution du binaire `orbit` directement ou via `glab orbit local`.

### `no local graph found` {#no-local-graph-found}

**Symptoms:**

```plaintext
Error: no local graph found at ~/.orbit/graph.duckdb. Run `orbit index` first.
```

**Cause:** Le dépôt n'a pas encore été indexé, ou le chemin `--db` que vous avez spécifié n'existe pas. Dans les versions antérieures de GitLab Orbit Local, cette erreur était signalée comme `Table 'Definition' does not exist`.

**Résolution :** Indexez d'abord le dépôt :

```shell
glab orbit local index /path/to/your/repo
```

### `IO Error: Could not set lock on file` {#io-error-could-not-set-lock-on-file}

**Symptoms:** Une commande semble se mettre brièvement en pause, puis échoue avec une erreur contenant `Could not set lock on file`.

**Cause:** Un autre processus `orbit` est déjà en cours d'exécution et détient le verrou d'écriture DuckDB. GitLab Orbit effectue des nouvelles tentatives automatiquement avec un backoff exponentiel, mais échoue si le verrou n'est pas libéré dans la fenêtre de nouvelle tentative.

**Résolution :** Attendez que l'autre processus se termine, ou arrêtez-le :

```shell
pkill orbit
```

Ensuite, réessayez votre commande.

### `list_contains source_tags` {#list_contains-source_tags}

**Symptoms:** Une requête échoue avec une erreur contenant `list_contains source_tags`.

**Cause:** Un bug connu déclenché par certaines combinaisons de filtres incluant la propriété `source_tags`.

**Résolution :** Supprimez tout filtre `source_tags` de votre requête et réessayez.

### `error: unrecognized subcommand 'mcp'` {#error-unrecognized-subcommand-mcp}

**Symptoms:**

```plaintext
error: unrecognized subcommand 'mcp'
```

**Cause:** La sous-commande `orbit mcp serve` n'est pas encore implémentée. La prise en charge MCP pour GitLab Orbit Local est sur le roadmap, mais n'est pas disponible dans la release actuelle.

**Résolution :** Utilisez l'une des [méthodes d'accès prises en charge](local/_index.md).

## GitLab Orbit Remote {#gitlab-orbit-remote}

Les erreurs de GitLab Orbit Remote se produisent lors de l'exécution des commandes `glab orbit remote`. GitLab Orbit Remote nécessite GitLab Premium ou GitLab Ultimate et que le feature flag `knowledge_graph` soit activé sur votre instance.

### Code de sortie 2 {#exit-code-2}

**Symptoms:** Les commandes `glab orbit remote` se terminent avec le code 2.

**Cause:** Le feature flag `knowledge_graph` n'est pas activé pour votre espace de nommage ou votre instance.

**Résolution :** Contactez votre administrateur GitLab pour activer le feature flag `knowledge_graph` pour votre espace de nommage.

### Code de sortie 3 {#exit-code-3}

**Symptoms:** Les commandes `glab orbit remote` se terminent avec le code 3.

**Cause:** Vous n'êtes pas authentifié avec le CLI GitLab.

**Résolution :** Connectez-vous :

```shell
glab auth login
```

### `insufficient_scope` sur le point de terminaison MCP {#insufficient_scope-on-the-mcp-endpoint}

**Symptoms:** La connexion au point de terminaison MCP de GitLab Orbit échoue avec `insufficient_scope`.

**Cause:** Le jeton d'accès personnel ou le jeton OAuth n'inclut pas la portée `mcp_orbit`. La portée `read_api` seule n'est pas suffisante pour le transport MCP.

**Résolution :** Créez un nouveau jeton avec la portée `mcp_orbit`, ou ré-authentifiez-vous pour accorder la portée supplémentaire.
