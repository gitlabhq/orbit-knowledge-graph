---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Connectez Claude Code, Codex ou tout agent d'IA compatible MCP à GitLab Orbit en utilisant les deux outils MCP list_commands et invoke_command."
title: Connexion via MCP
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

GitLab Orbit expose deux outils MCP qui permettent à tout agent d'IA compatible MCP de découvrir et d'invoquer des commandes GitLab Orbit sur votre graphe. Utilisez-les avec Claude Code, OpenAI Codex ou tout autre outil prenant en charge le Model Context Protocol.

## Prérequis {#prerequisites}

- GitLab Orbit est [activé sur votre groupe](../getting-started.md).
- Vous êtes authentifié auprès de GitLab. Exécutez `glab auth login` (utilise OAuth par défaut ; les jetons d'accès personnels avec la portée `read_api` fonctionnent également).
- Votre authentification a accès aux groupes que vous souhaitez interroger.
- Si votre client MCP se connecte directement via HTTP natif (et non via `mcp-remote`), sa requête OAuth doit inclure la portée `mcp_orbit`. Consultez l'exemple Gemini CLI ci-dessous.

## Outils MCP {#mcp-tools}

| Outil | Description |
|------|-------------|
| `list_commands` | Lister les commandes GitLab Orbit disponibles avec leurs descriptions et leurs schémas d'entrée. |
| `invoke_command` | Invoquer une commande par son nom avec des paramètres. Retourne des résultats typés. |

Commandes disponibles via `invoke_command` :

| Commande | Description |
|---------|-------------|
| `query_graph` | Exécuter une requête de graphe à l'aide du DSL de requête GitLab Orbit. |
| `get_graph_schema` | Récupérer le schéma actuel : tous les types de nœuds, leurs propriétés et les types de relations. |
| `get_query_dsl` | Retourner la grammaire DSL JSON `query_graph` et sa version. |
| `get_response_format` | Retourner le schéma JSON de réponse `query_graph` et sa version. |

## Connecter votre client MCP {#connect-your-mcp-client}

Configurez votre client MCP pour pointer vers `https://gitlab.com/api/v4/orbit/mcp`.

**Claude Code** prend en charge le point de terminaison GitLab Orbit via le transport HTTP intégré. Enregistrez-le avec une seule commande :

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

Le premier appel à `list_commands` ou `invoke_command` ouvre votre navigateur pour vous authentifier auprès de GitLab. Aucune modification du fichier de configuration JSON n'est requise.

> [!note]
Claude Code se connecte directement via HTTP. N'utilisez pas `npx mcp-remote` avec Claude Code — cela enveloppe le point de terminaison dans un processus stdio qui entre en conflit avec le transport intégré et provoque des erreurs « Failed to connect ». Utilisez plutôt la commande `claude mcp add --transport http` indiquée ci-dessus.

Certains clients ne prennent en charge que les serveurs MCP stdio locaux. Pour ceux-ci, [`mcp-remote`](https://www.npmjs.com/package/mcp-remote) encapsule le point de terminaison GitLab Orbit en tant que commande locale.

**Cursor, Codex, and other JSON-config clients** — ajoutez ceci à la configuration MCP de votre agent :

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "command": "npx",
      "args": ["mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

**opencode** — ajoutez ceci à `~/.config/opencode/opencode.json` :

```json
{
  "mcp": {
    "gitlab-orbit": {
      "type": "local",
      "command": ["npx", "mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

> [!note]
opencode nécessite `"type": "local"` et place la commande et les arguments ensemble dans un seul tableau. L'utilisation d'un champ `args` séparé ou l'omission de `type` provoque une `ConfigInvalidError`.

**Gemini CLI** — prend en charge le point de terminaison GitLab Orbit via le transport HTTP natif. Ajoutez ceci à `~/.gemini/settings.json` :

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "url": "https://gitlab.com/api/v4/orbit/mcp",
      "type": "http",
      "timeout": 5000,
      "oauth": {
        "enabled": true,
        "scopes": ["mcp_orbit"]
      }
    }
  }
}
```

Vous pouvez également générer cela avec `gemini mcp add gitlab-orbit https://gitlab.com/api/v4/orbit/mcp -t http -s user`, puis ajouter le bloc `oauth.scopes` manuellement.

> [!note]
Les clients MCP HTTP natifs doivent demander explicitement la portée OAuth `mcp_orbit`. Sans `oauth.scopes: ["mcp_orbit"]`, l'authentification échoue même si vous êtes déjà connecté à GitLab ailleurs. Si un client utilisant le transport HTTP natif ne parvient pas à s'authentifier, ajoutez cette portée à la configuration de son serveur MCP.
>
> Les configurations Gemini CLI plus anciennes peuvent utiliser `httpUrl` au lieu de `url` + `type: "http"`. `httpUrl` fonctionne toujours, mais est déprécié ; utilisez `url` + `type` pour les nouvelles configurations.

**Antigravity** — l'IDE et le CLI Antigravity lisent la même configuration MCP à l'emplacement `~/.gemini/config/mcp_config.json`. Antigravity n'exécute pas encore le flow OAuth MCP pour les serveurs distants (une entrée native `serverUrl` envoie `initialize` sans jeton et échoue avec `Unauthorized`), donc encapsulez le point de terminaison avec `mcp-remote` :

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "command": "npx",
      "args": ["mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

> [!note]
Aucun bloc `oauth` n'est nécessaire ici. `mcp-remote` découvre la portée `mcp_orbit` à partir des métadonnées OAuth du point de terminaison et ouvre votre navigateur pour autoriser l'accès lors de la première utilisation.

L'authentification utilise votre session `glab auth login` existante — aucun jeton à copier ou à coller. Clients pris en charge : Claude Code, OpenCode, Cursor, Codex, Gemini CLI, Antigravity.

> [!note]
Une sous-commande `glab orbit setup` planifiée installera la compétence GitLab Orbit et écrira cette configuration MCP en une seule étape. En attendant sa publication, configurez votre client MCP manuellement comme indiqué ci-dessus.

Vous pouvez également [installer manuellement la compétence GitLab Orbit](../../ai_coding_agents.md) dès aujourd'hui pour fournir à l'agent d'IA des recettes de requêtes, des conseils DSL et des informations de dépannage.

### Tester {#test-it}

Dans votre agent d'IA, demandez :

> « Utilisez GitLab Orbit pour lister les 5 projets mis à jour le plus récemment dans mon groupe. »

Vous devriez obtenir des résultats typés avec les noms et les chemins des projets. Si c'est le cas, vous êtes connecté. Sinon, exécutez `glab auth status` pour confirmer que vous êtes authentifié, et vérifiez que GitLab Orbit est activé sur au moins un de vos groupes.

## Facturation {#billing}

Les requêtes via MCP consomment des GitLab Credits. Chaque appel `invoke_command` qui exécute `query_graph` utilise des crédits de votre abonnement GitLab. `list_commands` ainsi que les commandes `get_graph_schema`, `get_query_dsl` et `get_response_format` sont gratuits.

## Utilisation des outils {#using-the-tools}

Une fois connecté, demandez à votre agent d'IA d'utiliser directement les outils GitLab Orbit :

Découvrir les commandes et le schéma :
> « Utilisez `list_commands` pour me montrer les commandes GitLab Orbit disponibles, puis exécutez la commande `get_graph_schema` pour me montrer les types de nœuds indexés par GitLab Orbit. »

Exécuter une requête :
> « Utilisez la commande `query_graph` pour trouver les 10 projets ayant le plus de merge requests ouvertes dans votre groupe. »

Analyse du rayon d'impact :
> « Utilisez GitLab Orbit pour trouver tous les fichiers de ce projet qui importent `AuthService` directement ou transitivement. »

Intégration :
> « Utilisez GitLab Orbit pour cartographier les services clés de ce groupe, leurs langages et les projets dont ils dépendent. »

L'agent d'IA compose le DSL de requête JSON et invoque la commande `query_graph` en votre nom. Vous pouvez également transmettre directement des requêtes JSON brutes si vous souhaitez un contrôle précis sur les résultats.

## Exemple : appel manuel d'invoke_command pour query_graph {#example-manual-invoke_command-call-for-query_graph}

Transmettez la requête ci-dessous en tant que `invoke_command` avec `{"command_name": "query_graph", "parameters": {"query": ...}}` :

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": ["p"],
  "aggregations": [
    { "count": "mr", "as": "open_mrs" }
  ],
  "aggregation_sort": "-open_mrs",
  "limit": 10
}
```

## Dépannage {#troubleshooting}

### « Failed to connect » dans Claude Code {#failed-to-connect-in-claude-code}

Claude Code dispose d'une prise en charge MCP HTTP intégrée. Si vous avez enregistré GitLab Orbit avec `npx mcp-remote` au lieu de `--transport http`, le wrapper `mcp-remote` crée un processus stdio local qui entre en conflit avec le transport natif.

Pour résoudre ce problème, supprimez l'enregistrement défaillant et rajoutez-le avec le transport HTTP :

```shell
claude mcp remove gitlab-orbit
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

### « Needs authentication » lors de la première utilisation {#needs-authentication-on-first-use}

C'est le comportement attendu. Le premier appel à `list_commands` ou `invoke_command` ouvre votre navigateur pour finaliser l'authentification OAuth auprès de GitLab. Si le flow du navigateur ne se déclenche pas, vérifiez votre session :

```shell
glab auth status
```

Si votre session a expiré, réauthentifiez-vous :

```shell
glab auth login
```

### Erreurs de requête après la connexion {#query-errors-after-connecting}

Pour les erreurs survenant lors des requêtes (échecs de validation, résultats vides, limites de débit), consultez la [documentation sur la compétence GitLab Orbit](../../ai_coding_agents.md), qui inclut des conseils DSL, des recettes de requêtes et des diagnostics de codes de sortie. Installez la compétence pour obtenir des conseils en ligne :

```shell
glab skills install --global orbit
```
