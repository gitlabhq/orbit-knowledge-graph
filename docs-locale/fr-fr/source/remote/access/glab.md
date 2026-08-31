---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Interrogez GitLab Orbit depuis la ligne de commande avec glab orbit remote, disponible dans glab 1.94 ou version ultérieure. L'assistant glab orbit setup est prévu pour une future release de glab."
title: Utiliser GitLab Orbit avec la CLI GitLab (`glab`)
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

<!-- -->

> [!disclaimer]

La [CLI GitLab (`glab`)](https://docs.gitlab.com/cli/) est la méthode recommandée pour configurer et interroger GitLab Orbit depuis la ligne de commande.

`glab orbit` exécute le binaire géré `orbit`. Il transmet chaque commande au binaire, que `glab` télécharge, vérifie et maintient à jour pour vous. Exécutez `glab orbit remote <command> --help` pour consulter la référence des commandes propre au binaire.

- `glab orbit remote` : interroge l'API REST GitLab Orbit Remote. `glab` injecte automatiquement vos identifiants GitLab. Disponible dans `glab` 1.94 ou version ultérieure.
- `glab orbit setup` : intégration guidée qui installe la compétence GitLab Orbit et configure votre agent d'IA.

## Prérequis {#prerequisites}

- GitLab Orbit est [activé sur votre groupe](../getting-started.md).
- `glab` est installé et authentifié :

  ```shell
  glab auth login
  ```

- Votre utilisateur a accès à au moins un groupe principal avec GitLab Orbit activé.

## Configurer votre agent d'IA {#set-up-your-ai-agent}

`glab orbit setup` configure les agents de codage IA (Claude Code, OpenCode, Cursor, Codex, Gemini CLI) pour consulter le graphe et installe la compétence GitLab Orbit :

```shell
glab orbit setup
```

Pour connecter un client MCP à la place, [configurez-le manuellement](mcp.md#connect-your-mcp-client).

## Interroger GitLab Orbit depuis la ligne de commande {#query-gitlab-orbit-from-the-command-line}

Utilisez `glab orbit remote` pour appeler directement l'API GitLab Orbit Remote. Utile pour les scripts, le débogage et l'exploration du schéma avant d'écrire des requêtes. Nécessite `glab` 1.94 ou une version ultérieure.

`glab` résout vos identifiants et les transmet au binaire, ce qui dispense de toute étape d'authentification supplémentaire. Utilisez `--hostname` pour cibler une instance GitLab spécifique, et `--yes` pour ignorer la confirmation d'exécution unique dans les scripts.

| Sous-commande | Point de terminaison | Objectif |
|------------|----------|---------|
| `glab orbit remote status` | `GET orbit/status` | Santé du cluster. |
| `glab orbit remote schema [node...]` | `GET orbit/schema` | Ontologie du graphe. Les arguments positionnels développent des nœuds spécifiques. |
| `glab orbit remote dsl` | `GET orbit/schema/dsl` | Schéma JSON DSL de requête. La source de référence pour la structure du corps de la requête. |
| `glab orbit remote tools` | `GET orbit/tools` | Manifeste d'outil MCP avec le schéma JSON DSL complet. |
| `glab orbit remote query [file\|-]` | `POST orbit/query` | Exécuter une requête depuis un fichier ou stdin. |
| `glab orbit remote graph-status` | `GET orbit/graph_status` | Progression de l'indexation pour un espace de nommage, un projet ou un chemin complet. |

### Explorer le schéma {#discover-the-schema}

```shell
glab orbit remote status
glab orbit remote schema
glab orbit remote schema MergeRequest Project
glab orbit remote dsl
glab orbit remote tools
```

### Exécuter une requête {#run-a-query}

Remplacez `your-group` par le chemin de votre propre groupe. Cette requête retourne les cinq premiers projets de ce groupe :

Placez le corps de la requête dans `query.json` :

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 5
  }
}
```

```shell
glab orbit remote query query.json
```

L'indicateur `--response-format` correspond au `response_format` du corps :

- `--response-format llm` : texte compact optimisé pour la consommation par les agents d'IA.
- `--response-format raw` : JSON structuré, adapté à la transmission via un pipe vers `jq`.

Si `--response-format` n'est pas défini, le `response_format` du corps prévaut, avec `llm` comme solution de repli finale.

### Vérifier la progression de l'indexation {#check-indexing-progress}

Transmettez exactement un indicateur de portée :

```shell
glab orbit remote graph-status --full-path your-group/your-project
glab orbit remote graph-status --namespace-id 24
glab orbit remote graph-status --project-id 2
```

## Codes de sortie {#exit-codes}

`glab orbit remote` associe les erreurs HTTP à des codes de sortie stables afin que les scripts et les agents puissent les traiter en différenciation sans analyser stderr.

| Statut | Code de sortie | Signification |
|--------|-----------|---------|
| `200` | `0` | Succès. |
| `404` | `2` | Le feature flag `knowledge_graph` est désactivé, ou erreur de saisie du chemin. |
| `401` | `3` | Jeton manquant ou expiré. |
| `403` | `4` | Aucun espace de nommage avec le graphe de connaissances activé n'est disponible. |
| `429` | `5` | Limite de débit atteinte. Inspectez `Retry-After` et patientez avant de réessayer. |
| Autre | `1` | Erreur non structurée. Le corps de la réponse, s'il existe, est inclus. |

## Facturation {#billing}

`glab orbit remote query` consomme des GitLab Credits de la même manière que les requêtes MCP. Les appels `status`, `schema`, `tools` et `graph-status` sont gratuits.
