---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Interrogez directement le graphe GitLab Orbit à l'aide de l'API REST. Référence pour les quatre endpoints avec les exigences d'authentification et des exemples de requêtes."
title: API REST
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

L'API REST GitLab Orbit vous permet d'interroger le graphe directement depuis des scripts, des pipelines CI ou des outils personnalisés.

## Authentification {#authentication}

Tous les endpoints nécessitent un jeton d'accès personnel GitLab avec la portée `read_api`, transmis en tant que jeton Bearer :

```shell
--header "Authorization: Bearer <your_token>"
```

Les résultats sont limités aux entités auxquelles le propriétaire du jeton peut accéder dans GitLab.

## Facturation {#billing}

Les appels à l'API consomment des GitLab Credits de votre abonnement. Chaque appel à `POST /api/v4/orbit/query` consomme des crédits. Les autres endpoints sont gratuits.

## Endpoints {#endpoints}

| Méthode | Point de terminaison | Description |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | Exécuter une requête de graphe |
| `GET` | `/api/v4/orbit/schema` | Récupérer le schéma actuel |
| `GET` | `/api/v4/orbit/status` | Vérifier le statut d'indexation |
| `GET` | `/api/v4/orbit/tools` | Lister les définitions d'outils MCP disponibles |

## Endpoint de requête {#query-endpoint}

Exécuter une requête de graphe à l'aide du DSL de requête GitLab Orbit.

Le corps de la requête contient :

- `query` : l'objet de requête GitLab Orbit.
- `format` : format de réponse optionnel. Utilisez `raw` pour du JSON structuré, ou `llm` pour du texte compact optimisé pour les agents d'IA. Par défaut : `llm`.

Par exemple :

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"query": <query_json>, "format": "raw"}' \
  "https://gitlab.com/api/v4/orbit/query"
```

Consultez la [référence du langage de requête](../queries/query-language.md) pour le DSL complet.

### Exemple de requête {#example-request}

Par exemple, une requête pour trouver les projets ayant le plus d'échecs de pipeline :

Placez le corps de la requête dans `request.json` :

```json orbit-query
{
  "query": {
    "query_type": "aggregation",
    "nodes": [
      {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
      {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
    ],
    "relationships": [
      {"type": "IN_PROJECT", "from": "pl", "to": "p"}
    ],
    "group_by": ["p"],
    "aggregations": [
      {
        "count": "pl",
        "as": "failed_pipelines"
      }
    ],
    "aggregation_sort": "-failed_pipelines",
    "limit": 10
  },
  "format": "raw"
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  "https://gitlab.com/api/v4/orbit/query"
```

Exemple de réponse :

```json
{
  "result": {
    "format_version": "2.0.0",
    "query_type": "aggregation",
    "nodes": [],
    "edges": [],
    "group_columns": [
      {
        "name": "p",
        "kind": "node",
        "node": "p",
        "entity": "Project"
      }
    ],
    "columns": [
      {
        "name": "failed_pipelines",
        "function": "count",
        "target": "pl"
      }
    ],
    "rows": [
      {
        "p": {
          "type": "Project",
          "id": "1",
          "properties": {
            "name": "payments-api",
            "full_path": "my-org/payments-api"
          }
        },
        "failed_pipelines": 47
      }
    ]
  },
  "query_type": "aggregation",
  "raw_query_strings": null,
  "row_count": 1
}
```

## Endpoint de schéma {#schema-endpoint}

Retourne l'ontologie actuelle : tous les types de nœuds, leurs propriétés et types, ainsi que tous les types de relations.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/schema"
```

Utilisez cet endpoint pour découvrir les types d'entités et les propriétés disponibles avant d'écrire des requêtes.

## Endpoint de statut {#status-endpoint}

Retourne le statut d'indexation pour les groupes où GitLab Orbit est activé.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

Exemple de réponse :

```json
{
  "status": "indexed",
  "domains": {
    "sdlc": {"indexed": true, "last_updated": "2026-05-05T14:22:00Z"},
    "code": {"indexed": true, "last_updated": "2026-05-05T14:18:00Z"}
  },
  "projects": {
    "total": 847,
    "indexed": 847
  }
}
```

## Endpoint d'outils {#tools-endpoint}

Retourne les définitions d'outils MCP pour `list_commands` et `invoke_command` dans un format compatible avec les clients MCP.

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/tools"
```
