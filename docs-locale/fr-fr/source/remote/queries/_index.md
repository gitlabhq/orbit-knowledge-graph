---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Interrogez le graphe GitLab Orbit pour trouver des données, du code et des relations GitLab."
title: Requêtes
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

Les requêtes GitLab Orbit sont des objets JSON qui décrivent des opérations sur le graphe. Une requête peut récupérer un type d'objet, parcourir les relations entre objets, compter les objets correspondants, trouver un chemin ou demander les voisins d'un nœud.

Les requêtes s'exécutent via l'autorisation GitLab. La réponse contient uniquement les données que l'utilisateur actuel peut lire dans GitLab.

## Choisir une forme de requête {#choose-a-query-shape}

| Cas d'utilisation | Forme de requête |
|----------|-------------|
| Récupérer les nœuds correspondants d'un type d'entité | `traversal` à nœud unique |
| Suivre les relations entre des types d'entités connus | `traversal` multi-nœuds |
| Compter, additionner, calculer la moyenne ou regrouper les résultats du graphe | `aggregation` |
| Trouver un chemin entre deux points de terminaison délimités | `path_finding` |
| Demander ce qui est connecté à un nœud délimité | `neighbors` |

Le `traversal` à nœud unique est la structure de recherche. GitLab Orbit ne dispose pas d'un type de requête `search` distinct.

## Exemple : récupérer le diff d'une merge request {#example-fetch-a-merge-request-diff}

Utilisez la colonne `diff` sur `MergeRequest` pour récupérer le diff unifié complet d'une merge request. Demandez explicitement les colonnes virtuelles par leur nom.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state", "diff"]
  }],
  "limit": 1
}
```

Le contenu du diff d'une merge request peut prendre plusieurs formes :

| Entité | Colonne | Ce qu'elle retourne |
|--------|--------|-----------------|
| `MergeRequest` | `diff` | Diff unifié complet de la merge request |
| `MergeRequestDiff` | `patch` | Patch complet pour un instantané de diff |
| `MergeRequestDiffFile` | `diff` | Texte de diff unifié par fichier |
| `File` | `content` | Texte brut du fichier source |
| `Definition` | `content` | Texte source pour une définition indexée |

La colonne `content` est destinée aux nœuds de code source. Pour le texte de diff d'une merge request, utilisez `diff` ou `patch`, selon l'entité.

## Exemple : récupérer les instantanés de diff et les fichiers modifiés {#example-fetch-diff-snapshots-and-changed-files}

Utilisez `HAS_DIFF` pour passer d'une merge request à ses instantanés de diff, puis `HAS_FILE` pour récupérer les fichiers dans ces instantanés.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    },
    {
      "id": "snapshot",
      "entity": "MergeRequestDiff",
      "columns": ["id", "state", "patch"]
    },
    {
      "id": "file",
      "entity": "MergeRequestDiffFile",
      "columns": ["new_path", "old_path", "too_large", "diff"]
    }
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "snapshot"},
    {"type": "HAS_FILE", "from": "snapshot", "to": "file"}
  ],
  "limit": 20
}
```

`MergeRequestDiffFile.diff` est `null` lorsque `too_large` est `true`.

## Exemple : récupérer le contenu d'un fichier source {#example-fetch-source-file-content}

Utilisez `content` sur les entités de code source. Cet exemple recherche des fichiers indexés par chemin et retourne le texte brut du fichier.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "file",
    "entity": "File",
    "filters": {
      "path": {"ends_with": "app/models/project.rb"}
    },
    "columns": ["path", "language", "content"]
  }],
  "limit": 5
}
```

Pour la syntaxe complète, les champs disponibles et les règles de validation, consultez [le langage de requête GitLab Orbit](query-language.md).
