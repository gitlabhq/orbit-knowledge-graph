---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Utilisez le langage de requête GitLab Orbit pour effectuer des recherches et des traversées dans le graphe.
title: Langage de requête GitLab Orbit
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

Utilisez le langage de requête GitLab Orbit lorsque vous avez besoin des données GitLab sous forme de graphe plutôt que sous forme de réponse API plate. Une requête est un objet JSON. Elle désigne les entités à faire correspondre, les relations à suivre et les propriétés à retourner.

## Enveloppe de requête {#request-envelope}

Lors de la soumission d'une requête via l'API REST ou `glab orbit remote query`, encapsulez l'objet de requête dans un champ `query` de niveau supérieur :

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    }],
    "limit": 1
  },
  "response_format": "raw"
}
```

| Champ | Obligatoire | Description |
|-------|----------|-------------|
| `query` | Oui | L'objet de requête documenté ci-dessous. |
| `response_format` | Non | `"llm"` (valeur par défaut si omis ; texte [GOON](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/design-documents/querying/graph_engine.md) compact optimisé pour la consommation par les LLM) ou `"raw"` (JSON structuré). Utilisez `"raw"` lors de la redirection de la sortie vers `jq`. |

L'interface en ligne de commande `orbit query` (pour les graphes locaux) prend le corps brut de la requête **without** l'enveloppe.

## Structure de la requête {#query-shape}

Chaque requête possède un champ `query_type` et un tableau `nodes` de sélecteurs de nœuds.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state"]
  }],
  "limit": 1
}
```

## Types de requêtes {#query-types}

| Type de requête | Utilisation |
|------------|-----------|
| `traversal` | Récupérer les nœuds correspondants ou suivre les relations entre les nœuds. |
| `aggregation` | Compter, sommer, faire la moyenne, regrouper ou trier les résultats de graphe correspondants. |
| `path_finding` | Trouver un chemin délimité entre deux sélecteurs de nœuds. |
| `neighbors` | Retourner les nœuds connectés à un nœud délimité. |

Le `traversal` à nœud unique est la structure de recherche. Il n'existe pas de type de requête `search` séparé.

## Champs de niveau supérieur {#top-level-fields}

| Champ | Type | Description |
|-------|------|-------------|
| `query_type` | `string` | L'un des suivants : `traversal`, `aggregation`, `path_finding` ou `neighbors`. |
| `nodes` | `array` | Sélecteurs de nœuds. Toujours requis ; les requêtes à nœud unique (`neighbors`, `traversal` en forme de recherche) utilisent un tableau à un seul élément. Maximum 5. |
| `relationships` | `array` | Sélecteurs de relations pour la traversée ou l'agrégation. Maximum 5. |
| `aggregations` | `array` | Définitions d'agrégation. Requis pour `aggregation`. Maximum 10. |
| `group_by` | `array` | Clés de regroupement pour les lignes d'agrégation. Maximum 4. |
| `path` | `object` | Configuration de la recherche de chemin. Requis pour `path_finding`. |
| `neighbors` | `object` | Configuration de la recherche de voisins. Requis pour `neighbors`. |
| `limit` | `integer` | Nombre maximum de lignes à retourner lorsqu'aucun `cursor` n'est défini. Par défaut 30. Maximum 1000. Vérifiez `pagination.truncated` dans la réponse : lorsque la valeur est true, d'autres lignes correspondantes existent. |
| `cursor` | `object` | Pagination par jeu de clés : `{"page_size": N}` pour la première page, puis `{"page_size": N, "after": "<pagination.next_cursor>"}` jusqu'à ce que `next_cursor` soit absent. Permet d'atteindre chaque ligne quelle que soit la taille du jeu de données. Le jeton est lié à la requête exacte qui l'a émis. |
| `order_by` | `string` | Trier les lignes par propriété de nœud : `"node.property"` (asc) ou `"-node.property"` (desc). |
| `aggregation_sort` | `string` | Trier les lignes d'agrégation par colonne de sortie (alias d'agrégation ou de clé de regroupement) : `"column"` (asc) ou `"-column"` (desc). |
| `options` | `object` | Options de présentation et de débogage. |

La pagination lit les données en direct au moment de la requête ; il n'y a pas de snapshot. Chaque page résout indépendamment la dernière version de chaque ligne et exclut les lignes supprimées de manière réversible, de sorte que la rotation des versions et le nettoyage des marqueurs de suppression entre les pages ne provoquent ni omission ni duplication de résultats. Les lignes insérées après la position du curseur dans l'ordre de tri apparaissent sur les pages ultérieures ; les lignes insérées ou réordonnées avant cette position ne sont pas revisitées. Les lignes dont la clé de tri est NULL sont triées en dernier et paginées comme toute autre ligne. Une ligne dont la clé de tri change entre les pages peut apparaître deux fois ou pas du tout, comme dans toute pagination par jeu de clés sans snapshot.

## Sélecteurs de nœuds {#node-selectors}

Un sélecteur de nœud désigne un type d'entité dans l'ontologie.

| Champ | Type | Description |
|-------|------|-------------|
| `id` | `string` | Alias local du nœud. Les relations, agrégations, chemins et voisins font référence à cet alias. |
| `entity` | `string` | Type de nœud d'ontologie, tel que `Project`, `User`, `MergeRequest`, `File` ou `Definition`. |
| `columns` | `string` ou `array` | Propriétés à retourner. Utilisez `"*"` pour toutes les propriétés non restreintes ou un tableau de noms. Si omis, GitLab Orbit retourne les colonnes par défaut de l'entité. |
| `filters` | `object` | Filtres de propriétés. |
| `node_ids` | `array` | Identifiants exacts à faire correspondre. Accepte des entiers ou des chaînes de chiffres. Maximum 500. |
| `id_range` | `object` | Plage d'identifiants inclusive avec `start` et `end`. |
| `id_property` | `string` | Propriété utilisée par `node_ids` et `id_range`. Par défaut `id`. |

Utilisez `node_ids` lorsque vous connaissez déjà l'identifiant dans le graphe. Utilisez `filters` lorsque vous connaissez une propriété naturelle telle que `username`, `full_path`, `state` ou `path`.

## Relations {#relationships}

Les relations connectent les sélecteurs de nœuds par alias.

```json
{
  "type": "AUTHORED",
  "from": "user",
  "to": "mr",
  "direction": "outgoing"
}
```

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `string` ou `array` | Type ou types de relation. Utilisez `"*"` uniquement lorsque vous avez besoin de n'importe quelle relation et que vous disposez d'une requête délimitée. |
| `from` | `string` | Alias du sélecteur de nœud de départ. |
| `to` | `string` | Alias du sélecteur de nœud d'arrivée. |
| `direction` | `string` | `outgoing`, `incoming` ou `both`. Par défaut `outgoing`. |
| `hops` | `array` | Plage de sauts `[min, max]` inclusive (`[1, 3]` ; `[2, 2]` pour exactement 2). Par défaut `[1, 1]`. Maximum 3. |
| `filters` | `object` | Filtres de propriétés de relation. Maximum 5 filtres. |

Par exemple, les merge requests pointent vers les projets avec `IN_PROJECT`, et les utilisateurs pointent vers les merge requests avec `AUTHORED`.

## Filtres {#filters}

Les filtres peuvent utiliser une égalité simple :

```json
{
  "filters": {
    "state": "merged"
  }
}
```

Ils peuvent également utiliser un objet opérateur. Plusieurs clés d'opérateur sur la même propriété sont combinées avec AND, ce qui permet d'exprimer des plages :

```json
{
  "filters": {
    "created_at": {"gte": "2026-01-01", "lt": "2026-02-01"},
    "state": {"in": ["opened", "merged"]}
  }
}
```

Pour répéter un opérateur sur la même propriété, utilisez un tableau d'objets opérateurs : `{"title": [{"contains": "foo"}, {"contains": "bar"}]}`.

| Opérateur | Utilisation |
|----------|-----|
| `eq` | Égal à une valeur scalaire. |
| `gt`, `gte`, `lt`, `lte` | Comparaison numérique, de date ou d'horodatage. |
| `in` | La valeur est dans un tableau. Maximum 100 valeurs. |
| `contains` | La chaîne contient une sous-chaîne. |
| `starts_with` | La chaîne commence par un préfixe. |
| `ends_with` | La chaîne se termine par un suffixe. |
| `is_null` | Vérification de la valeur nulle. Prend un booléen : `false` correspond aux valeurs non nulles. |
| `is_not_null` | Vérification de la valeur non nulle. Prend un booléen : `false` correspond aux valeurs nulles. |
| `token_match` | L'index de texte contient un token. |
| `all_tokens` | L'index de texte contient tous les tokens. |
| `any_tokens` | L'index de texte contient au moins un token. |

Les opérateurs de token ne fonctionnent que sur les propriétés dotées d'index de texte.

### Propriétés indexées par texte {#text-indexed-properties}

Les propriétés suivantes prennent en charge `token_match`, `all_tokens` et `any_tokens`. L'utilisation de ces opérateurs sur d'autres propriétés effectue un scan complet de la chaîne, ce qui est plus lent.

<!-- The table below is generated from the ontology's `text(...)` storage indexes. -->
<!-- Do not edit it by hand: run `mise run docs:query-language` and commit. CI fails on drift. -->
<!-- BEGIN GENERATED: text-indexed-properties -->

| Entité | Propriétés indexées par texte |
|--------|------------------------|
| `Branch` | `name` |
| `Definition` | `file_path`, `fqn`, `name` |
| `Deployment` | `ref` |
| `Directory` | `name`, `path` |
| `Environment` | `environment_type`, `name` |
| `File` | `name`, `path` |
| `Finding` | `description`, `name` |
| `Group` | `description`, `name` |
| `ImportedSymbol` | `file_path`, `import_path` |
| `Job` | `name`, `ref` |
| `Label` | `description`, `title` |
| `MergeRequest` | `description`, `source_branch`, `target_branch`, `title` |
| `MergeRequestDiffFile` | `new_path`, `old_path` |
| `Milestone` | `description`, `title` |
| `Note` | `note` |
| `Pipeline` | `ref` |
| `Project` | `description`, `name` |
| `Runner` | `name` |
| `Stage` | `name` |
| `User` | `name`, `username` |
| `Vulnerability` | `description`, `title` |
| `VulnerabilityIdentifier` | `external_id`, `external_type`, `name` |
| `VulnerabilityOccurrence` | `description`, `name` |
| `VulnerabilityScanner` | `external_id`, `name` |
| `WorkItem` | `description`, `title` |

<!-- END GENERATED: text-indexed-properties -->

## Colonnes et colonnes virtuelles {#columns-and-virtual-columns}

La plupart des colonnes proviennent des tables de graphes indexées dans ClickHouse. Certaines colonnes sont virtuelles : GitLab Orbit les récupère depuis un autre service après le retour de la requête de graphe.

Demandez explicitement les colonnes virtuelles dans `columns`. L'option `dynamic_columns` utilisée par `path_finding` et `neighbors` exclut les colonnes virtuelles car elles peuvent nécessiter des appels à des services externes.

| Entité | Colonne virtuelle | Ce qu'elle retourne |
|--------|----------------|-----------------|
| `MergeRequest` | `diff` | Diff unifié complet de la merge request. |
| `MergeRequestDiff` | `patch` | Patch complet pour un snapshot de diff de merge request. |
| `MergeRequestDiffFile` | `diff` | Texte de diff unifié par fichier. Retourne `null` lorsque `too_large` est `true`. |
| `File` | `content` | Texte source brut d'un fichier. |
| `Definition` | `content` | Texte source d'une définition indexée. |

La colonne `content` est destinée au code source. Pour le texte de diff de merge request, utilisez `MergeRequest.diff`, `MergeRequestDiff.patch` ou `MergeRequestDiffFile.diff`.

### Filtrage sur les colonnes virtuelles {#filtering-on-virtual-columns}

Les colonnes virtuelles prennent en charge les opérateurs de filtre `eq`, `contains`, `starts_with`, `ends_with`, `is_null` et `is_not_null` dans les requêtes `traversal`, et uniquement sur les nœuds qui épinglent des `node_ids` explicites. Le filtre résout la colonne pour ces lignes et effectue la comparaison en mémoire. L'utilisation d'un filtre de colonne virtuelle pour rechercher des lignes que vous n'avez pas encore identifiées (recherche de contenu) n'est pas prise en charge et est rejetée ; identifiez d'abord les lignes candidates avec des filtres indexés, puis filtrez les lignes épinglées par contenu de colonne virtuelle.

Vous n'avez pas besoin de demander la colonne pour la filtrer ; une colonne virtuelle filtrée mais non demandée est résolue pour la comparaison et omise de la réponse.

Les requêtes `aggregation`, `neighbors` et `path_finding` rejettent les filtres sur les colonnes virtuelles.

## Exemples de traversée {#traversal-examples}

Récupérer une merge request avec son diff complet :

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

Récupérer le contenu de diff par fichier à partir des snapshots de diff :

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

`HAS_DIFF` retourne chaque snapshot de diff que la merge request a jamais eu (FK `MergeRequestDiff.merge_request_id`). `HAS_LATEST_DIFF` retourne uniquement le snapshot le plus récent (FK `MergeRequest.latest_merge_request_diff_id`) — utile pour « à quoi ressemble la merge request en ce moment », mais pas pour les questions historiques. Pour « chaque merge request ayant touché un fichier », traversez `HAS_DIFF` sur tous les snapshots. L'utilisation de `HAS_LATEST_DIFF` pour des questions de couverture historique peut considérablement sous-compter les fichiers à longue durée de vie : une MR ayant touché le fichier dans une révision antérieure mais pas dans son diff final est invisible via `HAS_LATEST_DIFF`.

`MergeRequestDiffFile.old_path` est la colonne préférée pour la recherche de fichiers ; `new_path` diffère de `old_path` uniquement lors des renommages. Le filtrage et le regroupement par `old_path` maintiennent la même identité de ligne tout au long de l'historique d'une MR. Consultez les descriptions des champs de l'ontologie dans [`merge_request_diff_file.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/config/ontology/nodes/code_review/merge_request_diff_file.yaml).

Récupérer le contenu d'un fichier source :

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

Récupérer le texte source d'une fonction ou d'une définition de classe spécifique. La colonne `content` retourne le texte source brut uniquement de cette définition, et non le fichier complet. Utilisez `fqn` (nom pleinement qualifié) pour une correspondance exacte, ou `name` avec `contains` pour une recherche plus large :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "d",
    "entity": "Definition",
    "filters": {
      "fqn": {"eq": "Gitlab::Auth::authenticate"}
    },
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"]
  }],
  "limit": 5
}
```

Trouver les merge requests fusionnées dans un projet :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"},
      "columns": ["name", "full_path"]
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"},
      "columns": ["iid", "title", "state", "merged_at"]
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "limit": 25
}
```

Trouver chaque pipeline ayant été exécuté pour une merge request. Filtrez toujours `Pipeline.source = "merge_request_event"` pour correspondre à ce que l'onglet **Pipelines** de la merge request affiche :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "p",
    "entity": "Pipeline",
    "filters": {
      "merge_request_id": {"eq": 482908721},
      "source": {"eq": "merge_request_event"}
    },
    "columns": ["id", "status", "source", "sha", "ref", "created_at"]
  }],
  "order_by": "-p.created_at",
  "limit": 100
}
```

`merge_request_id` est le `id` numérique interne de la merge request, et non le `iid` limité au projet. Recherchez-le d'abord avec une traversée `MergeRequest` filtrée par `iid` et `project_id`, puis injectez le `id` dans la requête ci-dessus.

`Pipeline.merge_request_id` et l'arête `MergeRequest --TRIGGERED-->
Pipeline` relient une MR à chaque pipeline CI engendré dans son contexte, y compris les pipelines parent-enfant en aval (`source = "parent_pipeline"`) déclenchés par les pipelines MR de niveau supérieur. Sans le filtre `source = "merge_request_event"`, le résultat surévalue d'un facteur important pour toute MR utilisant le déploiement en éventail de pipeline parent-enfant, et ne correspond pas à ce que l'onglet **Pipelines** de la MR affiche. Appliquez le même filtre lors de la traversée de `MergeRequest --TRIGGERED--> Pipeline` dans une requête multi-nœuds.

`MergeRequest --HAS_HEAD_PIPELINE--> Pipeline` est une arête différente. Elle pointe vers le pipeline le plus récent en cours d'exécution contre le sommet de la branche source de la merge request. Utilisez-la pour « ce qui est en cours d'exécution », et non pour l'historique des pipelines.

## Agrégation {#aggregation}

Les requêtes d'agrégation utilisent `aggregations`. Chaque agrégation est un objet avec une seule clé de fonction dont la valeur correspond à ce qui doit être agrégé, plus un nom de colonne de sortie `as` optionnel : `{"avg": "mr.merge_duration", "as": "avg_dur"}`.

| Clé de fonction | Valeur | Types de propriétés pris en charge |
|--------------|-------|--------------------------|
| `count` | `"node"` (compter les lignes correspondantes) ou `"node.property"` (compter les valeurs non nulles) | Tous |
| `sum` | `"node.property"` | Numérique uniquement |
| `avg` | `"node.property"` | Numérique uniquement |
| `min` | `"node.property"` | Numérique, chaîne, booléen, `Date` ou `DateTime` |
| `max` | `"node.property"` | Numérique, chaîne, booléen, `Date` ou `DateTime` |

Sans `as`, le nom de la colonne de sortie est dérivé sous la forme `<function>_<node>` (`count_mr`) ou `<function>_<node>_<property>` (`avg_mr_merge_duration`). Référencez ces noms dans `aggregation_sort`.

`sum` et `avg` rejettent les propriétés `DateTime` avec une erreur de validation. Pour agréger des dates, utilisez `min` ou `max`.

Utilisez `group_by` de niveau supérieur pour regrouper les lignes d'agrégation. Il s'applique à chaque agrégation de la requête. Ne placez pas de regroupement à l'intérieur d'une agrégation individuelle.

Les clés de regroupement prennent en charge les formes suivantes :

| Clé de regroupement | Forme | Valeur résultante |
|-----------|-------|--------------|
| Nœud | `"<node-id>"` (par ex. `"p"`) | Un objet d'entité imbriqué dans chaque ligne. |
| Propriété | `"<node-id>.<property>"` (par ex. `"mr.state"`) | Une valeur de compartiment scalaire dans chaque ligne. |
| Date tronquée | `{"key": "<node-id>.<property>", "truncate": "<unit>"}` | La propriété tronquée au début de l'unité. |

Les noms de colonnes de sortie sont dérivés : les clés de nœud utilisent l'identifiant du nœud (`p`), les clés de propriété utilisent `<node>_<property>` (`mr_state`), et les clés tronquées ajoutent l'unité (`mr_created_at_month`). Référencez ces noms dans `aggregation_sort`. Les noms de sortie de regroupement ou d'agrégation en double sont rejetés.

Utilisez les noms dérivés. Uniquement lorsqu'un consommateur exige un nom de colonne spécifique, renommez avec le `as` optionnel de la forme objet : `{"key": "mr.state", "as":
"state"}`, ou avec la troncature `{"key": "mr.created_at", "truncate": "month",
"as": "month"}`.

Les unités de troncature sont `minute`, `hour`, `day`, `week`, `month`, `quarter` et `year`, et s'appliquent uniquement aux propriétés `Date`/`DateTime`. `minute` et `hour` nécessitent `node_ids` ou un filtre sur la propriété tronquée pour limiter la cardinalité des compartiments.

Les groupes de propriétés doivent référencer une propriété réelle, filtrable et prise en charge par ClickHouse, que l'appelant est autorisé à utiliser. Les champs virtuels et les champs non filtrables sont rejetés lors de la validation.

Compter les merge requests fusionnées par projet :

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"}
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "group_by": ["project"],
  "aggregations": [
    { "count": "mr", "as": "merged_mrs" }
  ],
  "aggregation_sort": "-merged_mrs",
  "limit": 10
}
```

Compter les vulnérabilités détectées par gravité :

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    }
  ],
  "group_by": ["v.severity"],
  "aggregations": [
    { "count": "v", "as": "vulnerability_count" }
  ],
  "aggregation_sort": "-vulnerability_count",
  "limit": 10
}
```

Les réponses d'agrégation ont une structure tabulaire. `columns` décrit les valeurs agrégées calculées, `group_columns` décrit les clés de regroupement, et `rows` contient les valeurs de groupe et les valeurs de métriques. Les lignes regroupées par nœud stockent l'entité regroupée sous la clé de groupe. Les lignes regroupées par propriété stockent la valeur de compartiment scalaire sous la clé de groupe.

`collect` est listé dans le type d'entrée mais actuellement rejeté par la validation.

## Recherche de chemin {#path-finding}

Les requêtes de recherche de chemin utilisent `path`.

| Champ | Type | Description |
|-------|------|-------------|
| `type` | `string` | `shortest`. |
| `from` | `string` | Alias du sélecteur de nœud de départ. |
| `to` | `string` | Alias du sélecteur de nœud d'arrivée. |
| `max_depth` | `integer` | Longueur maximale du chemin. Maximum 3. |
| `rel_types` | `array` | Types de relations à traverser. Requis sauf si les deux points d'extrémité utilisent `node_ids`. |

Les deux points d'extrémité doivent être délimités par `node_ids`, des filtres ou un `id_range` avec une plage de 500 ou moins. Si l'un ou l'autre des points d'extrémité utilise des filtres ou `id_range`, fournissez `rel_types`.

```json orbit-query
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "start", "entity": "Project", "node_ids": [278964]},
    {"id": "end", "entity": "User", "node_ids": [1]}
  ],
  "path": {
    "type": "shortest",
    "from": "start",
    "to": "end",
    "max_depth": 3,
    "rel_types": ["CREATOR", "AUTHORED", "IN_PROJECT"]
  },
  "limit": 5
}
```

## Voisins {#neighbors}

Les requêtes de voisins utilisent un tableau `nodes` à un seul élément et un objet `neighbors`. Le nœud central doit être délimité par `node_ids`, des filtres ou un `id_range` étroit.

```json orbit-query
{
  "query_type": "neighbors",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345]
  }],
  "neighbors": {
    "direction": "both",
    "rel_types": ["AUTHORED", "IN_PROJECT", "HAS_DIFF"]
  },
  "options": {
    "dynamic_columns": "default"
  },
  "limit": 25
}
```

Définissez `options.dynamic_columns` sur `"*"` si vous avez besoin de toutes les colonnes non restreintes prises en charge par ClickHouse pour les nœuds voisins ou de chemin découverts dynamiquement. Les colonnes virtuelles nécessitent toujours une demande explicite dans une requête de traversée.

## Limites de validation {#validation-limits}

GitLab Orbit rejette les requêtes trop larges ou ambiguës avant de compiler le SQL.

| Limite | Valeur |
|-------|-------|
| Nœuds par requête | 5 |
| Relations par requête | 5 |
| Agrégations par requête | 10 |
| `node_ids` par sélecteur | 500 |
| Valeurs dans un filtre `in` | 100 |
| Colonnes par sélecteur de nœud | 50 |
| Types de relations par sélecteur | 10 |
| Sauts de relation | 3 |
| Profondeur de chemin | 3 |
| Filtres par nœud | 10 |
| Filtres par relation | 5 |

Les requêtes de traversée et d'agrégation doivent inclure au moins un nœud sélectif : `node_ids`, des filtres ou un `id_range` avec une plage de 100 000 ou moins.

La traversée à nœud unique nécessite également de la sélectivité. Pour inspecter une entité large, ajoutez un filtre, fournissez des identifiants ou utilisez un `id_range` étroit.

## Options {#options}

| Option | Description |
|--------|-------------|
| `dynamic_columns` | Pour l'hydratation de `path_finding` et `neighbors`. Utilisez `default` pour les colonnes par défaut de chaque entité, ou `"*"` pour toutes les colonnes non restreintes prises en charge par ClickHouse. Par défaut `default`. |
| `include_debug_sql` | Inclure le SQL ClickHouse compilé dans les métadonnées de réponse lorsque l'appelant est autorisé à le consulter. |
