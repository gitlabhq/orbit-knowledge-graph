---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Une bibliothèque de prompts prêts à l'emploi qui transforment votre agent d'IA en expert de votre base de code, de vos pipelines, de vos dépendances et de la sécurité à l'aide de GitLab Orbit."
title: Cookbook
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

GitLab Orbit répond aux questions sur l'ensemble de votre cycle de vie du développement logiciel : le code, les merge requests, les pipelines, les dépendances et la sécurité. Vous n'écrivez pas de requêtes de graphe à la main. Vous posez une question à un agent d'IA en langage courant, et l'agent utilise GitLab Orbit pour parcourir le graphe et répondre.

Cette page est une bibliothèque de prompts qui fonctionnent. Chacun d'eux transforme votre agent en expert de vos propres projets.

## Comment utiliser cette page {#how-to-use-this-page}

1. Connectez un agent à GitLab Orbit. GitLab Duo Agent Platform intègre GitLab Orbit nativement. Les agents externes tels que Claude Code ou Codex se connectent via [MCP ou le CLI `glab`](access/mcp.md).
1. Choisissez le résultat souhaité et copiez le prompt correspondant.
1. Remplacez les valeurs entre `<angle brackets>` par votre propre groupe, projet, fichier ou fenêtre temporelle.
1. Collez le prompt dans votre agent et laissez-le travailler. Posez des questions de suivi dans la même conversation pour approfondir l'analyse.

Chaque prompt comporte également une section **See the GitLab Orbit queries behind this**. Vous n'avez jamais besoin de l'ouvrir, mais elle affiche les requêtes de graphe exactes exécutées par l'agent si vous souhaitez les auditer ou appeler l'[API REST](access/api.md) directement.

## Attribuez vos dépenses CI au code qui en est responsable {#attribute-your-ci-spend-to-the-code-that-causes-it}

Le calcul CI est coûteux, et la majeure partie du coût se cache dans des échecs qui sont relancés encore et encore. Ce prompt classe les échecs à l'échelle de votre organisation, identifie ceux causés par un modèle CI/CD partagé, puis remonte de chacun d'eux jusqu'aux fichiers et définitions de code exacts qui continuent de se briser. Cette dernière étape est la chaîne d'attribution des coûts : elle transforme « le CI est coûteux » en « ces fichiers continuent de faire échouer ces jobs. »

```plaintext
Using GitLab Orbit, help me understand what is driving our CI compute cost.

1. Find the job and pipeline failures across my organization over the last
   60 days, covering at least 20 projects. Rank the job names by how often
   they fail.
2. Flag any failing job name that recurs across three or more projects. Those
   usually point to a shared CI/CD template that is worth fixing once.
3. For the top recurring failures, find the merge requests that generate the
   most repeated failed pipelines.
4. Trace those failures back through the merge request diffs to the specific
   files, and the code definitions inside those files, that keep changing.
5. Show me the full chain from failing job to the exact code to review, and
   tell me where to focus a fix to cut the most CI spend.

Prioritize correctness and depth over speed.
```

Ce que vous obtenez : une liste classée de vos échecs récurrents les plus coûteux, les modèles partagés à l'origine des échecs inter-projets, et une courte liste de fichiers et de fonctions à corriger, chacun associé aux échecs qu'il provoque.

Adaptez-le : modifiez la fenêtre temporelle, limitez-le à un groupe ou un projet, ou demandez à l'agent d'estimer le calcul économisé si vous corrigiez les trois principaux problèmes.

<details>
<summary>See the GitLab Orbit queries behind this</summary>

L'agent les exécute en séquence. Remplacez l'horodatage de l'exemple par une date au début de votre fenêtre, et remplacez l'ID de la merge request et le chemin du fichier par les valeurs renvoyées par les étapes précédentes.

Classez les échecs de job les plus fréquents dans votre organisation :

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"gte": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": ["j.name"],
  "aggregations": [{ "count": "j", "as": "failures" }],
  "aggregation_sort": "-failures",
  "limit": 40
}
```

Trouvez les jobs en échec qui se reproduisent dans plusieurs projets. GitLab Orbit ne dispose pas de fonction de comptage distinct, donc regroupez par nom de job et par projet ensemble : un nom de job qui apparaît dans trois projets ou plus est un point chaud de modèle partagé.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"gte": "2025-01-01T00:00:00Z"}
      }
    },
    {"id": "p", "entity": "Project"}
  ],
  "relationships": [{"type": "IN_PROJECT", "from": "j", "to": "p"}],
  "group_by": [
    "j.name",
    "p.full_path"
  ],
  "aggregations": [{ "count": "j", "as": "failures" }],
  "aggregation_sort": "-failures",
  "limit": 200
}
```

Trouvez les merge requests générant le plus d'échecs répétés. Filtrez `source` sur `merge_request_event` afin de ne pas comptabiliser également les pipelines enfants en aval déclenchés par ces pipelines.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "pl",
      "entity": "Pipeline",
      "filters": {
        "status": "failed",
        "source": "merge_request_event",
        "created_at": {"gte": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": ["pl.merge_request_id"],
  "aggregations": [{ "count": "pl", "as": "failed_pipelines" }],
  "aggregation_sort": "-failed_pipelines",
  "limit": 20
}
```

Remontez d'une merge request aux fichiers qui continuent de changer. Limitez cela à une seule merge request ; le même parcours sur chaque pipeline en échec à la fois expire. Les arêtes `HAS_FILE` sont peu peuplées, traitez donc un résultat court comme une couverture incomplète plutôt qu'une source faisant autorité.

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest", "filters": {"id": {"eq": 123456789}}},
    {"id": "d", "entity": "MergeRequestDiff"},
    {"id": "f", "entity": "MergeRequestDiffFile"}
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "d"},
    {"type": "HAS_FILE", "from": "d", "to": "f"}
  ],
  "group_by": ["f.old_path"],
  "aggregations": [{ "count": "d", "as": "diff_snapshots" }],
  "aggregation_sort": "-diff_snapshots",
  "limit": 20
}
```

Explorez les définitions de code dans un fichier point chaud. Les nœuds `File` et `Definition` n'existent que pour les fichiers source indexés, donc certains chemins, comme les helpers de support de test, pourraient ne pas être indexés.

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"eq": "app/models/project.rb"}}
    },
    {
      "id": "def",
      "entity": "Definition",
      "columns": ["name", "fqn", "definition_type", "start_line"]
    }
  ],
  "relationships": [{"type": "DEFINES", "from": "f", "to": "def"}],
  "limit": 30
}
```

</details>

## Comprendre rapidement une base de code {#understand-a-codebase-fast}

Plongez dans un projet inconnu et orientez-vous en quelques minutes plutôt qu'en plusieurs jours.

```plaintext
I'm new to the <my-org/my-project> project. Using GitLab Orbit, give me a tour:
- The most active contributors over the last few months.
- The core classes, modules, and how they relate.
- The main entry points and the files I should read first.

Then summarize how this codebase is structured and suggest the three files
to read first to understand it.
```

<details>
<summary>See the GitLab Orbit queries behind this</summary>

Trouvez les contributeurs les plus actifs d'un projet :

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["username", "name"]},
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    },
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    }
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": ["u"],
  "aggregations": [
    { "count": "mr", "as": "merged_mrs" }
  ],
  "aggregation_sort": "-merged_mrs",
  "limit": 10
}
```

</details>

## Cartographier les dépendances et le rayon d'impact {#map-dependencies-and-blast-radius}

Répondez à la question « que se passe-t-il si je modifie ceci ? » avant d'effectuer la modification.

```plaintext
Using GitLab Orbit, map the blast radius of <shared-auth-lib>.
- Which projects and files import it?
- Which code definitions depend on it?
- What would break if I changed its public interface?

Rank the affected areas by how many places depend on them, and tell me the
riskiest change I could make.
```

<details>
<summary>See the GitLab Orbit queries behind this</summary>

Trouvez tous les fichiers qui importent un module spécifique. Remplacez `payments-service` par le module ou la bibliothèque que vous souhaitez tracer :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "sym",
    "entity": "ImportedSymbol",
    "columns": ["file_path", "import_path", "identifier_name"],
    "filters": {
      "import_path": {"contains": "payments-service"}
    }
  }],
  "limit": 100
}
```

Trouvez les projets qui dépendent d'une bibliothèque partagée :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"contains": "shared-auth-lib"}}
    },
    {"id": "b", "entity": "Branch", "columns": ["name", "is_default"]},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "ON_BRANCH", "from": "f", "to": "b"},
    {"type": "CONTAINS", "from": "p", "to": "b"}
  ],
  "limit": 100
}
```

Classez les définitions que le plus de code importe :

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "sym",
      "entity": "ImportedSymbol",
      "columns": ["import_path"],
      "filters": {
        "import_path": {"contains": "payments"}
      }
    },
    {"id": "def", "entity": "Definition", "columns": ["name", "fqn", "file_path"]}
  ],
  "relationships": [
    {"type": "IMPORTS", "from": "sym", "to": "def"}
  ],
  "group_by": ["def"],
  "aggregations": [
    { "count": "sym", "as": "import_count" }
  ],
  "aggregation_sort": "-import_count",
  "limit": 20
}
```

</details>

## Maintenez vos pipelines en bonne santé {#keep-your-pipelines-healthy}

Identifiez vos pires sources de problèmes CI/CD et les raisons de leurs échecs.

```plaintext
Using GitLab Orbit, show me where our CI/CD is unhealthy over the last 30 days:
- The projects with the most failed pipelines.
- The jobs that fail most often.
- The most common failure reasons.

Group the results so I can see which failures are worth fixing first.
```

<details>
<summary>See the GitLab Orbit queries behind this</summary>

Trouvez les projets avec le plus grand nombre de pipelines en échec :

```json orbit-query
{
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
    { "count": "pl", "as": "failed_count" }
  ],
  "aggregation_sort": "-failed_count",
  "limit": 10
}
```

Trouvez les jobs en échec et leurs raisons d'échec :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "j",
    "entity": "Job",
    "columns": ["name", "status", "failure_reason"],
    "filters": {"status": "failed"}
  }],
  "limit": 10
}
```

</details>

## Retracez le risque de sécurité jusqu'à sa source {#trace-security-risk-to-its-source}

Identifiez où se situe votre risque et comment il est apparu.

```plaintext
Using GitLab Orbit, find the critical and high severity vulnerabilities across
<my-org> that are still detected:
- Which projects are affected?
- How did each one get there? Trace it back to the scan and, where possible,
  the merge request that introduced the change.

Prioritize by severity and give me a short remediation shortlist.
```

<details>
<summary>See the GitLab Orbit queries behind this</summary>

Trouvez toutes les vulnérabilités critiques et de haute gravité :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "columns": ["title", "severity", "state", "report_type"],
      "filters": {
        "severity": {"in": ["critical", "high"]},
        "state": "detected"
      }
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "order_by": "-v.severity",
  "limit": 50
}
```

Comptez les vulnérabilités par projet :

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "group_by": ["p"],
  "aggregations": [
    { "count": "v", "as": "vuln_count" }
  ],
  "aggregation_sort": "-vuln_count",
  "limit": 20
}
```

Comptez les vulnérabilités par gravité :

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
    { "count": "v", "as": "vuln_count" }
  ],
  "aggregation_sort": "-vuln_count",
  "limit": 10
}
```

</details>

## Lire le code source réel {#read-the-actual-source}

Importez du code réel dans la conversation sans quitter votre agent.

```plaintext
Using GitLab Orbit, show me the source of <app/models/project.rb> and the definition
of <MyModule::my_function>, so I can review them here.
```

Les colonnes virtuelles (`content` sur `File` et `Definition`) déclenchent une récupération Gitaly après la requête de graphe, ce qui rend ces réponses plus lentes que les autres requêtes.

<details>
<summary>See the GitLab Orbit queries behind this</summary>

Récupérez le texte source d'un fichier. Utilisez `limit: 1` pour éviter les réponses volumineuses :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "f",
    "entity": "File",
    "columns": ["path", "language", "content"],
    "filters": {
      "path": {"ends_with": "app/models/project.rb"}
    }
  }],
  "limit": 1
}
```

Récupérer le texte source d'une fonction ou d'une définition de classe spécifique. Le champ `content` renvoie le texte source brut de cette seule définition, et non le fichier complet :

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"],
    "filters": {
      "fqn": {"eq": "Gitlab::Auth::authenticate"}
    }
  }],
  "limit": 5
}
```

</details>
