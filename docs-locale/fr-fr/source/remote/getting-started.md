---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Activez GitLab Orbit Remote sur GitLab.com et exécutez votre première requête.
title: Premiers pas avec GitLab Orbit Remote
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

## Prérequis {#prerequisites}

- Pour activer GitLab Orbit, le rôle Owner sur le groupe principal.
- Pour interroger un groupe après son indexation, le rôle Reporter ou supérieur.
- Pour afficher les données de sécurité, le rôle Responsable sécurité. Pour plus d'informations, consultez [les rôles requis pour interroger GitLab Orbit](security.md#roles-required-to-query-gitlab-orbit).

GitLab Orbit indexe uniquement les groupes principaux. Les sous-groupes et les projets héritent automatiquement de l'indexation.

## Étape 1 : activer GitLab Orbit {#step-1-enable-gitlab-orbit}

1. Dans la barre latérale gauche, développez **Your Work**.
1. Sélectionnez **Orbit** > **Configuration**.
1. Trouvez votre groupe principal dans la liste **Index**.
1. Activez le bouton **Activer**.

GitLab Orbit commence l'indexation immédiatement. L'indexation initiale prend quelques minutes pour les petits groupes et jusqu'à 30 minutes pour les groupes comportant des milliers de projets.

Vérifiez le statut d'indexation à tout moment :

```shell
glab orbit remote status
```

## Étape 2 : exécuter votre première requête {#step-2-run-your-first-query}

GitLab Orbit Remote expose le même graphe via trois interfaces. Choisissez celle qui correspond à l'utilisateur qui effectue les requêtes :

| Méthode | Idéale pour | Configuration | Facturation |
|---|---|---|---|
| **GitLab Duo Agent Platform** | Utilisateurs finaux dans l'interface GitLab | Aucune | Non facturé |
| **MCP** | Claude Code, Codex, autres agents d'IA | Configuration unique de l'agent | GitLab Credits |
| **API REST** | Scripts, tableaux de bord, outils personnalisés | Jeton d'API | GitLab Credits |

### GitLab Duo Agent Platform (aucune configuration requise) {#gitlab-duo-agent-platform-no-setup-required}

GitLab Orbit est intégré à GitLab Duo Agent Platform. L'agent GitLab Duo, l'agent Planner, l'agent Security Analyst, l'agent Data Analyst, l'agent CI Expert et le flow Developer appellent automatiquement les outils `list_commands` et `invoke_command` de GitLab Orbit, en exécutant des commandes telles que `query_graph` et `get_graph_schema`, lorsqu'une question est mieux résolue par traversée de graphe. Aucune sélection d'outil ni configuration n'est requise.

Par exemple, créez un élément de travail demandant de renommer la méthode `deploy_user`. Le flow Developer utilise GitLab Orbit pour identifier chaque service qui l'appelle, puis rédige une merge request qui met à jour chacun d'eux.

Les requêtes GitLab Duo ne sont pas facturées et ne consomment pas de GitLab Credits.

### MCP (Claude Code, Codex, autres agents) {#mcp-claude-code-codex-other-agents}

Consultez [Utiliser GitLab Orbit via MCP](access/mcp.md) pour la configuration. Une fois configuré, vous disposez de deux outils : `query_graph` et `get_graph_schema`.

### Installer la compétence GitLab Orbit pour les agents d'IA {#install-the-gitlab-orbit-skill-for-ai-agents}

La compétence GitLab Orbit fournit à votre agent d'IA des recettes de requêtes, des conseils DSL et des informations de dépannage afin qu'il rédige des requêtes GitLab Orbit correctes dès la première tentative :

```shell
glab skills install --global orbit
```

Consultez [Configurer les agents de codage IA avec la compétence GitLab Orbit](../ai_coding_agents.md) pour l'installation à portée de projet, les instructions de mise à jour et le contenu de la compétence.

### API REST {#rest-api}

Remplacez `your-group` par le chemin du groupe principal sur lequel vous avez activé GitLab Orbit. Le filtre `full_path` restreint la portée de la requête afin qu'elle passe la validation de sélectivité de GitLab Orbit.

Placez le corps de la requête dans `request.json` :

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"],
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
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

## Que faire ensuite {#what-to-try-next}

- [Ce qu'indexe GitLab Orbit](indexing.md) \- comprendre la couverture avant d'écrire des requêtes
- [Référence du schéma](schema.md) \- explorer les 28 types de nœuds et leurs propriétés
- [Cookbook](cookbook.md) \- requêtes prêtes à l'emploi pour les cas d'utilisation courants
- [Premiers pas avec GitLab Orbit Local](../local/getting-started.md) \- interroger un dépôt local hors ligne
