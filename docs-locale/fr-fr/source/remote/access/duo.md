---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Utilisez GitLab Orbit via GitLab Duo Agent Platform. Les agents appellent les outils graphiques de GitLab Orbit pour ancrer leurs réponses dans vos données GitLab en temps réel, à travers le GitLab Duo Agent, l'agent Planner, l'agent Security Analyst, l'agent Data Analyst, l'agent CI Expert et le flow Developer."
title: Utiliser GitLab Orbit avec GitLab Duo Agent Platform
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

GitLab Orbit est intégré dans GitLab Duo Agent Platform. Les agents appellent automatiquement les outils de commande de GitLab Orbit (`list_commands`, `invoke_command`), en exécutant des commandes telles que `get_graph_schema` et `query_graph`, lorsqu'une question est mieux répondue en parcourant votre graphe SDLC : dépendances inter-projets, rayon d'impact, héritage de pipeline, lignée des vulnérabilités, patterns de contribution. Lorsque GitLab Orbit ne dispose pas de la réponse, l'agent se replie sur ses outils existants.

## Prérequis {#prerequisites}

- GitLab Orbit est [activé sur votre groupe](../getting-started.md).
- Vous avez accès à [GitLab Duo Agent Platform](https://docs.gitlab.com/user/duo_agent_platform/).

## Disponibilité de GitLab Orbit {#where-gitlab-orbit-is-available}

GitLab Orbit est intégré aux agents et flows GitLab Duo Agent Platform suivants :

| Agent ou flow | Quand l'utiliser |
|---|---|
| GitLab Duo Agent | Assistant général de développement. Obtenez de l'aide pour le code, la planification, la sécurité et la gestion de projet. Appelle GitLab Orbit lorsque les réponses bénéficient d'un contexte graphique. |
| Agent Planner | Planification des tickets et des jalons. Interrogez sur la propriété des éléments de travail, les bloquants, la charge des contributeurs, la progression des jalons entre les projets. |
| Agent Security Analyst | Triage des vulnérabilités. Interrogez sur les vulnérabilités ouvertes par gravité, la couverture CVE à l'échelle du groupe, les chronologies d'introduction des vulnérabilités. |
| Agent Data Analyst | Analyses SDLC alimentées par GLQL. Interrogez sur la santé des pipelines, le temps de cycle des merge requests, les patterns de contribution, la fréquence de déploiement. |
| Agent CI Expert | Triage des pipelines. Interrogez sur les causes d'échec des jobs, l'héritage des pipelines, les jobs les plus lents, les projets échouant le plus fréquemment. |
| Flow Developer | Transformez un élément de travail en une merge request brouillon dans l'interface. GitLab Orbit ancre l'implémentation de l'agent dans votre graphe SDLC en temps réel : dépendances, propriété, rayon d'impact. |

Lorsqu'un agent utilise GitLab Orbit pour répondre à une question, la réponse est ancrée dans votre graphe en temps réel plutôt que dans les connaissances générales de l'agent.

## Facturation {#billing}

Les requêtes que GitLab Duo Agent Platform effectue contre GitLab Orbit en votre nom sont à tarification nulle. Elles ne consomment pas de GitLab Credits.

## Exemples de prompts {#example-prompts}

Posez ces questions dans l'une des interfaces ci-dessus : l'agent sélectionne le bon outil.

Exploration de la base de code :

- « Quels sont les 10 projets mis à jour le plus récemment dans mon groupe ? »
- « Quels projets ont le plus de merge requests ouvertes ? »
- « Qui sont les principaux contributeurs à ce projet par merge requests fusionnées ? »

Rayon d'impact et effet de blast :

- « Quels projets importent la bibliothèque `payments-service` ? »
- « Quels fichiers dans ce projet dépendent de `UserAuthService` ? »
- « Si je déprécie cette fonction, quels autres fichiers y font référence ? »

CI/CD et santé des pipelines :

- « Quels projets ont le taux d'échec de pipeline le plus élevé ? »
- « Quelles sont les causes d'échec de job les plus fréquentes dans ce groupe ? »
- « Quels pipelines mettent le plus de temps à s'exécuter ? »

Sécurité :

- « Montrez-moi toutes les vulnérabilités ouvertes de gravité critique et élevée dans ce groupe. »
- « Quels projets ont des vulnérabilités non résolues introduites au cours des 30 derniers jours ? »
- « Quels CVE sont présents dans mes projets ? »

Planification et éléments de travail :

- « Combien de tickets ouverts sont attribués à chaque utilisateur dans ce groupe ? »
- « Quels jalons sont en retard ? »
- « Quels éléments de travail bloquent cet epic ? »

## Limitations {#limitations}

- GitLab Orbit répond uniquement aux questions concernant les groupes pour lesquels il est activé et auxquels vous avez accès.
- Les questions complexes à plusieurs étapes peuvent nécessiter un suivi pour affiner la portée.
- Le contenu du code (texte des fichiers, corps des fonctions) est disponible mais peut ne pas être retourné par défaut pour les résultats volumineux. Demandez explicitement : « Montrez-moi le code source de cette fonction. »
