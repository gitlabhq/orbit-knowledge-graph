---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Comment GitLab Orbit Remote indexe les données GitLab et le code source, construit un graphe dans ClickHouse et l'expose en tant qu'API interrogeable."
title: Fonctionnement de GitLab Orbit Remote
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

## Pipeline d'indexation {#indexing-pipeline}

GitLab Orbit indexe les données provenant de deux sources et les combine en un seul graphe.

### Données SDLC {#sdlc-data}

GitLab diffuse les événements de modification via un pipeline de capture des données de modification (CDC) vers la [GitLab Data Insights Platform](https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/data_insights_platform/). La plateforme écrit des enregistrements dans les tables ClickHouse que GitLab Orbit lit et sur lesquelles il écrit son graphe.

Cela se produit en continu. Lorsqu'un utilisateur ouvre une merge request, crée un élément de travail ou lance un pipeline, la modification se propage au graphe GitLab Orbit en quelques minutes.

### Code source {#source-code}

GitLab Orbit appelle l'API interne GitLab Rails pour récupérer les fichiers source depuis vos dépôts. Il analyse chaque fichier avec un analyseur spécifique au langage, extrait les définitions (fonctions, classes, modules) et les références d'import, puis les écrit sous forme de nœuds et d'arêtes dans le graphe.

Le code est indexé uniquement depuis la branche par défaut. Une réindexation s'exécute automatiquement lorsque la branche par défaut change.

### Construction du graphe {#graph-construction}

Après lecture des données SDLC et du code, GitLab Orbit écrit un graphe unifié dans ClickHouse. Chaque entité (un projet, un utilisateur, une définition de fonction) devient un nœud. Chaque relation (un utilisateur a rédigé une merge request, un fichier importe un module) devient une arête orientée.

Lorsque vous envoyez une requête, GitLab Orbit compile le DSL JSON en SQL ClickHouse, l'exécute et renvoie des résultats typés.

## Le modèle de graphe {#the-graph-model}

Le graphe comporte deux couches :

- Couche SDLC : les objets GitLab et leurs relations. Les projets appartiennent à des groupes. Les utilisateurs rédigent des merge requests. Les pipelines s'exécutent sur des projets. Les éléments de travail sont assignés à des utilisateurs.
- Couche de code : la structure du code source et les références entre fichiers. Les fonctions sont définies dans des fichiers. Les fichiers importent des symboles depuis d'autres fichiers. Les définitions existent au sein de projets et de branches.

Les deux couches sont connectées. Une merge request (couche SDLC) touche des fichiers (couche de code). Un utilisateur (couche SDLC) est propriétaire d'une définition (couche de code) s'il a été le dernier à modifier le fichier la contenant.

## Performances {#performance}

GitLab Orbit s'exécute dans un cluster Kubernetes dédié. Il ne partage ni les ressources de calcul ni la mémoire avec votre instance GitLab.

L'indexation initiale d'un groupe important (des milliers de projets, des millions de lignes de code) s'effectue en quelques minutes. La réindexation incrémentielle après une modification s'effectue en quelques secondes à quelques minutes selon la taille de la modification.

## Exécution des requêtes {#query-execution}

Toutes les requêtes suivent le même chemin :

1. GitLab Orbit reçoit une charge utile de requête JSON (via REST, MCP ou GitLab Duo Agent Platform).
1. Le moteur de requêtes valide la requête par rapport au schéma actuel.
1. GitLab Orbit compile le DSL JSON en SQL ClickHouse.
1. ClickHouse exécute la requête sur les tables du graphe.
1. GitLab Orbit applique le filtrage des autorisations : les résultats sont limités aux entités auxquelles l'utilisateur demandeur a accès dans GitLab. Pour plus d'informations, consultez [Sécurité](security.md).
1. GitLab Orbit renvoie des résultats JSON typés.

Vous pouvez demander le SQL compilé dans les réponses aux requêtes en définissant `options.include_debug_sql: true`. Ce champ est uniquement renseigné pour les administrateurs d'instance et les membres directs de l'organisation GitLab disposant d'un accès Reporter ou supérieur.

## Conservation et suppression des données {#data-retention-and-deletion}

Lorsque vous désactivez GitLab Orbit sur un groupe, vos données indexées ne sont pas supprimées immédiatement. GitLab Orbit les conserve pendant 30 jours afin que vous puissiez le réactiver sans perdre l'historique de votre graphe. À l'issue de la période de grâce, toutes les données de graphe pour ce groupe, y compris tous les nœuds, arêtes et points de contrôle d'indexation, sont définitivement supprimées.

Si vous réactivez GitLab Orbit avant l'expiration des 30 jours, la suppression est annulée et l'indexation reprend là où elle s'était arrêtée.
