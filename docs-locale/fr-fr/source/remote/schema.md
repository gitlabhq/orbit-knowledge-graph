---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Référence complète pour l'ensemble des 27 types de nœuds GitLab Orbit répartis sur 6 domaines, incluant les propriétés et leurs types."
title: Référence de schéma
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

GitLab Orbit indexe 27 types de nœuds répartis sur 6 domaines. Utilisez-les comme noms d'entités dans vos requêtes.

Pour récupérer le schéma en direct à tout moment :

```shell
glab orbit remote schema
```

## Core {#core}

| Type de nœud | Description | Propriétés clés |
|-----------|-------------|----------------|
| `Group` | Groupe ou sous-groupe GitLab | `id`, `full_path`, `name`, `visibility`, `traversal_path` |
| `Project` | Projet et dépôt GitLab | `id`, `full_path`, `name`, `visibility`, `archived`, `star_count` |
| `User` | Compte utilisateur GitLab | `id`, `username`, `email`, `name`, `state`, `is_admin` |
| `Note` | Commentaire ou annotation sur tout objet GitLab | `id`, `note`, `noteable_type`, `noteable_id`, `internal`, `confidential` |

## Code source {#source-code}

| Type de nœud | Description | Propriétés clés |
|-----------|-------------|----------------|
| `Branch` | Branche Git | `id`, `project_id`, `name`, `is_default` |
| `Definition` | Définition de fonction, de classe, de méthode ou de module | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `Directory` | Répertoire dans un dépôt | `id`, `project_id`, `path`, `name` |
| `File` | Fichier de code source | `id`, `path`, `name`, `extension`, `language`, `content` |
| `ImportedSymbol` | Référence d'importation ou de symbole inter-fichiers | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## Revue de code {#code-review}

| Type de nœud | Description | Propriétés clés |
|-----------|-------------|----------------|
| `MergeRequest` | Demande de fusion | `id`, `iid`, `title`, `description`, `source_branch`, `target_branch`, `state`, `draft`, `squash` |
| `MergeRequestDiff` | Instantané des modifications dans une merge request | `id`, `merge_request_id`, `commits_count`, `files_count` |
| `MergeRequestDiffFile` | Fichier modifié dans le diff d'une merge request | `id`, `new_path`, `old_path`, `new_file`, `renamed_file`, `deleted_file` |

## CI/CD {#cicd}

| Type de nœud | Description | Propriétés clés |
|-----------|-------------|----------------|
| `Pipeline` | Exécution de pipeline CI/CD | `id`, `sha`, `ref`, `status`, `source`, `duration`, `failure_reason` |
| `Stage` | Étape de pipeline | `id`, `name`, `status`, `position` |
| `Job` | Job CI/CD | `id`, `name`, `status`, `ref`, `allow_failure`, `environment`, `failure_reason` |
| `Deployment` | Déploiement CI/CD d'un commit | `id`, `iid`, `status`, `ref`, `sha`, `environment_id` |
| `Environment` | Cible de déploiement CI/CD | `id`, `name`, `state`, `tier`, `external_url` |
| `Runner` | Runner CI/CD | `id`, `runner_type`, `name`, `active`, `locked` |

## Planification {#planning}

| Type de nœud | Description | Propriétés clés |
|-----------|-------------|----------------|
| `WorkItem` | Ticket, epic, tâche, incident ou autre élément de travail | `id`, `iid`, `title`, `description`, `state`, `work_item_type`, `due_date`, `weight` |
| `Milestone` | Jalon | `id`, `title`, `state`, `due_date`, `start_date` |
| `Label` | Label de catégorisation du travail | `id`, `title`, `color` |

## Sécurité {#security}

| Type de nœud | Description | Propriétés clés |
|-----------|-------------|----------------|
| `Finding` | Résultat d'analyse de sécurité provenant de `security_findings` | `id`, `uuid`, `name`, `description`, `severity`, `deduplicated` |
| `SecurityScan` | Exécution d'une analyse de sécurité dans un pipeline | `id`, `scan_type`, `status`, `latest` |
| `Vulnerability` | Vulnérabilité de sécurité confirmée ou potentielle | `id`, `title`, `state`, `severity`, `report_type`, `resolved_on_default_branch` |
| `VulnerabilityIdentifier` | CVE, CWE ou autre référence externe | `id`, `external_type`, `external_id`, `name`, `url` |
| `VulnerabilityOccurrence` | Occurrence spécifique d'une vulnérabilité (`Vulnerabilities::Finding` dans Rails) | `id`, `uuid`, `severity`, `report_type`, `detection_method`, `cve`, `location` |
| `VulnerabilityScanner` | Analyseur de sécurité | `id`, `external_id`, `name`, `vendor` |

## Notes {#notes}

- Les identifiants de définition sont des entiers hachés par contenu, dont la portée est définie par projet et par branche. Deux définitions du même symbole dans des projets différents ont des identifiants différents, même si le nom de la fonction et le chemin de fichier sont identiques.
- Tous les identifiants d'entités sont retournés sous forme de chaînes de caractères dans les réponses aux requêtes, même lorsque la valeur sous-jacente est un entier. Cela évite toute perte de précision dans les clients JavaScript pour les valeurs supérieures à `Number.MAX_SAFE_INTEGER`.
- Les champs `content` sur les nœuds `Definition` et `File` contiennent le texte source complet de la définition ou du fichier. Ces champs sont disponibles pour les outils d'agent qui doivent charger le contenu des fichiers sans effectuer d'appels API séparés à GitLab.
- Tous les nœuds incluent une propriété `traversal_path` utilisée pour le filtrage des autorisations. Les résultats des requêtes sont automatiquement filtrés selon les entités auxquelles l'utilisateur à l'origine de la requête a accès.
