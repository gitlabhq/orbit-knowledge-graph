---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Découvrez quelles données vous pouvez indexer avec GitLab Orbit Local et GitLab Orbit Remote.
title: Indexer des données avec GitLab Orbit
---

GitLab Orbit indexe des données provenant de plusieurs sources, notamment des dépôts de code et des données SDLC associées à un groupe principal. Une fois que GitLab Orbit a indexé les données, il construit un graphe de connaissances que vous pouvez interroger pour récupérer les relations, les dépendances et le contexte structurel de votre base de code.

Utilisez GitLab Orbit Remote pour indexer les données d'un groupe principal et de ses sous-groupes et projets.

Utilisez GitLab Orbit Local pour indexer les données de l'arbre de travail de tout dépôt local.

## Données indexées par GitLab Orbit {#what-data-gitlab-orbit-indexes}

GitLab Orbit Local et GitLab Orbit Remote indexent différents types de données. Les sections suivantes répertorient ce que chaque fonctionnalité indexe.

GitLab Orbit Remote et GitLab Orbit Local n'indexent pas :

- Les fichiers binaires
- Les branches autres que la branche extraite (GitLab Orbit Local) ou la branche par défaut (GitLab Orbit Remote)

### Code source {#source-code}

| Structure du code | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------|--------|
| Fichiers et répertoires | {{< yes >}} | {{< yes >}} |
| Définitions de fonctions, classes, méthodes et modules | {{< yes >}} | {{< yes >}} |
| Déclarations d'importation | {{< yes >}} | {{< yes >}} |
| Références de symboles inter-fichiers | {{< yes >}} | {{< yes >}} |

### Groupes, projets et utilisateurs {#groups-projects-and-users}

| Groupes, projets et utilisateurs | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Groupes | {{< no >}} | {{< yes >}} |
| Projets | {{< no >}} | {{< yes >}} |
| Utilisateurs | {{< no >}} | {{< yes >}} |
| Notes et commentaires | {{< no >}} | {{< yes >}} |

### Revue de code {#code-review}

| Revue de code | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Les merge requests | {{< no >}} | {{< yes >}} |
| Diffs de merge request | {{< no >}} | {{< yes >}} |
| Fichiers modifiés | {{< no >}} | {{< yes >}} |

### Pipelines CI/CD {#cicd-pipelines}

| CI/CD | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Pipelines | {{< no >}} | {{< yes >}} |
| Étapes | {{< no >}} | {{< yes >}} |
| Jobs | {{< no >}} | {{< yes >}} |

### Planification du code {#code-planning}

| Planification du code | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Les tickets | {{< no >}} | {{< yes >}} |
| Les epics | {{< no >}} | {{< yes >}} |
| Tâches | {{< no >}} | {{< yes >}} |
| Incidents | {{< no >}} | {{< yes >}} |
| Les jalons | {{< no >}} | {{< yes >}} |
| Labels | {{< no >}} | {{< yes >}} |

### Sécurité {#security}

| Sécurité | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Vulnérabilités | {{< no >}} | {{< yes >}} |
| Résultats de sécurité | {{< no >}} | {{< yes >}} |
| Analyses de sécurité | {{< no >}} | {{< yes >}} |
| Scanners | {{< no >}} | {{< yes >}} |
| Identifiants CVE | {{< no >}} | {{< yes >}} |
| Identifiants CWE | {{< no >}} | {{< yes >}} |

### Portée du travail {#work-scope}

| Portée | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| Arbre de travail (code extrait) | {{< yes >}} | {{< no >}} |
| Branche par défaut uniquement | {{< no >}} | {{< yes >}} |
| Plusieurs dépôts | {{< yes >}} | {{< no >}} |
| Sélection de branche | {{< no >}} | {{< no >}} |

## Langages pris en charge {#supported-languages}

GitLab Orbit Remote et Local indexent les données pour les langages suivants :

| Langage | Définitions | Références inter-fichiers |
|----------|-------------|----------------------|
| Ruby | {{< yes >}} | {{< yes >}} |
| Java | {{< yes >}} | {{< yes >}} |
| Kotlin | {{< yes >}} | {{< yes >}} |
| Python | {{< yes >}} | {{< yes >}} |
| TypeScript | {{< yes >}} | {{< yes >}} |
| JavaScript | {{< yes >}} | {{< yes >}} |
| Rust | {{< yes >}} | {{< yes >}} |
| Go | {{< yes >}} | {{< yes >}} |
| C# | {{< yes >}} | {{< yes >}} |
| C | {{< yes >}} | {{< yes >}} |
| C++ | {{< yes >}} | {{< yes >}} |
| PHP | {{< yes >}} | {{< yes >}} |
| Bash/Shell | {{< yes >}} | {{< no >}} |
