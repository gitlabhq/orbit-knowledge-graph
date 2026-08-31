---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Comment GitLab Orbit Remote sécurise vos données, notamment les rôles requis pour effectuer des requêtes, le modèle d'autorisation et l'accès programmatique."
title: Sécurité de GitLab Orbit Remote
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

Les réponses aux requêtes adressées à GitLab Orbit n'incluent que les informations disponibles pour votre rôle. Si vous ou un agent tentez d'accéder à une partie de GitLab qui nécessite un rôle utilisateur supérieur, les informations correspondantes ne seront pas affichées dans le graphe.

L'accès dans GitLab Orbit est hiérarchique. Un rôle attribué au niveau du groupe principal s'applique à chaque sous-groupe et projet qui en dépend. L'activation de GitLab Orbit ne modifie pas les accès existants.

## Rôles requis pour interroger GitLab Orbit {#roles-required-to-query-gitlab-orbit}

Pour interroger un groupe, vous devez disposer au minimum du rôle Reporter pour ce groupe.

L'accès aux données de sécurité nécessite le rôle Responsable sécurité. Cela inclut les données suivantes :

- Vulnérabilités
- Résultats de sécurité
- Analyses de sécurité
- Scanners
- Identifiants CVE/CWE

Le rôle Responsable sécurité est requis car les résultats agrégés ne peuvent pas être filtrés après l'exécution de la requête, ce qui pourrait sinon exposer des détails de sécurité aux utilisateurs disposant du rôle Reporter. Un utilisateur disposant du rôle Reporter peut interroger le reste du graphe, mais les entités de sécurité sont exclues des résultats, y compris des comptages agrégés.

| Domaine de données | Rôle minimum |
|---|---|
| Core, revue de code, CI/CD, planification | Rapporteur |
| Sécurité | Responsable sécurité |

## Architecture de sécurité {#security-architecture}

GitLab Orbit ne crée jamais de permissions de toute pièce. GitLab est la source unique de vérité pour déterminer qui peut voir quoi, et chaque requête est autorisée via GitLab.

L'accès est appliqué selon les couches suivantes :

- Isolation de l'organisation. Une requête ne voit jamais que les données de votre propre organisation.
- Portée hiérarchique basée sur les rôles. Les résultats sont limités aux groupes, sous-groupes et projets pour lesquels vous disposez du rôle requis. Les groupes frères restent hors de portée.
- Vérifications sur chaque résultat. Avant que les résultats ne soient retournés, GitLab vérifie à nouveau vos permissions sur chaque élément et supprime tout ce à quoi vous ne pouvez pas accéder. Cela permet de détecter les éléments confidentiels et les contrôles d'exécution tels que les liens de groupes SAML et les restrictions d'adresse IP.

Les [restrictions d'adresse IP](https://docs.gitlab.com/user/group/access_and_permissions/#restrict-group-access-by-ip-address) de groupe s'appliquent aux résultats des requêtes : une requête provenant d'une adresse IP en dehors des plages autorisées d'un groupe ne retourne aucun résultat de ce groupe.

GitLab Orbit est en lecture seule. Il lit les modifications depuis GitLab sans jamais les réécrire, s'exécute dans un environnement séparé et ne stocke aucune donnée de permissions qui lui soit propre.

## Accès programmatique {#programmatic-access}

L'accès programmatique utilise votre authentification GitLab existante, dont la portée est limitée à ce que le propriétaire du jeton peut voir dans GitLab.

- API REST : un jeton d'accès personnel standard (hérité) avec la portée `read_api`, envoyé en tant que jeton Bearer. Les jetons d'accès personnels à granularité fine ne sont pas pris en charge. Pour plus d'informations, consultez [API REST](access/api.md).
- MCP : GitLab OAuth. Les clients HTTP natifs demandent la portée `mcp_orbit`. Pour plus d'informations, consultez [MCP](access/mcp.md).
- GitLab Duo Agent Platform : aucun jeton à configurer. Pour plus d'informations, consultez [GitLab Duo Agent Platform](access/duo.md).
