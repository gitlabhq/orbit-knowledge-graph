---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Utilisez les outils disponibles, comme l'interface de ligne de commande GitLab Orbit ou le serveur MCP local de GitLab Orbit, pour interagir avec votre graphe local."
title: Connecter vos outils
---

{{< details >}}

- Édition : Gratuite, GitLab Premium, GitLab Ultimate
- Offre : GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Statut : version bêta

{{< /details >}}

{{< history >}}

- [Introduit](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) dans GitLab 19.0 en tant que [version expérimentale](https://docs.gitlab.com/policy/development_stages_support/#experiment).
- [Passage](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324) en [version bêta](https://docs.gitlab.com/policy/development_stages_support/#beta) dans GitLab 19.1.

{{< /history >}}

GitLab Orbit Local fournit des outils que vous pouvez utiliser pour accéder à votre graphe local et interagir avec des agents d'IA de codage.

## GitLab Orbit Local avec l'interface de ligne de commande GitLab Orbit {#gitlab-orbit-local-with-the-gitlab-orbit-cli}

Avec l'interface de ligne de commande GitLab Orbit, vous pouvez créer un graphe local et l'interroger sans connexion à une instance GitLab.

Vous pouvez également exposer votre graphe à des agents d'IA avec le serveur MCP local de GitLab Orbit et configurer des assistants de codage basés sur l'IA pour consulter votre graphe.

## GitLab Orbit Local avec l'interface de ligne de commande GitLab {#gitlab-orbit-local-with-the-gitlab-cli}

Étendez l'interface de ligne de commande GitLab pour installer et exécuter des commandes de l'interface de ligne de commande GitLab Orbit. L'interface de ligne de commande GitLab gère également les mises à jour à votre place et installe une compétence GitLab Orbit qui met à jour les agents avec des recettes de requêtes, des conseils DSL et une aide à la résolution des problèmes.

## Serveur MCP local de GitLab Orbit {#gitlab-orbit-local-mcp-server}

Connectez le serveur MCP local de GitLab Orbit à un client IA. Une fois connecté à un client, vous pouvez configurer des agents pour qu'ils interagissent avec votre graphe local.
