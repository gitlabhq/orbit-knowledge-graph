---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Installez le skill GitLab Orbit pour donner aux agents de codage IA des recettes de requêtes prêtes à l'emploi, des conseils DSL et des informations de dépannage pour GitLab Orbit Remote et GitLab Orbit Local."
title: Configurer des agents de codage IA avec le skill GitLab Orbit
---

{{< details >}}

- Édition : Gratuite, GitLab Premium, GitLab Ultimate
- Offre : GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Statut : version bêta

{{< /details >}}

Le skill GitLab Orbit fournit aux agents de codage IA des conseils structurés pour interroger le graphe GitLab Orbit. Il comprend :

- **Query recipes** \- corps JSON prêts à coller pour les questions courantes (rayon d'impact, historique de pipeline, modèles de contribution).
- **DSL reference** \- le langage de requête complet permettant aux agents de composer des requêtes valides dès la première tentative.
- **Troubleshooting** \- codes de sortie, diagnostics de résultats vides et pièges courants.
- **Repository map helpers** \- scripts qui résument la structure du code source depuis un checkout local ou depuis GitLab Orbit Remote.

Le skill fonctionne avec [GitLab Orbit Remote](remote/_index.md) et [GitLab Orbit Local](local/_index.md).

## Prérequis {#prerequisites}

- [GitLab CLI (`glab`)](https://docs.gitlab.com/cli/) v1.95.0 ou version ultérieure, qui a introduit `glab skills install`. Si la sous-commande n'est pas reconnue, mettez d'abord à jour `glab`.

## Installer le skill {#install-the-skill}

Installer globalement (disponible pour tous les projets) :

```shell
glab skills install --global orbit
```

Cette commande installe le skill dans `~/.agents/skills/orbit`.

Installer pour le projet actuel uniquement :

```shell
glab skills install orbit
```

Cette commande installe le skill dans `.agents/skills/orbit` à la racine du projet.

Si le skill est déjà installé, `glab` indique que `SKILL.md` existe et suggère `--force` pour écraser.

## Mettre à jour le skill GitLab Orbit {#update-the-gitlab-orbit-skill}

Pour mettre à jour vers la dernière version, réexécutez la commande d'installation avec `--force` :

```shell
glab skills install --global --force orbit
```
