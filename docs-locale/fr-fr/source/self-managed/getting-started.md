---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Prérequis, ordre d'installation et valeurs de configuration partagées pour GitLab Orbit sur GitLab Self-Managed."
title: Premiers pas avec GitLab Orbit sur GitLab Self-Managed
---

{{< details >}}

- Édition : GitLab Premium, GitLab Ultimate
- Offre : GitLab Self-Managed
- Statut : version bêta

{{< /details >}}

{{< history >}}

- [Introduit](https://gitlab.com/groups/gitlab-org/-/epics/22739) dans GitLab 19.2.2.

{{< /history >}}

> [!note]
GitLab Orbit sur GitLab Self-Managed est en [version bêta](https://docs.gitlab.com/policy/development_stages_support/#beta). Cette fonctionnalité est disponible pour être testée, mais elle n'est pas prête pour une utilisation en production.

GitLab Orbit dépend de trois systèmes qu'il n'installe pas : ClickHouse, Kubernetes et NATS. Les étapes de configuration restantes nécessitent que ces trois systèmes existent et soient accessibles.

## Prérequis {#prerequisites}

- GitLab 19.2.2 ou version ultérieure.
- Accès administrateur à l'instance GitLab et à son serveur PostgreSQL.
- Une fenêtre de maintenance pour un redémarrage de PostgreSQL.
- ClickHouse 26.2 ou version ultérieure, configuré pour GitLab.
- Kubernetes 1.33 ou version ultérieure.
- NATS avec JetStream activé.

## Ordre d'installation {#installation-order}

Chaque étape dépend de l'étape précédente. Installez les composants dans cet ordre :

1. Configurez ClickHouse pour GitLab et exécutez les migrations GitLab ClickHouse.
1. Installez NATS sur le cluster avec JetStream activé.
1. [Configurez la réplication des données](data-replication.md) : préparez PostgreSQL, puis installez Siphon. Après cette étape, les lignes GitLab arrivent dans le lac de données ClickHouse.
1. [Configurez GitLab Orbit](orbit-setup.md) : créez les identités ClickHouse, installez le chart, pointez GitLab vers GitLab Orbit et activez l'indexation pour un groupe principal.

Les étapes 1 à 3 ne dépendent pas de GitLab Orbit. Vous pouvez vérifier chacune d'elles indépendamment.

## ClickHouse {#clickhouse}

GitLab Orbit nécessite ClickHouse 26.2 ou version ultérieure, car le graphe utilise des index de texte intégral et des expressions de table communes matérialisées introduites dans cette release. GitLab prend en charge toute release 25.x ou 26.x ; ainsi, une release 25.x répond aux besoins du reste de GitLab, mais pas de GitLab Orbit. Toutes les autres exigences GitLab pour ClickHouse restent inchangées.

Configurez d'abord ClickHouse pour GitLab. Pour plus d'informations, consultez [ClickHouse](https://docs.gitlab.com/integration/clickhouse/). Effectuez chaque étape de cette page, notamment [Run ClickHouse migrations](https://docs.gitlab.com/integration/clickhouse/#run-clickhouse-migrations) et [Enable ClickHouse for analytics](https://docs.gitlab.com/integration/clickhouse/#enable-clickhouse-for-analytics). Ces migrations créent les tables du lac de données dans lesquelles la réplication écrit. Sans ces tables, Siphon n'a aucune cible vers laquelle écrire et la réplication échoue.

GitLab Orbit utilise deux bases de données :

| Base de données | Écrit par | Lu par |
|----------|------------|---------|
| `gitlab_clickhouse_main_production` | GitLab, Siphon | GitLab, l'indexeur et le dispatcher GitLab Orbit |
| `orbit` | Le dispatcher GitLab Orbit (schéma) et l'indexeur (données) | Les trois composants GitLab Orbit |

Les deux bases de données peuvent se trouver sur des instances ClickHouse distinctes. Dans ce cas, fournissez à GitLab, Siphon et GitLab Orbit les identifiants de l'instance qui héberge la base de données que chacun utilise.

La base de données `gitlab_clickhouse_main_production` existe une fois la configuration de ClickHouse terminée. Vous créez la base de données `orbit` lors de la configuration de GitLab Orbit. GitLab Orbit ne la crée pas au démarrage.

### Dimensionnement et paramètres {#sizing-and-settings}

Provisionnez au moins 8 CPU et 32 Gio de mémoire pour ClickHouse. ClickHouse sature les 8 cœurs pendant la construction du graphe.

Provisionnez au moins autant de stockage ClickHouse que la taille de la base de données GitLab PostgreSQL.

Une instance ClickHouse que vous gérez vous-même définit `max_bytes_before_external_sort` et `max_bytes_before_external_group_by` à `0`, ce qui désactive le déversement sur le disque. ClickHouse Cloud définit les deux à la moitié de la mémoire disponible. Sans déversement, un tri volumineux conserve l'intégralité du résultat en mémoire et le serveur manque de mémoire. Définissez les deux dans le profil `default`. Les valeurs suivantes conviennent à une instance de 32 Gio, avec 8 Gio pour chaque seuil et 20 Gio pour le plafond mémoire :

```xml
<profiles>
  <default>
    <max_bytes_before_external_sort>8589934592</max_bytes_before_external_sort>
    <max_bytes_before_external_group_by>8589934592</max_bytes_before_external_group_by>
    <max_memory_usage>21474836480</max_memory_usage>
  </default>
</profiles>
```

## Kubernetes {#kubernetes}

GitLab Orbit nécessite Kubernetes 1.33 ou version ultérieure, avec la feature gate `ImageVolume` activée et prise en charge par le runtime de conteneur.

GitLab Orbit peut s'exécuter sur un cluster distinct de GitLab. Ce cluster doit accéder à GitLab, PostgreSQL, ClickHouse et NATS, et GitLab doit accéder à ce cluster.

## NATS {#nats}

Siphon publie chaque ligne modifiée dans un stream NATS JetStream, et Siphon comme GitLab Orbit lisent depuis ce stream. NATS nécessite JetStream activé et un volume persistant.

Définissez le paramètre `max_payload` du serveur NATS à 64 Mo. La valeur par défaut de 1 Mo est inférieure à la taille de certaines lignes GitLab. Siphon lit la valeur du serveur pour déterminer si une ligne est trop volumineuse pour être envoyée.

## Stockage d'objets (facultatif) {#object-storage-optional}

Siphon stocke les événements d'instantané dans un magasin d'objets, ainsi que toute ligne encore trop volumineuse après augmentation de `max_payload`. Par défaut, Siphon utilise un bucket de magasin d'objets NATS JetStream, donc aucun service supplémentaire n'est requis. Pour éviter ce trafic sur le volume JetStream ou pour appliquer une politique de rétention distincte, configurez plutôt un bucket compatible S3 ou Google Cloud Storage.

GitLab Orbit n'utilise pas de stockage d'objets. Le graphe est stocké dans ClickHouse et peut être reconstruit par une nouvelle indexation. L'indexeur nécessite un disque local au nœud pour les extractions de dépôt. Le chart dimensionne ce disque avec des requêtes de stockage éphémère.

## Valeurs de configuration partagées {#shared-configuration-values}

Choisissez chacune de ces valeurs une seule fois et utilisez la même valeur dans chaque composant qui la lit. Aucun composant ne valide ces valeurs par rapport aux autres. Si elles ne correspondent pas, le composant concerné démarre mais ne traite aucune donnée.

| Valeur | Utilisé par | Exemple |
|-------|---------|---------|
| Nom du stream NATS | Siphon et GitLab Orbit | `siphon_stream_main_db` |
| Base de données du lac de données | GitLab, Siphon et GitLab Orbit | `gitlab_clickhouse_main_production` |
| Base de données du graphe | GitLab Orbit | `orbit` |
| Hôte et port PostgreSQL | Siphon | `postgres.example.com:5432` |
| URL GitLab accessible depuis le cluster | GitLab Orbit | `https://gitlab.example.com` |
| Point de terminaison gRPC GitLab Orbit accessible depuis GitLab | GitLab | `tls://orbit.example.com:50054` |

GitLab Orbit attend par défaut le nom de stream `siphon_stream_main_db`. Pour utiliser un nom différent, définissez `stream_name` dans `siphon-values.yaml` et `schedule.tasks.siphon.events_stream_name` dans `orbit-values.yaml`.

## Étape suivante {#next-step}

- [Configurer la réplication des données](data-replication.md)
