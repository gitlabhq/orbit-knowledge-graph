---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Configurer la réplication logique PostgreSQL et installer Siphon pour que les données GitLab atteignent ClickHouse.
title: Configurer la réplication des données
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

GitLab Orbit lit les données GitLab à partir d'une copie de la base de données GitLab dans ClickHouse, et non à partir de la base de données GitLab elle-même. [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon) maintient cette copie à jour.

Siphon s'exécute sous la forme de trois déploiements sur Kubernetes :

| Déploiement | Rôle |
|------------|------|
| Producer | Lit le journal write-ahead PostgreSQL et publie chaque ligne modifiée vers NATS. |
| Consumer | Lit NATS et écrit les lignes dans ClickHouse. |
| Reconciler | Recalcule les colonnes dérivées, telles que le chemin de traversée de l'espace de nommage, selon un calendrier, et republie les lignes affectées via NATS. |

Prérequis :

- Les [prérequis](getting-started.md#prerequisites) pour GitLab Orbit sur GitLab Self-Managed.
- Une instance ClickHouse configurée pour GitLab, avec les migrations GitLab ClickHouse appliquées. Pour plus d'informations, consultez [ClickHouse](getting-started.md#clickhouse).
- Accès superutilisateur au serveur PostgreSQL GitLab.
- Une fenêtre de maintenance pour un redémarrage de PostgreSQL.
- Helm 3 et accès `kubectl` au cluster.

Configurez la réplication dans cet ordre :

1. Activer la réplication logique dans PostgreSQL.
1. Créer les utilisateurs PostgreSQL Siphon.
1. Créer la publication et les droits.
1. Créer l'utilisateur ClickHouse Siphon.
1. Rendre les mots de passe disponibles pour Siphon.
1. Installer Siphon.

## Activer la réplication logique dans PostgreSQL {#turn-on-logical-replication-in-postgresql}

Le producer Siphon s'arrête au démarrage sauf si `wal_level` est défini sur `logical`. Les modifications apportées à `wal_level`, `max_replication_slots` et `max_wal_senders` nécessitent toutes un redémarrage complet de PostgreSQL, pas un rechargement, et `gitlab-ctl reconfigure` ne redémarre pas PostgreSQL.

{{< tabs >}}

{{< tab title="Paquet Linux (Omnibus)" >}}

1. Modifiez `/etc/gitlab/gitlab.rb` :

   ```ruby
   postgresql['wal_level'] = 'logical'
   postgresql['max_replication_slots'] = 10
   postgresql['max_wal_senders'] = 10

   # Accept connections from the cluster. Replace with the address and CIDR for your network.
   postgresql['listen_address'] = '0.0.0.0'
   postgresql['md5_auth_cidr_addresses'] = ['10.0.0.0/8']

   # Keep GitLab itself on the local socket. Without this, setting listen_address
   # also repoints GitLab at that address, and GitLab loses its database connection.
   gitlab_rails['db_host'] = '/var/opt/gitlab/postgresql'
   ```

1. Enregistrez le fichier, reconfigurez GitLab, puis redémarrez PostgreSQL :

   ```shell
   sudo gitlab-ctl reconfigure
   sudo gitlab-ctl restart postgresql
   ```

1. Confirmez les paramètres :

   ```shell
   sudo gitlab-psql -c 'SHOW wal_level'
   sudo gitlab-psql -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level` retourne `logical`, les deux autres correspondent à `gitlab.rb`, et aucune ligne ne signale `pending_restart = t`. Une combinaison partiellement appliquée laisse PostgreSQL incapable de démarrer lors de son prochain redémarrage. Pour récupérer, corrigez `/etc/gitlab/gitlab.rb`, reconfigurez, et confirmez que le service est en cours d'exécution.

{{< /tab >}}

{{< tab title="Chart Helm (Kubernetes)" >}}

Le chart Helm GitLab ne gère pas un PostgreSQL de production ; appliquez donc ces paramètres sur votre propre serveur ou base de données managée.

1. Définissez les paramètres suivants :

   | Paramètre | Valeur |
   |-----------|-------|
   | `wal_level` | `logical` |
   | `max_replication_slots` | `10` ou plus |
   | `max_wal_senders` | `10` ou plus |

   Sur une base de données managée, définissez-les via le groupe de paramètres du fournisseur plutôt que dans `postgresql.conf`. Certains fournisseurs exposent `wal_level` sous un nom différent, comme un indicateur de réplication logique.

1. Redémarrez le serveur.

1. Autorisez les connexions depuis le cluster à atteindre le port 5432.

1. Confirmez les paramètres :

   ```shell
   psql -h <postgresql_host> -U <admin_user> -d gitlabhq_production \
     -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level` retourne `logical`, les deux autres correspondent aux valeurs que vous avez définies, et aucune ligne ne signale `pending_restart = t`.

{{< /tab >}}

{{< /tabs >}}

## Créer les utilisateurs PostgreSQL Siphon {#create-the-siphon-postgresql-users}

Siphon utilise trois rôles. Créez-les en tant que superutilisateur. Un rôle ne peut accorder `REPLICATION` que s'il possède déjà l'attribut `REPLICATION`, et le rôle de l'application GitLab ne le possède pas.

Connectez-vous à `gitlabhq_production` en tant que superutilisateur et exécutez :

```sql
CREATE USER siphon WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_replicator WITH REPLICATION LOGIN PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_snapshot WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
```

Les trois rôles peuvent utiliser le même mot de passe.

Avec le fichier de valeurs de cette procédure, Siphon se connecte en tant que `siphon_replicator` pour chaque connexion. Les deux autres rôles existent parce que la tâche Rake de l'étape suivante leur accorde également un accès en lecture. Une configuration avec utilisateurs séparés peut les utiliser ultérieurement.

## Créer la publication et les droits {#create-the-publication-and-grants}

GitLab fournit une tâche Rake qui prépare PostgreSQL pour Siphon. La tâche est idempotente et crée :

- La publication.
- La fonction helper que Siphon appelle pour ajouter des tables à la publication.
- L'accès en lecture à chaque schéma GitLab.

GitLab 19.2.2 et versions ultérieures incluent la tâche.

{{< tabs >}}

{{< tab title="Paquet Linux (Omnibus)" >}}

1. Confirmez que la tâche existe :

   ```shell
   sudo gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. Exécutez la tâche :

   ```shell
   sudo gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< tab title="Chart Helm (Kubernetes)" >}}

1. Trouvez le déploiement toolbox :

   ```shell
   kubectl -n <gitlab_namespace> get deploy -l app=toolbox
   ```

1. Confirmez que la tâche existe :

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. Exécutez la tâche :

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< /tabs >}}

La tâche crée une publication nommée `siphon_publication_main_1` et accorde `EXECUTE` sur `public.siphon_alter_publication` au rôle `siphon`. Le producer appelle la fonction sur la connexion `siphon_replicator`, donc accordez la même permission `EXECUTE` à `siphon_replicator`. En tant que superutilisateur, exécutez :

```sql
GRANT EXECUTE ON FUNCTION public.siphon_alter_publication(text, text, integer)
  TO siphon_replicator;
```

La publication est vide jusqu'au démarrage du producer. Siphon ajoute ensuite des tables à la publication via la même fonction.

## Créer l'utilisateur ClickHouse Siphon {#create-the-siphon-clickhouse-user}

Siphon écrit dans la même base de données que celle déjà utilisée par GitLab. Siphon ne nécessite aucun droit sur le schéma, car les migrations GitLab ClickHouse créent les tables cibles.

Connectez-vous à ClickHouse en tant qu'administrateur et exécutez :

```sql
CREATE USER siphon IDENTIFIED WITH sha256_password BY '<your_password>';
CREATE ROLE siphon_app;
GRANT SELECT, INSERT, dictGet ON gitlab_clickhouse_main_production.* TO siphon_app;
GRANT siphon_app TO siphon;
```

L'autorisation `dictGet` est obligatoire. Sans elle, les pods réussissent toujours leurs vérifications d'état, car la sonde ne fait que scraper le point de terminaison des métriques. L'échec apparaît sous forme d'erreurs de permission dans le journal du consumer. Chaque table associée à un dictionnaire reste vide.

Sur un ClickHouse managé qui restreint la base de données `system`, exécutez également :

```sql
GRANT SELECT ON system.tables, system.columns TO siphon_app;
```

## Rendre les mots de passe disponibles pour Siphon {#make-the-passwords-available-to-siphon}

Siphon lit les deux mots de passe à partir de variables d'environnement associées à un Secret Kubernetes. L'espace de nommage doit exister avant de créer le Secret. Le fichier de valeurs de la section suivante attend un Secret nommé `siphon-secrets` dans l'espace de nommage Siphon, avec ces clés :

| Clé | Contient |
|-----|-------|
| `pg-password` | Le mot de passe pour les rôles PostgreSQL `siphon`, `siphon_replicator` et `siphon_snapshot` |
| `ch-siphon-password` | Le mot de passe pour l'utilisateur ClickHouse `siphon` |

Vous devez conserver les valeurs dans le gestionnaire de secrets que vous utilisez déjà. Synchronisez-les dans le cluster avec un outil tel que l'External Secrets Operator. Ne stockez pas le texte en clair ailleurs.

## Installer Siphon {#install-siphon}

Avec `configMode: split`, Siphon construit sa liste de tables au démarrage du pod à partir d'une image fournie avec GitLab. La liste correspond alors à votre version de GitLab et ne nécessite aucune mise à jour manuelle.

`global.gitlabVersion` est le tag de l'image `gitlab-siphon-tables`, qui épingle l'ensemble de tables répliquées à votre version GitLab. Les tags commencent à `v19.2.0-ee`. Confirmez que le tag existe dans le [registre de conteneurs](https://gitlab.com/gitlab-org/gitlab/container_registry) avant d'installer, car un tag inexistant empêche les pods de démarrer.

1. Enregistrez ce qui suit sous `siphon-values.yaml` et remplacez les espaces réservés :

   ```yaml
   configMode: split

   global:
     # Tag of the gitlab-siphon-tables image.
     # Must match your GitLab version exactly, patch level included.
     gitlabVersion: v19.2.2-ee

   image:
     repository: registry.gitlab.com/gitlab-org/analytics-section/siphon
     tag: 0.0.124-beta

   siphonConnectionConfigMap:
     create: true
     data:
       prometheus:
         port: 8080
       connection:
         queueing:
           driver: nats
           url: nats://nats.nats.svc.cluster.local:4222
           stream_name: siphon_stream_main_db
           nats_config:
             replicas: 1
             max_age_seconds: 1296000
         clickhouse:
           host: <clickhouse_host>
           # Native protocol, not the HTTP port GitLab uses.
           port: 9000
           ssl: false
           username: siphon
           password: "${CLICKHOUSE_SIPHON_PASSWORD}"
           database: gitlab_clickhouse_main_production
         databases:
           main:
             host: <postgresql_host>
             port: 5432
             database: gitlabhq_production
             user: siphon_replicator
             password: "${SIPHON_DB_PASSWORD}"
             ssl_mode: require
             advisory_lock_id: 1
             application_name: siphon_main_1
             advisory_lock_timeout_ms: 100
             advisory_lock_timeout_fuzziness_ms: 50
             lock_timeout_ms: 500
             lock_timeout_fuzziness_ms: 300
       overrides:
         siphon_main_1:
           database_ref: main

   siphonLayoutConfigMap:
     create: true
     data:
       stream_name: siphon_stream_main_db
       refresh_mode: inline
       partitions_monitoring_interval_in_seconds: 3600
       max_column_size_in_bytes: 10485760
       producers:
         main:
           - siphon_main_1
       consumers:
         - siphon_consumer_1
       reconcilers:
         - siphon_reconciler_1
       # Point the producer at the publication the Rake task created. Without this
       # override, the producer derives the name from the application identifier
       # and tries to create its own.
       replication_overrides:
         siphon_main_1:
           publication_name: siphon_publication_main_1
       # GitLab splits its schema three ways even on one database. Map ci and sec onto main.
       database_mapping:
         ci: main
         sec: main

   deployments:
     postgres-producer:
       configMode: split
       split:
         role: producer
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
     clickhouse-consumer:
       configMode: split
       split:
         role: consumer
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
     reconciler:
       configMode: split
       split:
         role: reconciler
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
   ```

1. Installez le chart :

   ```shell
   helm repo add siphon https://gitlab.com/api/v4/projects/76780115/packages/helm/stable
   helm repo update

   helm upgrade --install siphon siphon/siphon \
     --version 1.18.0 \
     --namespace siphon \
     --create-namespace \
     --values siphon-values.yaml
   ```

   Ces commandes constituent une référence pour une installation Helm directe. Ajustez les noms d'espace de nommage et la méthode de déploiement pour correspondre à vos propres outils.

1. Confirmez que les trois déploiements sont en cours d'exécution :

   ```shell
   kubectl -n siphon get pods
   ```

La sortie liste les pods `postgres-producer`, `clickhouse-consumer` et `reconciler` dans l'état `Running`.

Siphon ne redémarre pas ses pods lorsque vous modifiez le fichier de valeurs. Pour appliquer une modification, redémarrez les déploiements :

```shell
kubectl -n siphon rollout restart deployment
```

### Paramètres du fichier de valeurs {#values-file-settings}

| Paramètre | Exigence |
|---------|-------------|
| `database_mapping` | Requis. Le schéma GitLab est réparti sur trois bases de données logiques (`main`, `ci` et `sec`), que votre instance les stocke séparément ou non. Sans le mappage, le générateur échoue. Ne supprimez pas les définitions de tables qui référencent `ci` et `sec` à la place, car cela supprime silencieusement chaque table CI, vulnérabilité et dépendance. |
| `stream_name` | Doit correspondre au nom de stream que GitLab Orbit lit. Pour la liste complète des valeurs partagées par les deux côtés, voir [Valeurs de configuration partagées](getting-started.md#shared-configuration-values). |
| `advisory_lock_id` et les délais d'expiration de verrou | Requis. Le producer s'arrête au démarrage sans eux. |
| `nats_config.replicas` | Doit correspondre à la taille de votre cluster NATS. Un seul serveur NATS ne prend en charge qu'un seul réplica. |
| `ssl_mode` | Doit correspondre à ce que le serveur PostgreSQL propose. L'exemple utilise `require`, qui fonctionne avec un PostgreSQL de package Linux car il sert TLS par défaut. Un serveur qui ne sert pas TLS refuse la connexion, et le producer s'arrête au démarrage avec `server refused TLS connection`. |
| `connection.replication.use_alter_publication_function` | Doit rester `true`, qui est la valeur par défaut du chart. L'autorisation `EXECUTE` sur `public.siphon_alter_publication` existe pour ce paramètre : la publication appartient à l'utilisateur de la base de données GitLab, donc un `ALTER PUBLICATION` direct depuis un rôle Siphon échoue. |
| `max_age_seconds` | Contrôle jusqu'où un consumer peut rejouer. Conserver 15 jours de chaque ligne modifiée sur plus de 60 tables produit un large fichier de stockage JetStream. Dimensionnez le volume NATS pour la fenêtre de rétention complète, ou réduisez la valeur. |

### Stockage d'objets pour les grandes lignes {#object-storage-for-large-rows}

Les lignes trop volumineuses pour le stream sont envoyées vers un magasin d'objets. Le fichier de valeurs précédent n'en configure pas, donc Siphon utilise un magasin d'objets NATS JetStream et n'a besoin d'aucune accréditation.

Pour utiliser un bucket externe, ajoutez `object_storage_config` sous `queueing` avec `identifier`, `type` et `bucket_name`, et donnez à chaque pod Siphon les accréditations pour le bucket. Avec `type: s3`, Siphon suit la chaîne SDK AWS standard ; définissez donc `AWS_REGION` et attachez un rôle d'instance ou transmettez `AWS_ACCESS_KEY_ID` et `AWS_SECRET_ACCESS_KEY` via `env` et `envFromSecrets`. Pour un service compatible S3, définissez également `AWS_ENDPOINT_URL_S3`. Pour Google Cloud Storage, utilisez `type: gcs` avec les accréditations d'application par défaut.

## Vérifier la réplication {#verify-replication}

La première copie traite une table à la fois et suspend la réplication pendant la fusion de chaque table. La durée dépend donc du nombre de tables, et non de la quantité de données. Une instance GitLab 19.2.2 réplique plus de 60 tables. Ces vérifications confirment que la réplication est en cours d'exécution. Elles ne confirment pas que la première copie est terminée.

1. Confirmez que Siphon lit PostgreSQL. Le slot doit être actif, et `confirmed_flush_lsn` doit avancer après une écriture dans GitLab :

   ```sql
   SELECT slot_name, active, wal_status, confirmed_flush_lsn
   FROM pg_replication_slots
   WHERE slot_name = 'siphon_main_1_slot';
   ```

1. Confirmez que les lignes arrivent dans ClickHouse :

   ```sql
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_namespaces FINAL;
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_projects FINAL;
   ```

   Comparez les comptages avec les mêmes tables dans PostgreSQL. `FINAL` est obligatoire, car les tables sont de type `ReplacingMergeTree` et une ligne mise à jour apparaît plus d'une fois jusqu'à ce qu'une fusion en arrière-plan soit terminée. Les comptages ClickHouse sont généralement légèrement supérieurs, car une table peut recevoir une ligne de remplacement lors du premier enregistrement.

> [!warning]
Un slot de réplication inactif conserve le journal write-ahead sur le disque PostgreSQL GitLab et peut le saturer. Si vous arrêtez Siphon pendant plus longtemps que ne le permet l'espace disque disponible, supprimez le slot avec `SELECT pg_drop_replication_slot('siphon_main_1_slot');` et prenez un nouvel instantané ultérieurement.

## Étape suivante {#next-step}

- [Configurer GitLab Orbit](orbit-setup.md)
