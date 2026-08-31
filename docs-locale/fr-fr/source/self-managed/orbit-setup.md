---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "Installez GitLab Orbit sur Kubernetes, connectez-y GitLab et indexez votre premier groupe."
title: Configurer GitLab Orbit
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

GitLab Orbit s'exécute en tant que release Helm à côté de votre instance. L'indexeur lit le lac de données ClickHouse et récupère le code source via l'API interne de GitLab. Le serveur web répond aux requêtes du graphe.

GitLab Orbit appelle GitLab via HTTP, et GitLab appelle GitLab Orbit via gRPC sur le port 50054. Les deux directions doivent être ouvertes. GitLab Orbit ne se connecte jamais directement à Gitaly.

Prérequis :

- [Réplication des données](data-replication.md) en cours d'exécution, avec des lignes arrivant dans le lac de données.
- Le rôle Owner pour le groupe que vous souhaitez indexer.
- Accès administrateur à GitLab.

Configurez GitLab Orbit dans cet ordre :

1. Créez la base de données ClickHouse et les identités.
1. Activez GitLab Orbit dans GitLab.
1. Rendez les informations d'identification disponibles pour GitLab Orbit.
1. Installez GitLab Orbit.
1. Activez l'indexation pour un groupe.

## Créer la base de données ClickHouse et les identités {#create-the-clickhouse-database-and-identities}

GitLab Orbit nécessite sa propre base de données de graphe et trois utilisateurs :

| Utilisateur | Rôle | Accès |
|------|------|--------|
| `gkg_writer` | `gkg_app` | Lire et écrire dans la base de données de graphe |
| `gkg_reader` | `gkg_reader_app` | Lire la base de données de graphe |
| `gkg_siphon_reader` | `gkg_siphon_reader_app` | Lire le lac de données |

Les instructions qui créent la base de données et les utilisateurs sont incluses dans l'image GitLab Orbit, de sorte qu'elles correspondent toujours à la version que vous exécutez. Les instructions sont idempotentes. GitLab Orbit ne crée pas la base de données de graphe au démarrage, et le dispatcher n'exécute les migrations de schéma que sur une base de données déjà existante.

Pour créer la base de données et les identités :

1. Lisez les instructions :

   ```shell
   docker run --rm --entrypoint cat \
     registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.96.0 \
     /usr/share/gkg/clickhouse-setup.sql
   ```

1. Remplacez le nom de la base de données de graphe, le nom de la base de données du lac de données et un mot de passe pour chaque utilisateur. Utilisez `orbit` comme nom de base de données de graphe.

1. Exécutez le résultat sur ClickHouse en tant qu'administrateur. Les instructions créent la base de données de graphe et accordent au rôle `gkg_app` les privilèges dont il a besoin sur cette base de données.

1. Ajoutez les trois autorisations que le fichier fourni n'inclut pas :

   ```sql
   GRANT SELECT ON system.parts TO gkg_app;
   GRANT SELECT ON system.tables TO gkg_app;
   GRANT SELECT ON system.dictionaries TO gkg_reader_app;
   ```

Sur une instance ClickHouse que vous gérez vous-même, chaque utilisateur peut lire la base de données `system`, ces autorisations ne changent donc rien. Un ClickHouse géré restreint généralement la base de données `system`. Sans les autorisations, les migrations de schéma s'arrêtent lorsqu'une nouvelle version est promue, et la résolution du chemin de requête échoue. Ajoutez les autorisations dans les deux cas. La configuration fonctionne alors sans modification si vous migrez vers un service géré.

GitLab Orbit atteint ClickHouse via l'interface HTTP sur le port 8123, ou le port 8443 avec TLS. Siphon utilise le protocole natif sur le port 9000, ainsi le port HTTP et le port 9000 doivent tous deux être accessibles depuis le cluster.

## Activer GitLab Orbit dans GitLab {#turn-on-gitlab-orbit-in-gitlab}

GitLab et GitLab Orbit s'authentifient mutuellement avec une clé symétrique, et GitLab en est propriétaire. Activez d'abord GitLab Orbit dans GitLab, afin que la clé existe avant de la copier dans le cluster lors d'une étape ultérieure.

Définissez le point de terminaison gRPC avant de commencer. GitLab se connecte au point de terminaison via TLS sur le port 50054. Le schéma dans le paramètre détermine si la connexion est chiffrée, la valeur doit donc commencer par `tls://`.

{{< tabs >}}

{{< tab title="Paquet Linux (Omnibus)" >}}

1. Modifiez `/etc/gitlab/gitlab.rb` :

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   ```

1. Enregistrez le fichier et reconfigurez GitLab :

   ```shell
   sudo gitlab-ctl reconfigure
   ```

   GitLab génère la clé partagée et l'écrit dans `/var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret`.

1. Lisez la clé. Copiez la valeur exactement, sans la réencoder ni la tronquer :

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

Dans une configuration multi-nœuds, chaque nœud Rails doit détenir la même clé. Générez la clé sur un nœud, puis définissez `gitlab_rails['orbit_secret']` avec cette valeur sur les autres nœuds, ou synchronisez `/etc/gitlab/gitlab-secrets.json` avant leur première reconfiguration.

{{< /tab >}}

{{< tab title="Chart Helm (Kubernetes)" >}}

Le chart GitLab ne génère pas la clé, vous devez donc la créer vous-même. Le Secret doit exister avant de mettre à niveau GitLab. Le chart monte le Secret sans indicateur `optional`, donc un Secret manquant laisse chaque nouveau pod dans l'état `ContainerCreating`.

1. Créez une clé de 32 octets aléatoires, encodée en base64 :

   ```shell
   openssl rand -base64 32
   ```

1. Placez cette valeur dans un Secret dans l'espace de nommage de la release GitLab.

1. Référencez le Secret :

   ```yaml
   global:
     appConfig:
       knowledgeGraph:
         enabled: true
         grpcEndpoint: 'tls://orbit.example.com:50054'
         jwtSecret:
           secret: gitlab-orbit-jwt
           key: secret
   ```

{{< /tab >}}

{{< /tabs >}}

GitLab ne peut pas atteindre GitLab Orbit tant que vous n'avez pas installé le chart GitLab Orbit. Les erreurs de connexion avant cette étape sont attendues.

## Rendre les informations d'identification disponibles pour GitLab Orbit {#make-the-credentials-available-to-gitlab-orbit}

GitLab Orbit lit chaque information d'identification depuis sa propre clé dans un Secret Kubernetes. L'espace de nommage doit exister avant de créer le Secret. Le fichier de valeurs de la section suivante attend un Secret nommé `gkg-secrets` dans l'espace de nommage GitLab Orbit, avec ces clés :

| Clé | Contient |
|-----|-------|
| `gitlab-jwt-verifying-key` | La clé partagée générée par GitLab à l'étape précédente, utilisée pour vérifier les jetons provenant de GitLab |
| `gitlab-jwt-signing-key` | La même clé partagée, utilisée pour signer les jetons renvoyés à GitLab |
| `datalake-password` | Le mot de passe pour l'utilisateur ClickHouse `gkg_siphon_reader` |
| `graph-password` | Le mot de passe pour l'utilisateur ClickHouse `gkg_writer` |
| `graph-read-password` | Le mot de passe pour l'utilisateur ClickHouse `gkg_reader` |

Les deux entrées de clé contiennent la même valeur. Pour extraire chaque clé d'un Secret différent, utilisez `secrets.perKey`.

Vous devez conserver les valeurs dans le gestionnaire de secrets que vous utilisez déjà. Synchronisez-les dans le cluster avec un outil tel que l'External Secrets Operator. Ne stockez pas le texte en clair ailleurs.

Vous devez également fournir un certificat TLS pour le point de terminaison gRPC. Pour plus d'informations, consultez [Exigences TLS et réseau](#tls-and-network-requirements).

## Installer GitLab Orbit {#install-gitlab-orbit}

1. Enregistrez ce qui suit sous `orbit-values.yaml` et remplacez les espaces réservés :

   ```yaml
   image:
     tag: "0.96.0"

   secrets:
     perKey:
       gitlabJwtVerifyingKey: {secretName: gkg-secrets}
       gitlabJwtSigningKey: {secretName: gkg-secrets}
       datalakePassword: {secretName: gkg-secrets}
       graphPassword: {secretName: gkg-secrets}
       graphReadPassword: {secretName: gkg-secrets}

   # Off by default. Creates the graph schema on first run and schedules indexing.
   dispatcher:
     enabled: true

   # HTTP interface. For a TLS endpoint such as ClickHouse Cloud,
   # add httpPort: 8443 and ssl: true to both blocks.
   clickhouse:
     datalake:
       host: <clickhouse_host>
       database: gitlab_clickhouse_main_production
       user: gkg_siphon_reader
     graph:
       host: <clickhouse_host>
       database: orbit
       user: gkg_writer
       readUser: gkg_reader

   # The same NATS cluster Siphon publishes to.
   nats:
     url: "nats://nats.nats.svc.cluster.local:4222"

   # The GitLab URL the cluster can reach. If GitLab is exposed under a host name
   # that does not match its certificate, keep baseUrl on the certificate name and
   # add resolveHost with the host to route to.
   gitlab:
     baseUrl: "https://gitlab.example.com"

   webserver:
     service:
       type: ClusterIP

   # Only when the webserver terminates TLS itself. Omit both keys if a load
   # balancer terminates in front of it.
   tls:
     enabled: true
     existingSecret: gkg-webserver-tls
   ```

1. Installez le chart :

   ```shell
   helm upgrade --install gkg \
     oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg \
     --version 1.5.0 \
     --namespace gitlab-orbit \
     --create-namespace \
     --values orbit-values.yaml
   ```

   Cette commande est une référence pour une installation Helm directe. Ajustez les noms d'espace de nommage et la méthode de déploiement pour correspondre à vos propres outils.

1. Confirmez que les trois composants sont en cours d'exécution :

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

La sortie liste les pods du serveur web, de l'indexeur et du dispatcher dans l'état `Running`. Le chart désactive tout le reste par défaut, y compris les métriques, la mise à l'échelle automatique, l'analytique et la facturation. Laissez-les désactivés sur GitLab Self-Managed.

### Exigences TLS et réseau {#tls-and-network-requirements}

GitLab se connecte au point de terminaison gRPC via TLS sur le port 50054. Rails et Workhorse ouvrent chacun leurs propres connexions, ils nécessitent donc tous deux une route vers le point de terminaison.

TLS peut se terminer à l'un de deux endroits : un équilibreur de charge devant le service `gkg-webserver`, ou le serveur web lui-même avec `tls.enabled` et `tls.existingSecret`. Seul le cas du serveur web nécessite le certificat à l'intérieur du cluster.

Un certificat émis par une autorité de certification (CA) publiquement approuvée ne nécessite aucune configuration supplémentaire dans GitLab. Pour un certificat émis par votre propre CA, ajoutez le certificat CA au magasin de confiance de GitLab. Pour plus d'informations, consultez [Installer des certificats publics personnalisés](https://docs.gitlab.com/omnibus/settings/ssl/#install-custom-public-certificates).

Si GitLab s'exécute dans le même cluster, `ClusterIP` est suffisant et le point de terminaison est `tls://gkg-webserver.gitlab-orbit.svc.cluster.local:50054`. Le certificat doit être valide pour le nom d'hôte auquel GitLab se connecte. Dans le cas contraire, exposez le service avec une méthode prise en charge par votre cluster et conservez l'adresse sur un réseau privé.

### Exigences en ressources {#resource-requirements}

Les valeurs par défaut du chart conviennent à la plupart des installations. Chacun des trois réplicas du serveur web demande 500m de CPU et 4 Gio. Chacun des trois réplicas de l'indexeur demande 2 CPU, 4 Gio et 5 Gio de stockage éphémère. Ces demandes totalisent environ 8 CPU et 24 Gio avant Siphon et NATS.

Donnez à l'indexeur suffisamment de ressources pour atteindre au moins 8 CPU et 16 Gio de mémoire. De nombreuses tâches d'indexation simultanées peuvent en utiliser la totalité. Les limites du chart autorisent cette marge par défaut.

L'indexeur a également besoin de suffisamment de stockage éphémère sur chaque nœud pour télécharger les archives de code. Le chart dimensionne cet espace de travail avec `indexer.tmpSizeLimit`, dont la valeur par défaut est 10 Gio, ainsi qu'avec les demandes et limites de stockage éphémère de l'indexeur.

## Activer l'indexation pour un groupe {#turn-on-indexing-for-a-group}

GitLab Orbit indexe les groupes principaux. Les sous-groupes et les projets héritent automatiquement de l'indexation.

1. Dans le coin supérieur droit, sélectionnez **Admin**.
1. Accédez à la page de configuration de GitLab Orbit à l'adresse `/admin/orbit`.
1. Sous **Groupes disponibles**, trouvez votre groupe principal.
1. Sélectionnez **Activer l'indexation**, puis confirmez.

Le groupe est déplacé vers **Groupes indexés**.

Pour activer l'indexation avec l'API à la place, utilisez un jeton avec un accès administrateur :

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

L'activation de l'indexation n'indexe pas immédiatement les données existantes. Les nouvelles modifications transitent par la réplication en quelques minutes. Un balayage récupère les données existantes au plus une heure plus tard.

## Vérifier l'installation {#verify-the-installation}

Pour vérifier l'installation, interrogez le graphe pour un projet dans le groupe pour lequel vous avez activé l'indexation.

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
  "response_format": "raw"
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_access_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  --url "https://gitlab.example.com/api/v4/orbit/query"
```

La requête retourne des lignes une fois le groupe indexé. Le statut des pods et le point de terminaison de santé ne confirment pas l'indexation, car les deux passent bien avant la fin du premier index.

## Sujets connexes {#related-topics}

- [Ce que GitLab Orbit indexe](../remote/indexing.md)
- [Référence de schéma](../remote/schema.md)
- [Cookbook](../remote/cookbook.md)
- [Langage de requête](../remote/queries/_index.md)
