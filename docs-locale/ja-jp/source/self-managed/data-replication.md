---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: PostgreSQLの論理レプリケーションを設定し、SiphonをインストールしてGitLabのデータをClickHouseに送信します。
title: データレプリケーションのセットアップ
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab Self-Managed
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 19.2.2で[導入](https://gitlab.com/groups/gitlab-org/-/epics/22739)されました。

{{< /history >}}

> [!note]
> GitLab Self-Managed上のGitLab Orbitは
> [ベータ版](https://docs.gitlab.com/policy/development_stages_support/#beta)です。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

GitLab OrbitはClickHouse内のGitLabデータベースのコピーからGitLabデータを読み取ります。GitLabデータベース自体からは読み取りません。[Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon)がそのコピーを最新の状態に保ちます。

SiphonはKubernetes上で3つのデプロイとして動作します。

| デプロイ | 役割 |
|------------|------|
| Producer | PostgreSQLの先行書き込みログを読み取り、変更された各行をNATSにパブリッシュします。 |
| Consumer | NATSを読み取り、行をClickHouseに書き込みます。 |
| Reconciler | ネームスペーストラバーサルパスなどの派生列をスケジュールに従って再計算し、影響を受けた行をNATSを通じて再パブリッシュします。 |

前提条件:

- GitLab Self-Managed上のGitLab Orbitの[前提条件](getting-started.md#prerequisites)。
- GitLab向けにセットアップされたClickHouseインスタンス（GitLab ClickHouseマイグレーション適用済み）。詳細については、[ClickHouse](getting-started.md#clickhouse)を参照してください。
- GitLab PostgreSQLサーバーへのスーパーユーザーアクセス。
- PostgreSQLの再起動1回分のメンテナンスウィンドウ。
- Helm 3およびクラスターへの`kubectl`アクセス。

次の順序でレプリケーションをセットアップします。

1. PostgreSQLで論理レプリケーションを有効にする。
1. SiphonのPostgreSQLユーザーを作成する。
1. パブリケーションと権限を作成する。
1. SiphonのClickHouseユーザーを作成する。
1. パスワードをSiphonで利用可能にする。
1. Siphonをインストールする。

<!-- markdownlint-disable-next-line MD044 -->
## PostgreSQLで論理レプリケーションを有効にする {#turn-on-logical-replication-in-postgresql}

`wal_level`が`logical`でない場合、Siphon producerは起動時に停止します。`wal_level`、`max_replication_slots`、`max_wal_senders`への変更はすべて、リロードではなくPostgreSQLの完全な再起動が必要です。また、`gitlab-ctl reconfigure`はPostgreSQLを再起動しません。

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. `/etc/gitlab/gitlab.rb`を編集します。

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

1. ファイルを保存し、GitLabを再設定してからPostgreSQLを再起動します。

   ```shell
   sudo gitlab-ctl reconfigure
   sudo gitlab-ctl restart postgresql
   ```

1. 設定を確認します。

   ```shell
   sudo gitlab-psql -c 'SHOW wal_level'
   sudo gitlab-psql -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level`は`logical`を返し、他の2つは`gitlab.rb`と一致し、`pending_restart = t`を報告する行はありません。部分的に適用された組み合わせでは、次回の再起動時にPostgreSQLが起動できなくなります。復旧するには、`/etc/gitlab/gitlab.rb`を修正し、再設定してサービスが実行中であることを確認してください。

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

GitLab Helmチャートは本番PostgreSQLを管理しないため、これらの設定は独自のサーバーまたはマネージドデータベースに適用してください。

1. 次のパラメーターを設定します。

   | パラメーター | 値 |
   |-----------|-------|
   | `wal_level` | `logical` |
   | `max_replication_slots` | `10`以上 |
   | `max_wal_senders` | `10`以上 |

   マネージドデータベースの場合は、`postgresql.conf`ではなくプロバイダーのパラメーターグループを通じて設定します。一部のプロバイダーでは、`wal_level`が論理レプリケーションフラグなど別の名前で公開されています。

1. サーバーを再起動します。

1. クラスターからポート5432への接続を許可します。

1. 設定を確認します。

   ```shell
   psql -h <postgresql_host> -U <admin_user> -d gitlabhq_production \
     -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level`は`logical`を返し、他の2つは設定した値と一致し、`pending_restart = t`を報告する行はありません。

{{< /tab >}}

{{< /tabs >}}

<!-- markdownlint-disable-next-line MD044 -->
## SiphonのPostgreSQLユーザーを作成する {#create-the-siphon-postgresql-users}

Siphonは3つのロールを使用します。スーパーユーザーとして作成してください。ロールが`REPLICATION`を付与できるのは、そのロール自身がすでに`REPLICATION`属性を持っている場合のみです。GitLabアプリケーションロールはこの属性を持っていません。

スーパーユーザーとして`gitlabhq_production`に接続し、次を実行します。

```sql
CREATE USER siphon WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_replicator WITH REPLICATION LOGIN PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_snapshot WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
```

3つのロールは同じパスワードを使用できます。

この手順のvaluesファイルでは、Siphonはすべての接続に`siphon_replicator`として接続します。他の2つのロールが存在するのは、次のステップのRakeタスクがそれらに読み取りアクセスも付与するためです。スプリットユーザーセットアップでは、後でこれらを使用できます。

## パブリケーションと権限を作成する {#create-the-publication-and-grants}

GitLabにはSiphon向けにPostgreSQLを準備するRakeタスクが同梱されています。このタスクは冪等であり、次を作成します。

- パブリケーション。
- Siphonがパブリケーションにテーブルを追加するために呼び出すヘルパー関数。
- すべてのGitLabスキーマへの読み取りアクセス。

GitLab 19.2.2以降にはこのタスクが含まれています。

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. タスクが存在することを確認します。

   ```shell
   sudo gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. タスクを実行します。

   ```shell
   sudo gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

1. toolboxデプロイを見つけます。

   ```shell
   kubectl -n <gitlab_namespace> get deploy -l app=toolbox
   ```

1. タスクが存在することを確認します。

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. タスクを実行します。

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< /tabs >}}

このタスクは`siphon_publication_main_1`という名前のパブリケーションを作成し、`siphon`ロールに`public.siphon_alter_publication`への`EXECUTE`権限を付与します。producerは`siphon_replicator`接続でこの関数を呼び出すため、`siphon_replicator`にも同じ`EXECUTE`権限を付与してください。スーパーユーザーとして次を実行します。

```sql
GRANT EXECUTE ON FUNCTION public.siphon_alter_publication(text, text, integer)
  TO siphon_replicator;
```

パブリケーションはproducerが起動するまで空です。その後、Siphonは同じ関数を通じてパブリケーションにテーブルを追加します。

## SiphonのClickHouseユーザーを作成する {#create-the-siphon-clickhouse-user}

SiphonはGitLabがすでに使用しているデータベースに書き込みます。GitLab ClickHouseマイグレーションがターゲットテーブルを作成するため、Siphonにはスキーマ権限は不要です。

管理者としてClickHouseに接続し、次を実行します。

```sql
CREATE USER siphon IDENTIFIED WITH sha256_password BY '<your_password>';
CREATE ROLE siphon_app;
GRANT SELECT, INSERT, dictGet ON gitlab_clickhouse_main_production.* TO siphon_app;
GRANT siphon_app TO siphon;
```

`dictGet`権限は必須です。これがないと、プローブはメトリクスエンドポイントをスクレイピングするだけなので、ポッドはヘルスチェックを通過します。障害はconsumerログにパーミッションエラーとして現れます。ディクショナリに基づくすべてのテーブルは空のままになります。

`system`データベースを制限するマネージドClickHouseの場合は、次も実行します。

```sql
GRANT SELECT ON system.tables, system.columns TO siphon_app;
```

## パスワードをSiphonで利用可能にする {#make-the-passwords-available-to-siphon}

SiphonはKubernetes Secretに基づく環境変数から両方のパスワードを読み取ります。Secretを作成する前にネームスペースが存在している必要があります。次のセクションのvaluesファイルは、Siphonネームスペース内に`siphon-secrets`という名前のSecretが1つあり、次のキーを持つことを想定しています。

| キー | 内容 |
|-----|-------|
| `pg-password` | `siphon`、`siphon_replicator`、`siphon_snapshot` PostgreSQLロールのパスワード |
| `ch-siphon-password` | ClickHouseの`siphon`ユーザーのパスワード |

すでに運用しているシークレットマネージャーに値を保管することをお勧めします。External Secrets Operatorなどのツールを使用してクラスターに同期してください。平文を他の場所に保存しないでください。

## Siphonをインストールする {#install-siphon}

`configMode: split`を使用すると、SiphonはGitLabに同梱されているイメージからポッド起動時にテーブルリストを構築します。リストはGitLabのバージョンと一致し、手動での更新は不要です。

`global.gitlabVersion`は`gitlab-siphon-tables`イメージのタグであり、レプリケートされるテーブルセットをGitLabのバージョンに固定します。タグは`v19.2.0-ee`から始まります。タグが存在しないとポッドが起動できなくなるため、インストール前に[コンテナレジストリ](https://gitlab.com/gitlab-org/gitlab/container_registry)でタグが存在することを確認してください。

1. 次の内容を`siphon-values.yaml`として保存し、プレースホルダーを置き換えます。

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

1. チャートをインストールします。

   ```shell
   helm repo add siphon https://gitlab.com/api/v4/projects/76780115/packages/helm/stable
   helm repo update

   helm upgrade --install siphon siphon/siphon \
     --version 1.18.0 \
     --namespace siphon \
     --create-namespace \
     --values siphon-values.yaml
   ```

   これらのコマンドは直接Helmインストールのリファレンスです。ネームスペース名とデプロイ方法は独自のツールに合わせて調整してください。

1. 3つのデプロイがすべて実行中であることを確認します。

   ```shell
   kubectl -n siphon get pods
   ```

出力には、`Running`状態の`postgres-producer`、`clickhouse-consumer`、`reconciler`ポッドが一覧表示されます。

Siphonはvaluesファイルを変更してもポッドを再起動しません。変更を適用するには、デプロイを再起動します。

```shell
kubectl -n siphon rollout restart deployment
```

### Valuesファイルの設定 {#values-file-settings}

| 設定 | 要件 |
|---------|-------------|
| `database_mapping` | 必須。GitLabのスキーマは、インスタンスが個別に保存しているかどうかにかかわらず、3つの論理データベース（`main`、`ci`、`sec`）に分割されています。マッピングがないとジェネレーターが失敗します。`ci`と`sec`を参照するテーブル定義を削除しないでください。削除すると、すべてのCI、脆弱性、依存関係テーブルが暗黙的に削除されます。 |
| `stream_name` | GitLab Orbitが読み取るストリーム名と一致する必要があります。両側で共有される値の完全なリストについては、[共有設定値](getting-started.md#shared-configuration-values)を参照してください。 |
| `advisory_lock_id`とロックタイムアウト | 必須。これらがないとproducerは起動時に停止します。 |
| `nats_config.replicas` | NATSクラスターのサイズと一致する必要があります。単一のNATSサーバーはレプリカを1つしかサポートしません。 |
| `ssl_mode` | PostgreSQLサーバーが提供するものと一致する必要があります。例では`require`を使用しています。これはLinuxパッケージのPostgreSQLがデフォルトでTLSを提供するため機能します。TLSを提供しないサーバーは接続を拒否し、producerは`server refused TLS connection`で起動時に停止します。 |
| `connection.replication.use_alter_publication_function` | チャートのデフォルトである`true`のままにする必要があります。`public.siphon_alter_publication`への`EXECUTE`権限はこの設定のために存在します。パブリケーションはGitLabデータベースユーザーに属しているため、SiphonロールからのダイレクトなALTER PUBLICATIONは失敗します。 |
| `max_age_seconds` | consumerがどこまで遡って再生できるかを制御します。60以上のテーブルにわたって変更された各行を15日間保持すると、大きなJetStreamファイルストアが生成されます。完全な保持ウィンドウに対応できるようNATSボリュームをサイジングするか、値を下げてください。 |

### 大きな行のオブジェクトストレージ {#object-storage-for-large-rows}

ストリームに収まらない大きな行はオブジェクトストアに送られます。前述のvaluesファイルはオブジェクトストアを設定していないため、SiphonはNATS JetStreamオブジェクトストアを使用し、認証情報は不要です。

外部バケットを使用するには、`queueing`の下に`identifier`、`type`、`bucket_name`を含む`object_storage_config`を追加し、すべてのSiphonポッドにバケットの認証情報を付与します。`type: s3`の場合、SiphonはAWS SDKの標準チェーンに従うため、`AWS_REGION`を設定し、インスタンスロールをアタッチするか、`env`と`envFromSecrets`を通じて`AWS_ACCESS_KEY_ID`と`AWS_SECRET_ACCESS_KEY`を渡します。S3互換サービスの場合は、`AWS_ENDPOINT_URL_S3`も設定します。Google Cloud Storageの場合は、アプリケーションデフォルト認証情報で`type: gcs`を使用します。

## レプリケーションを確認する {#verify-replication}

最初のコピーは一度に1つのテーブルを処理し、各テーブルのマージ中はレプリケーションを一時停止します。そのため、所要時間はデータ量ではなくテーブル数に依存します。GitLab 19.2.2インスタンスは60以上のテーブルをレプリケートします。以下のチェックはレプリケーションが実行中であることを確認するものです。最初のコピーが完了したことを確認するものではありません。

1. SiphonがPostgreSQLを読み取っていることを確認します。スロットがアクティブであり、GitLabへの書き込み後に`confirmed_flush_lsn`が進んでいる必要があります。

   ```sql
   SELECT slot_name, active, wal_status, confirmed_flush_lsn
   FROM pg_replication_slots
   WHERE slot_name = 'siphon_main_1_slot';
   ```

1. ClickHouseに行が届いていることを確認します。

   ```sql
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_namespaces FINAL;
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_projects FINAL;
   ```

   PostgreSQLの同じテーブルとカウントを比較してください。テーブルは`ReplacingMergeTree`であり、バックグラウンドマージが完了するまで更新された行が複数回現れるため、`FINAL`が必要です。テーブルが最初の書き込み時にプレースホルダー行を受け取ることがあるため、ClickHouseのカウントは通常わずかに多くなります。

> [!warning]
> 非アクティブなレプリケーションスロットはGitLab PostgreSQLディスク上に先行書き込みログを保持し、ディスクを満杯にする可能性があります。ディスクの空き容量が許す時間を超えてSiphonを停止する場合は、`SELECT pg_drop_replication_slot('siphon_main_1_slot');`でスロットを削除し、後で新しいスナップショットを取得してください。

## 次のステップ {#next-step}

- [GitLab Orbitをセットアップする](orbit-setup.md)
