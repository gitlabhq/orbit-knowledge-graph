---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Self-Managed上のGitLab Orbitの前提条件、インストール順序、および共有設定値。
title: GitLab Self-Managed上のGitLab Orbitを使い始める
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

GitLab Orbitは、ClickHouse、Kubernetes、NATSという3つのシステムに依存していますが、これらはGitLab Orbitがインストールするものではありません。
残りのセットアップ手順を進めるには、この3つのシステムがすべて存在し、到達可能である必要があります。

## 前提条件 {#prerequisites}

- GitLab 19.2.2以降。
- GitLabインスタンスおよびそのPostgreSQLサーバーへの管理者アクセス権。
- PostgreSQLの再起動1回分のメンテナンスウィンドウ。
- GitLab用にセットアップされたClickHouse 26.2以降。
- Kubernetes 1.33以降。
- JetStreamを有効にしたNATS。

## インストール順序 {#installation-order}

各ステップは前のステップに依存しています。以下の順序でコンポーネントをインストールしてください。

1. GitLab用にClickHouseをセットアップし、GitLab ClickHouseマイグレーションを実行します。
1. JetStreamを有効にしてNATSをクラスターにインストールします。
1. [データレプリケーションをセットアップします](data-replication.md): PostgreSQLを準備してからSiphonをインストールします。このステップが完了すると、GitLabの行がClickHouseデータレイクに届くようになります。
1. [GitLab Orbitをセットアップします](orbit-setup.md): ClickHouseのIDを作成し、チャートをインストールし、GitLabがGitLab Orbitを参照するよう設定して、トップレベルグループのインデックス作成を有効にします。

ステップ1から3はGitLab Orbitに依存しません。それぞれ個別に検証できます。

## ClickHouse {#clickhouse}

GitLab Orbitには、ClickHouse 26.2以降が必要です。これは、グラフがそのリリースで導入された全文インデックスとマテリアライズド共通テーブル式を使用するためです。GitLabは25.xおよび26.xのリリースをサポートしているため、25.xリリースはGitLabの他の部分には対応しますが、GitLab Orbitには対応しません。ClickHouseに関するその他のGitLab要件は変更ありません。

まずGitLab用にClickHouseをセットアップしてください。詳細については、
[ClickHouse](https://docs.gitlab.com/integration/clickhouse/)を参照してください。
[ClickHouseマイグレーションの実行](https://docs.gitlab.com/integration/clickhouse/#run-clickhouse-migrations)および
[分析用ClickHouseの有効化](https://docs.gitlab.com/integration/clickhouse/#enable-clickhouse-for-analytics)を含む、そのページのすべての手順を完了してください。
これらのマイグレーションにより、レプリケーションが書き込むデータレイクテーブルが作成されます。これらのテーブルがない場合、Siphonには書き込み先がなく、レプリケーションが失敗します。

GitLab Orbitは2つのデータベースを使用します。

| データベース | 書き込み元 | 読み取り元 |
|----------|------------|---------|
| `gitlab_clickhouse_main_production` | GitLab、Siphon | GitLab、GitLab OrbitのIndexerおよびディスパッチャー |
| `orbit` | GitLab Orbitディスパッチャー（スキーマ）およびIndexer（データ） | GitLab Orbitの3つのコンポーネントすべて |

2つのデータベースは別々のClickHouseインスタンスに配置できます。その場合、GitLab、Siphon、およびGitLab Orbitに対して、それぞれが使用するデータベースを持つインスタンスの認証情報を付与してください。

`gitlab_clickhouse_main_production`データベースは、ClickHouseのセットアップが完了した後に存在します。`orbit`データベースは、GitLab Orbitのセットアップ時に作成します。GitLab Orbitは起動時にこれを作成しません。

### サイジングと設定 {#sizing-and-settings}

ClickHouseには少なくとも8 CPUと32 GiBのメモリをプロビジョニングしてください。グラフのビルド中、ClickHouseは8コアをフル活用します。

ClickHouseのストレージは、少なくともGitLab PostgreSQLデータベースのサイズ以上をプロビジョニングしてください。

自分で運用するClickHouseインスタンスでは、`max_bytes_before_external_sort`と`max_bytes_before_external_group_by`が`0`に設定されており、ディスクへのスピルが無効になっています。ClickHouse Cloudでは、両方が利用可能なメモリの半分に設定されています。スピルが無効の場合、大規模なソートはすべての結果をメモリに保持するため、サーバーがメモリ不足になります。`default`プロファイルで両方を設定してください。以下の値は32 GiBインスタンスに適しており、各しきい値に8 GiB、メモリ上限に20 GiBを割り当てています。

```xml
<profiles>
  <default>
    <max_bytes_before_external_sort>8589934592</max_bytes_before_external_sort>
    <max_bytes_before_external_group_by>8589934592</max_bytes_before_external_group_by>
    <max_memory_usage>21474836480</max_memory_usage>
  </default>
</profiles>
```

<!-- markdownlint-disable-next-line MD044 -->
## Kubernetes {#kubernetes}

GitLab Orbitには、Kubernetes 1.33以降が必要です。また、`ImageVolume`フィーチャーゲートが有効になっており、コンテナランタイムでサポートされている必要があります。

GitLab OrbitはGitLabとは別のクラスターで実行できます。そのクラスターはGitLab、PostgreSQL、ClickHouse、およびNATSに到達できる必要があり、GitLabもそのクラスターに到達できる必要があります。

## NATS {#nats}

Siphonは変更されたすべての行をNATS JetStreamストリームに公開し、SiphonとGitLab Orbitの両方がそのストリームから読み取ります。NATSにはJetStreamの有効化と永続ボリュームが必要です。

NATSサーバーの`max_payload`を64 MBに設定してください。デフォルトの1 MBは一部のGitLabの行より小さいサイズです。Siphonはサーバーの値を読み取り、行が送信するには大きすぎるかどうかを判断します。

## オブジェクトストレージ（オプション） {#object-storage-optional}

Siphonはスナップショットイベントをオブジェクトストアに保存します。また、`max_payload`を引き上げた後もまだ大きすぎる行も同様に保存されます。デフォルトでは、SiphonはNATS JetStreamオブジェクトストアバケットを使用するため、追加のサービスは不要です。JetStreamボリュームへのトラフィックを抑えたい場合や、別の保持ポリシーを適用したい場合は、代わりにS3互換またはGoogle Cloud Storageバケットを設定してください。

GitLab Orbitはオブジェクトストレージを使用しません。グラフはClickHouseに保存されており、再度インデックス作成を行うことで再構築できます。Indexerはリポジトリのチェックアウトにノードローカルディスクを必要とします。チャートは一時的なストレージリクエストによってこのディスクのサイズを設定します。

## 共有設定値 {#shared-configuration-values}

以下の各値を一度決定し、それを読み取るすべてのコンポーネントで同じ値を使用してください。コンポーネントはこれらの値を他のコンポーネントと照合して検証しません。値が一致しない場合、影響を受けるコンポーネントは起動しますが、データを処理しません。

| 値 | 使用元 | 例 |
|-------|---------|---------|
| NATSストリーム名 | SiphonおよびGitLab Orbit | `siphon_stream_main_db` |
| データレイクデータベース | GitLab、Siphon、およびGitLab Orbit | `gitlab_clickhouse_main_production` |
| グラフデータベース | GitLab Orbit | `orbit` |
| PostgreSQLホストとポート | Siphon | `postgres.example.com:5432` |
| クラスターから到達可能なGitLab URL | GitLab Orbit | `https://gitlab.example.com` |
| GitLabから到達可能なGitLab Orbit gRPCエンドポイント | GitLab | `tls://orbit.example.com:50054` |

GitLab Orbitはデフォルトでストリーム名`siphon_stream_main_db`を想定しています。別の名前を使用するには、`siphon-values.yaml`で`stream_name`を設定し、`orbit-values.yaml`で`schedule.tasks.siphon.events_stream_name`を設定してください。

## 次のステップ {#next-step}

- [データレプリケーションをセットアップする](data-replication.md)
