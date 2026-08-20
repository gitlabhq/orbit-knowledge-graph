---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: KubernetesにGitLab Orbitをインストールし、GitLabに接続して、最初のグループをインデックス作成します。
title: GitLab Orbitをセットアップする
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
> GitLab Self-ManagedのGitLab Orbitは
> [ベータ版](https://docs.gitlab.com/policy/development_stages_support/#beta)です。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

GitLab Orbitは、インスタンスの隣にHelmリリースとして動作します。IndexerはClickHouseデータレイクを読み取り、GitLab内部APIを通じてソースコードをフェッチします。Webサーバーはグラフへのクエリに応答します。

GitLab OrbitはHTTP経由でGitLabを呼び出し、GitLabはポート50054のgRPC経由でGitLab Orbitを呼び出します。両方向が開いている必要があります。GitLab OrbitはGitalyに直接接続しません。

前提条件:

- [データレプリケーション](data-replication.md)が実行中で、データレイクに行が届いていること。
- インデックス作成するグループのオーナーロール。
- GitLabへの管理者アクセス。

GitLab Orbitを次の順序でセットアップします:

1. ClickHouseデータベースとIDを作成する。
1. GitLabでGitLab Orbitを有効にする。
1. 認証情報をGitLab Orbitで利用可能にする。
1. GitLab Orbitをインストールする。
1. グループのインデックス作成を有効にする。

## ClickHouseデータベースとIDを作成する {#create-the-clickhouse-database-and-identities}

GitLab Orbitには専用のグラフデータベースと3つのユーザーが必要です:

| ユーザー | ロール | アクセス |
|------|------|--------|
| `gkg_writer` | `gkg_app` | グラフデータベースの読み取りと書き込み |
| `gkg_reader` | `gkg_reader_app` | グラフデータベースの読み取り |
| `gkg_siphon_reader` | `gkg_siphon_reader_app` | データレイクの読み取り |

データベースとユーザーを作成するステートメントはGitLab Orbitイメージ内に含まれているため、実行するバージョンと常に一致します。ステートメントはべき等です。GitLab Orbitは起動時にグラフデータベースを作成せず、ディスパッチャーは既存のデータベースに対してのみスキーママイグレーションを実行します。

データベースとIDを作成するには:

1. ステートメントを読み取ります:

   ```shell
   docker run --rm --entrypoint cat \
     registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.96.0 \
     /usr/share/gkg/clickhouse-setup.sql
   ```

1. グラフデータベース名、データレイクデータベース名、および各ユーザーのパスワードを置き換えます。グラフデータベース名には`orbit`を使用します。

1. 管理者としてClickHouseに対して結果を実行します。ステートメントはグラフデータベースを作成し、`gkg_app`ロールにそのデータベースで必要な権限を付与します。

1. 同梱ファイルに含まれていない3つのGRANTを追加します:

   ```sql
   GRANT SELECT ON system.parts TO gkg_app;
   GRANT SELECT ON system.tables TO gkg_app;
   GRANT SELECT ON system.dictionaries TO gkg_reader_app;
   ```

自分で運用するClickHouseインスタンスでは、すべてのユーザーが`system`データベースを読み取れるため、これらのGRANTは何も変更しません。マネージドClickHouseは通常`system`データベースへのアクセスを制限します。GRANTがない場合、新しいバージョンがプロモートされるとスキーママイグレーションが停止し、クエリパスの解決が失敗します。どちらの場合もGRANTを追加してください。マネージドサービスに移行しても、設定はそのまま機能します。

GitLab OrbitはHTTPインターフェース（ポート8123）またはTLS使用時はポート8443でClickHouseに接続します。Siphonはポート9000のネイティブプロトコルを使用するため、HTTPポートとポート9000の両方がクラスターから到達可能である必要があります。

<!-- markdownlint-disable-next-line MD044 -->
## GitLabでGitLab Orbitを有効にする {#turn-on-gitlab-orbit-in-gitlab}

GitLabとGitLab Orbitは1つの対称キーで相互に認証し、GitLabがそのキーを所有します。後のステップでクラスターにキーをコピーする前にキーが存在するよう、まずGitLabでGitLab Orbitを有効にします。

開始前にgRPCエンドポイントを決定します。GitLabはポート50054のTLS経由でエンドポイントに接続します。設定のスキームが接続の暗号化を制御するため、値は`tls://`で始まる必要があります。

{{< tabs >}}

{{< tab title="Linux package (Omnibus)" >}}

1. `/etc/gitlab/gitlab.rb`を編集します:

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   ```

1. ファイルを保存してGitLabを再設定します:

   ```shell
   sudo gitlab-ctl reconfigure
   ```

   GitLabは共有キーを生成し、
   `/var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret`に書き込みます。

1. キーを読み取ります。再エンコードやトリミングをせず、値を正確にコピーします:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

マルチノード構成では、すべてのRailsノードが同じキーを保持する必要があります。1つのノードでキーを生成し、他のノードで`gitlab_rails['orbit_secret']`にその値を設定するか、最初の再設定前に`/etc/gitlab/gitlab-secrets.json`を同期します。

{{< /tab >}}

{{< tab title="Helm chart (Kubernetes)" >}}

GitLabチャートはキーを生成しないため、自分で作成します。GitLabをアップグレードする前にSecretが存在している必要があります。チャートは`optional`フラグなしでSecretをマウントするため、Secretがない場合、すべての新しいポッドが`ContainerCreating`状態のままになります。

1. 32バイトのランダムなバイト列をbase64エンコードしてキーを作成します:

   ```shell
   openssl rand -base64 32
   ```

1. その値をGitLabリリースのネームスペース内のSecretに格納します。

1. Secretを参照します:

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

GitLab Orbitチャートをインストールするまで、GitLabはGitLab Orbitに接続できません。それ以前の接続エラーは想定内です。

<!-- markdownlint-disable-next-line MD044 -->
## 認証情報をGitLab Orbitで利用可能にする {#make-the-credentials-available-to-gitlab-orbit}

GitLab OrbitはKubernetes Secretの各キーから認証情報を読み取ります。Secretを作成する前にネームスペースが存在している必要があります。次のセクションのvaluesファイルは、GitLab Orbitネームスペース内に`gkg-secrets`という名前のSecretが1つあり、以下のキーを持つことを想定しています:

| キー | 内容 |
|-----|-------|
| `gitlab-jwt-verifying-key` | 前のステップでGitLabが生成した共有キー。GitLabからのトークンの検証に使用 |
| `gitlab-jwt-signing-key` | 同じ共有キー。GitLabに送り返すトークンの署名に使用 |
| `datalake-password` | ClickHouseの`gkg_siphon_reader`ユーザーのパスワード |
| `graph-password` | ClickHouseの`gkg_writer`ユーザーのパスワード |
| `graph-read-password` | ClickHouseの`gkg_reader`ユーザーのパスワード |

両方のキーエントリは同じ値を保持します。各キーを異なるSecretから取得するには、`secrets.perKey`を使用します。

値は既存のシークレットマネージャーで管理することをお勧めします。External Secrets Operatorなどのツールを使用してクラスターに同期します。平文を他の場所に保存しないでください。

gRPCエンドポイント用のTLS証明書も提供する必要があります。詳細については、[TLSとネットワーク要件](#tls-and-network-requirements)を参照してください。

<!-- markdownlint-disable-next-line MD044 -->
## GitLab Orbitをインストールする {#install-gitlab-orbit}

1. 以下を`orbit-values.yaml`として保存し、プレースホルダーを置き換えます:

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

1. チャートをインストールします:

   ```shell
   helm upgrade --install gkg \
     oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg \
     --version 1.5.0 \
     --namespace gitlab-orbit \
     --create-namespace \
     --values orbit-values.yaml
   ```

   このコマンドは直接Helmインストールのリファレンスです。ネームスペース名とデプロイ方法は、使用するツールに合わせて調整してください。

1. 3つのコンポーネントがすべて実行中であることを確認します:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

出力には、`Running`状態のWebサーバー、Indexer、ディスパッチャーのポッドが一覧表示されます。チャートはメトリクス、オートスケール、アナリティクス、課金を含む他のすべてをデフォルトで無効にしています。GitLab Self-Managedではそれらを無効のままにしてください。

### TLSとネットワーク要件 {#tls-and-network-requirements}

GitLabはポート50054のTLS経由でgRPCエンドポイントに接続します。RailsとWorkhorse両方が独自の接続を開くため、両方ともエンドポイントへのルートが必要です。

TLSは2か所のいずれかで終端できます: `gkg-webserver`サービスの前にあるロードバランサー、またはWebサーバー自体（`tls.enabled`と`tls.existingSecret`を使用）。証明書がクラスター内に必要なのはWebサーバーの場合のみです。

公的に信頼された認証局（CA）が発行した証明書は、GitLabで追加設定は不要です。独自のCAからの証明書の場合は、CA証明書をGitLabトラストストアに追加します。詳細については、[カスタム公開証明書のインストール](https://docs.gitlab.com/omnibus/settings/ssl/#install-custom-public-certificates)を参照してください。

GitLabが同じクラスター内で動作している場合、`ClusterIP`で十分で、エンドポイントは`tls://gkg-webserver.gitlab-orbit.svc.cluster.local:50054`です。証明書はGitLabが接続するホスト名に対して有効である必要があります。それ以外の場合は、クラスターがサポートする方法でサービスを公開し、アドレスをプライベートネットワーク上に保持します。

### リソース要件 {#resource-requirements}

チャートのデフォルト値はほとんどのインストールに適しています。3つのWebサーバーレプリカはそれぞれ500m CPUと4 GiBをリクエストします。3つのIndexerレプリカはそれぞれ2 CPU、4 GiB、および5 GiBの一時的なストレージをリクエストします。これらのリクエストの合計は、SiphonとNATSを除いて約8 CPUと24 GiBになります。

Indexerには少なくとも8 CPUと16 GiBのメモリを確保してください。多数の同時インデックス作成タスクがそのすべてを使用する可能性があります。チャートの制限はデフォルトでそのヘッドルームを許容します。

Indexerはコードアーカイブをダウンロードするために、各ノードに十分な一時的なストレージも必要です。チャートはそのスクラッチスペースを`indexer.tmpSizeLimit`（デフォルトは10 GiB）とIndexerの一時的なストレージのリクエストおよび制限でサイズ設定します。

## グループのインデックス作成を有効にする {#turn-on-indexing-for-a-group}

GitLab Orbitはトップレベルグループをインデックス作成します。サブグループとプロジェクトは自動的にインデックス作成を継承します。

1. 右上隅で**管理者**を選択します。
1. `/admin/orbit`のGitLab Orbit設定ページに移動します。
1. **利用可能なグループ**でトップレベルグループを見つけます。
1. **インデックス作成を有効にする**を選択し、確認します。

グループが**インデックス済みグループ**に移動します。

代わりにAPIでインデックス作成を有効にするには、管理者アクセス権を持つトークンを使用します:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

インデックス作成を有効にしても、既存のデータはすぐにインデックス作成されません。新しい変更は数分でレプリケーションを通じて反映されます。既存のデータは最大1時間後にスイープによって取り込まれます。

## インストールを確認する {#verify-the-installation}

インストールを確認するには、インデックス作成を有効にしたグループ内のプロジェクトのグラフをクエリします。

リクエストボディを`request.json`に格納します:

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

グループのインデックス作成が完了すると、クエリは行を返します。ポッドのステータスとヘルスエンドポイントはインデックス作成を確認しません。最初のインデックス作成が完了するずっと前に両方がパスするためです。

## 関連トピック {#related-topics}

- [GitLab Orbitがインデックス作成する内容](../remote/indexing.md)
- [スキーマリファレンス](../remote/schema.md)
- [Cookbook](../remote/cookbook.md)
- [クエリ言語](../remote/queries/_index.md)
