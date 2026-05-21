```markdown
---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit クエリ言語を使用して、ナレッジグラフを検索およびトラバースします。
title: Orbit クエリ言語
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- GitLab 18.10 で `knowledge_graph` という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト用に利用可能ですが、本番環境での使用には対応していません。

フラットな API レスポンスではなくグラフとして GitLab データが必要な場合は、Orbit クエリ言語を使用してください。クエリは JSON オブジェクトです。マッチするエンティティ、辿るリレーションシップ、返すプロパティを指定します。

## クエリの形式 {#query-shape}

すべてのクエリには `query_type` と、`node` または `nodes` のいずれかが必要です。

```json
{
  "query_type": "traversal",
  "node": {
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state"]
  },
  "limit": 1
}
```

1 つのノードセレクターには `node` を使用します。セレクターの配列には `nodes` を使用します。同じクエリで両方を使用することはできません。

## クエリタイプ {#query-types}

| クエリタイプ | 用途 |
|------------|-----------|
| `traversal` | マッチするノードをフェッチするか、ノード間のリレーションシップを辿ります。 |
| `aggregation` | マッチするグラフ結果をカウント、合計、平均、グループ化、またはソートします。 |
| `path_finding` | 2 つのノードセレクター間の有界パスを見つけます。 |
| `neighbors` | 1 つの有界ノードに接続されたノードを返します。 |

単一ノードの `traversal` が検索の形式です。独立した `search` クエリタイプはありません。

## トップレベルフィールド {#top-level-fields}

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `query_type` | `string` | `traversal`、`aggregation`、`path_finding`、または `neighbors` のいずれか。 |
| `node` | `object` | 1 つのノードセレクター。単一ノードの `traversal` および `neighbors` に必須。 |
| `nodes` | `array` | 複数のノードセレクター。マルチノードの `traversal`、`aggregation`、および `path_finding` に必須。最大 5 個。 |
| `relationships` | `array` | トラバーサルまたは集計のリレーションシップセレクター。最大 5 個。 |
| `aggregations` | `array` | 集計の定義。`aggregation` に必須。最大 10 個。 |
| `group_by` | `array` | 集計行のグループキー。最大 4 個。 |
| `path` | `object` | パス検索の設定。`path_finding` に必須。 |
| `neighbors` | `object` | 近傍ルックアップの設定。`neighbors` に必須。 |
| `limit` | `integer` | 返す最大行数。デフォルト 30。最大 1000。 |
| `cursor` | `object` | 認可された結果に対するオフセットページネーション。 |
| `order_by` | `object` | ノードプロパティで行をソートします。 |
| `aggregation_sort` | `object` | 出力カラムで集計行をソートします。 |
| `options` | `object` | 表示およびデバッグオプション。 |

## ノードセレクター {#node-selectors}

ノードセレクターはオントロジー内の 1 つのエンティティタイプを指定します。

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `id` | `string` | ノードのローカルエイリアス。リレーションシップ、集計、パス、および近傍はこのエイリアスを参照します。 |
| `entity` | `string` | `Project`、`User`、`MergeRequest`、`File`、`Definition` などのオントロジーノードタイプ。 |
| `columns` | `string` または `array` | 返すプロパティ。すべての非制限プロパティには `"*"` を、名前の配列を使用します。省略した場合、Orbit はエンティティのデフォルトカラムを返します。 |
| `filters` | `object` | プロパティフィルター。 |
| `node_ids` | `array` | マッチする正確な ID。整数または数字の文字列を受け付けます。最大 500 個。 |
| `id_range` | `object` | `start` と `end` を持つ包括的な ID 範囲。 |
| `id_property` | `string` | `node_ids` および `id_range` で使用されるプロパティ。デフォルト `id`。 |

グラフ ID が既にわかっている場合は `node_ids` を使用します。`username`、`full_path`、`state`、`path` などの自然なプロパティがわかっている場合は `filters` を使用します。

## リレーションシップ {#relationships}

リレーションシップはエイリアスでノードセレクターを接続します。

```json
{
  "type": "AUTHORED",
  "from": "user",
  "to": "mr",
  "direction": "outgoing"
}
```

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `type` | `string` または `array` | リレーションシップタイプまたはタイプの配列。有界クエリで任意のリレーションシップが必要な場合にのみ `"*"` を使用します。 |
| `from` | `string` | 開始ノードセレクターのエイリアス。 |
| `to` | `string` | 終了ノードセレクターのエイリアス。 |
| `direction` | `string` | `outgoing`、`incoming`、または `both`。デフォルト `outgoing`。 |
| `min_hops` | `integer` | 最小ホップ数。デフォルト 1。最大 3。 |
| `max_hops` | `integer` | 最大ホップ数。デフォルト 1。最大 3。 |
| `filters` | `object` | リレーションシッププロパティフィルター。最大 5 フィルター。 |

たとえば、マージリクエストは `IN_PROJECT` でプロジェクトを指し、ユーザーは `AUTHORED` でマージリクエストを指します。

## フィルター {#filters}

フィルターは単純な等値比較を使用できます：

```json
{
  "filters": {
    "state": "merged"
  }
}
```

または演算子を使用できます：

```json
{
  "filters": {
    "created_at": {"op": "gte", "value": "2026-01-01"},
    "state": {"op": "in", "value": ["opened", "merged"]}
  }
}
```

| 演算子 | 用途 |
|----------|-----|
| `eq` | スカラー値と等しい。 |
| `gt`、`gte`、`lt`、`lte` | 数値、日付、またはタイムスタンプの比較。 |
| `in` | 値が配列に含まれる。最大 100 個の値。 |
| `contains` | 文字列が部分文字列を含む。 |
| `starts_with` | 文字列がプレフィックスで始まる。 |
| `ends_with` | 文字列がサフィックスで終わる。 |
| `is_null` | 値が null。`value` は指定しないでください。 |
| `is_not_null` | 値が null でない。`value` は指定しないでください。 |
| `token_match` | テキストインデックスが 1 つのトークンを含む。 |
| `all_tokens` | テキストインデックスがすべてのトークンを含む。 |
| `any_tokens` | テキストインデックスがいずれかのトークンを含む。 |

トークン演算子はテキストインデックスを持つプロパティにのみ機能します。

## カラムと仮想カラム {#columns-and-virtual-columns}

ほとんどのカラムは ClickHouse のインデックス付きグラフテーブルから取得されます。一部のカラムは仮想です：Orbit はグラフクエリが返された後、別のサービスからそれらをフェッチします。

仮想カラムは `columns` で明示的にリクエストしてください。`path_finding` および `neighbors` で使用される `dynamic_columns` オプションは、外部サービス呼び出しが必要になる可能性があるため、仮想カラムを除外します。

| エンティティ | 仮想カラム | 返す内容 |
|--------|----------------|-----------------|
| `MergeRequest` | `diff` | マージリクエストの完全な統合差分。 |
| `MergeRequestDiff` | `patch` | 1 つのマージリクエスト差分スナップショットの完全なパッチ。 |
| `MergeRequestDiffFile` | `diff` | ファイルごとの統合差分テキスト。`too_large` が `true` の場合は `null` を返します。 |
| `File` | `content` | ファイルの raw ソーステキスト。 |
| `Definition` | `content` | 1 つのインデックス付き定義のソーステキスト。 |

`content` カラムはソースコード用です。マージリクエストの差分テキストには、`MergeRequest.diff`、`MergeRequestDiff.patch`、または `MergeRequestDiffFile.diff` を使用してください。

## トラバーサルの例 {#traversal-examples}

完全な差分とともに 1 つのマージリクエストをフェッチする：

```json
{
  "query_type": "traversal",
  "node": {
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state", "diff"]
  },
  "limit": 1
}
```

差分スナップショットからファイルごとの差分コンテンツをフェッチする：

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    },
    {
      "id": "snapshot",
      "entity": "MergeRequestDiff",
      "columns": ["id", "state", "patch"]
    },
    {
      "id": "file",
      "entity": "MergeRequestDiffFile",
      "columns": ["new_path", "old_path", "too_large", "diff"]
    }
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "snapshot"},
    {"type": "HAS_FILE", "from": "snapshot", "to": "file"}
  ],
  "limit": 20
}
```

ソースファイルのコンテンツをフェッチする：

```json
{
  "query_type": "traversal",
  "node": {
    "id": "file",
    "entity": "File",
    "filters": {
      "path": {"op": "ends_with", "value": "app/models/project.rb"}
    },
    "columns": ["path", "language", "content"]
  },
  "limit": 5
}
```

プロジェクト内のマージ済みマージリクエストを検索する：

```json
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "gitlab-org/gitlab"},
      "columns": ["name", "full_path"]
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"},
      "columns": ["iid", "title", "state", "merged_at"]
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "limit": 25
}
```

1 つのマージリクエストに対して実行されたすべてのパイプラインを検索する。マージリクエストの **Pipelines** タブ、REST `/merge_requests/:iid/pipelines` エンドポイント、および GraphQL `mergeRequest.pipelines` コネクションが返す内容と一致させるために、常に `Pipeline.source = "merge_request_event"` でフィルタリングしてください：

```json
{
  "query_type": "traversal",
  "node": {
    "id": "p",
    "entity": "Pipeline",
    "filters": {
      "merge_request_id": {"op": "eq", "value": 482908721},
      "source": {"op": "eq", "value": "merge_request_event"}
    },
    "columns": ["id", "status", "source", "sha", "ref", "created_at"]
  },
  "order_by": {"node": "p", "property": "created_at", "direction": "DESC"},
  "limit": 100
}
```

`merge_request_id` はマージリクエストの内部数値 `id` であり、プロジェクトスコープの `iid` ではありません。まず `iid` と `project_id` でフィルタリングする `MergeRequest` トラバーサルで調べてから、その `id` を上記のクエリに使用してください。

`Pipeline.merge_request_id` と `MergeRequest --TRIGGERED--> Pipeline` エッジはどちらも、MR のコンテキストで起動されたすべての CI パイプライン（トップレベルの MR パイプラインがトリガーするダウンストリームの子パイプライン（`source = "parent_pipeline"`）を含む）に MR をリンクします。`source = "merge_request_event"` フィルターなしでは、親子パイプラインのファンアウトを使用する MR では結果が大幅に過剰カウントされ、MR UI や「この MR のパイプライン」の REST および GraphQL の定義と一致しません。マルチノードクエリで `MergeRequest --TRIGGERED--> Pipeline` をトラバースする場合も同じフィルターを適用してください。

`MergeRequest --HAS_HEAD_PIPELINE--> Pipeline` は別のエッジです。マージリクエストのソースブランチの先端に対して実行されている最新の単一パイプラインを指します。パイプラインの履歴ではなく、「現在実行中のもの」に使用してください。

## 集計 {#aggregation}

集計クエリは `aggregations` を使用します。

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `function` | `string` | `count`、`sum`、`avg`、`min`、または `max`。 |
| `target` | `string` | 集計するノードエイリアス。 |
| `property` | `string` | 集計するプロパティ。`sum`、`avg`、`min`、および `max` に必須。 |
| `alias` | `string` | 出力カラムの名前。 |

集計行をグループ化するにはトップレベルの `group_by` を使用します。クエリ内のすべての集計に適用されます。個々の集計内にグループ化を入れないでください。

グループキーは以下の形式をサポートします：

| グループキー | 形式 | 結果の値 |
|-----------|-------|--------------|
| Node | `{"kind": "node", "node": "<node-id>", "alias": "<optional-name>"}` | 各行のネストされたエンティティオブジェクト。 |
| Property | `{"kind": "property", "node": "<node-id>", "property": "<property>", "alias": "<optional-name>"}` | 各行のスカラーバケット値。 |

`alias` を省略した場合、ノードグループはノード ID を出力キーとして使用します。プロパティグループは、`group_by` リスト内で一意の場合はプロパティ名を使用し、曖昧さを避けるために必要な場合は `<node>_<property>` を使用します。グループまたは集計の出力名が重複している場合は拒否されます。

プロパティグループは、呼び出し元が使用を許可されている、実際の ClickHouse バックエンドのフィルター可能なプロパティを参照する必要があります。仮想フィールドおよびフィルター不可能なフィールドは検証時に拒否されます。

プロジェクトごとのマージ済みマージリクエスト数をカウントする：

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "gitlab-org/gitlab"}
    },
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    }
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "project"}
  ],
  "group_by": [{"kind": "node", "node": "project"}],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "merged_mrs"}
  ],
  "aggregation_sort": {"column": "merged_mrs", "direction": "DESC"},
  "limit": 10
}
```

重大度別に検出された脆弱性をカウントする：

```json
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    }
  ],
  "group_by": [
    {"kind": "property", "node": "v", "property": "severity", "alias": "severity"}
  ],
  "aggregations": [
    {"function": "count", "target": "v", "alias": "vulnerability_count"}
  ],
  "aggregation_sort": {"column": "vulnerability_count", "direction": "DESC"},
  "limit": 10
}
```

集計レスポンスはテーブル形式です。`columns` は計算された集計値を表し、`group_columns` はグループ化キーを表し、`rows` はグループ値とメトリクス値を保持します。ノードグループ化された行はグループキーの下にグループ化されたエンティティを格納します。プロパティグループ化された行はグループキーの下にスカラーバケットを格納します。

`collect` は入力タイプに記載されていますが、現在は検証によって拒否されます。

## パス検索 {#path-finding}

パス検索クエリは `path` を使用します。

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `type` | `string` | `shortest`、`all_shortest`、または `any`。 |
| `from` | `string` | 開始ノードセレクターのエイリアス。 |
| `to` | `string` | 終了ノードセレクターのエイリアス。 |
| `max_depth` | `integer` | 最大パス長。最大 3。 |
| `rel_types` | `array` | トラバースするリレーションシップタイプ。両方のエンドポイントが `node_ids` を使用する場合を除き必須。 |

両方のエンドポイントは `node_ids`、フィルター、またはスパンが 500 以下の `id_range` によって有界である必要があります。いずれかのエンドポイントがフィルターまたは `id_range` を使用する場合は、`rel_types` を指定してください。

```json
{
  "query_type": "path_finding",
  "nodes": [
    {"id": "start", "entity": "Project", "node_ids": [278964]},
    {"id": "end", "entity": "User", "node_ids": [1]}
  ],
  "path": {
    "type": "shortest",
    "from": "start",
    "to": "end",
    "max_depth": 3,
    "rel_types": ["CREATOR", "AUTHORED", "IN_PROJECT"]
  },
  "limit": 5
}
```

## 近傍 {#neighbors}

近傍クエリは 1 つの `node` セレクターと `neighbors` オブジェクトを使用します。中心ノードは `node_ids`、フィルター、または狭い `id_range` によって有界である必要があります。

```json
{
  "query_type": "neighbors",
  "node": {
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345]
  },
  "neighbors": {
    "node": "mr",
    "direction": "both",
    "rel_types": ["AUTHORED", "IN_PROJECT", "HAS_DIFF"]
  },
  "options": {
    "dynamic_columns": "default"
  },
  "limit": 25
}
```

動的に検出された近傍またはパスノードのすべての非制限 ClickHouse バックエンドカラムが必要な場合は、`options.dynamic_columns` を `"*"` に設定してください。仮想カラムはトラバーサルクエリで明示的なリクエストが引き続き必要です。

## 検証の制限 {#validation-limits}

Orbit は SQL をコンパイルする前に、広範または曖昧なクエリを拒否します。

| 制限 | 値 |
|-------|-------|
| クエリあたりのノード数 | 5 |
| クエリあたりのリレーションシップ数 | 5 |
| クエリあたりの集計数 | 10 |
| セレクターあたりの `node_ids` 数 | 500 |
| `in` フィルターの値の数 | 100 |
| ノードセレクターあたりのカラム数 | 50 |
| セレクターあたりのリレーションシップタイプ数 | 10 |
| リレーションシップのホップ数 | 3 |
| パスの深さ | 3 |
| ノードあたりのフィルター数 | 10 |
| リレーションシップあたりのフィルター数 | 5 |

トラバーサルおよび集計クエリには、少なくとも 1 つの選択的なノードが必要です：`node_ids`、フィルター、またはスパンが 100,000 以下の `id_range`。

単一ノードのトラバーサルも選択性が必要です。広範なエンティティを検査するには、フィルターを追加するか、ID を指定するか、狭い `id_range` を使用してください。

## オプション {#options}

| オプション | 説明 |
|--------|-------------|
| `dynamic_columns` | `path_finding` および `neighbors` のハイドレーション用。各エンティティのデフォルトカラムには `default` を、すべての非制限 ClickHouse バックエンドカラムには `"*"` を使用します。デフォルト `default`。 |
| `include_debug_sql` | 呼び出し元が参照を許可されている場合、レスポンスメタデータにコンパイル済みの ClickHouse SQL を含めます。 |
| `skip_dedup` | トラバーサル、近傍、およびパス検索クエリの ReplacingMergeTree 重複排除パスをスキップします。集計には使用できません。 |
| `materialize_ctes` | 再利用される CTE をマテリアライズ済みとしてマークします。 |
| `use_semi_join` | 対象となる `IN` サブクエリをセミジョインに書き換えます。 |
| `auth_scope_cascade` | 認証スコープのカスケードシーディングを強制します。 |
| `cascade_distinct` | カスケードおよびホップフロンティア CTE で `SELECT DISTINCT` を出力します。 |

ほとんどの呼び出し元はパフォーマンスオプションを未設定のままにしてください。
```
