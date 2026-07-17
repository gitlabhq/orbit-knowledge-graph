---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbitクエリ言語を使用して、ナレッジグラフを検索およびトラバースします。
title: GitLab Orbitクエリ言語
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 18.10で`knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ版](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

フラットなAPIレスポンスではなくグラフとしてGitLabデータが必要な場合は、GitLab Orbitクエリ言語を使用してください。クエリはJSONオブジェクトで、マッチするエンティティ、辿るリレーションシップ、返すプロパティを指定します。

## リクエストエンベロープ {#request-envelope}

REST APIまたは`glab orbit remote query`でクエリを送信する場合は、クエリオブジェクトをトップレベルの`query`フィールドでラップしてください。

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "mr",
      "entity": "MergeRequest",
      "node_ids": [12345],
      "columns": ["iid", "title", "state"]
    }],
    "limit": 1
  },
  "response_format": "raw"
}
```

| フィールド | 必須 | 説明 |
|-------|----------|-------------|
| `query` | はい | 以下に記載するクエリオブジェクト。 |
| `response_format` | いいえ | `"llm"`（省略時のデフォルト。LLM消費向けに最適化されたコンパクトな[GOON](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/docs/design-documents/querying/graph_engine.md)テキスト）または`"raw"`（構造化されたJSON）。`jq`に出力をパイプする場合は`"raw"`を使用してください。 |

`orbit query` CLI（ローカルグラフ用）は、エンベロープ**なし**でrawクエリボディを受け取ります。

## クエリの形式 {#query-shape}

すべてのクエリには`query_type`と、ノードセレクターの`nodes`配列が必要です。

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state"]
  }],
  "limit": 1
}
```

## クエリタイプ {#query-types}

| クエリタイプ | 用途 |
|------------|-----------|
| `traversal` | マッチするノードのフェッチ、またはノード間のリレーションシップの追跡。 |
| `aggregation` | マッチするグラフ結果のカウント、合計、平均、グループ化、またはソート。 |
| `path_finding` | 2つのノードセレクター間の有界パスの検索。 |
| `neighbors` | 1つの有界ノードに接続されたノードの返却。 |

単一ノードの`traversal`が検索の形式です。独立した`search`クエリタイプはありません。

## トップレベルフィールド {#top-level-fields}

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `query_type` | `string` | `traversal`、`aggregation`、`path_finding`、または`neighbors`のいずれか。 |
| `nodes` | `array` | ノードセレクター。常に必須。単一ノードクエリ（`neighbors`、検索形式の`traversal`）は1要素の配列を使用します。最大5個。 |
| `relationships` | `array` | トラバーサルまたは集計のリレーションシップセレクター。最大5個。 |
| `aggregations` | `array` | 集計定義。`aggregation`に必須。最大10個。 |
| `group_by` | `array` | 集計行のグループキー。最大4個。 |
| `path` | `object` | パス検索の設定。`path_finding`に必須。 |
| `neighbors` | `object` | 近傍ルックアップの設定。`neighbors`に必須。 |
| `limit` | `integer` | `cursor`が設定されていない場合に返す最大行数。デフォルト30。最大1000。レスポンスの`pagination.truncated`を確認してください。trueの場合、マッチする行がさらに存在します。 |
| `cursor` | `object` | キーセットページネーション: 最初のページには`{"page_size": N}`、その後`next_cursor`がなくなるまで`{"page_size": N, "after": "<pagination.next_cursor>"}`を使用します。データセットのサイズに関わらず、すべての行に到達できます。トークンは発行したクエリに紐付けられています。 |
| `order_by` | `object` | ノードプロパティで行をソート。 |
| `aggregation_sort` | `object` | 出力列で集計行をソート。 |
| `options` | `object` | 表示およびデバッグオプション。 |

ページネーションはリクエスト時にライブデータを読み取るため、スナップショットはありません。各ページは独立してすべての行の最新バージョンを解決し、ソフト削除された行をフィルタリングするため、ページ間でのバージョン変更やトゥームストーンのクリーンアップによって結果がスキップまたは重複することはありません。ソート順でカーソル位置より後に挿入された行は後のページに表示され、カーソルより前に挿入または並び替えられた行は再訪されません。ソートキーがNULLの行は最後にソートされ、他の行と同様にページネーションされます。ページ間でソートキーが変わった行は、スナップショットなしのキーセットページネーションと同様に、2回表示されるか、まったく表示されない場合があります。

## ノードセレクター {#node-selectors}

ノードセレクターはオントロジー内の1つのエンティティタイプを指定します。

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `id` | `string` | ノードのローカルエイリアス。リレーションシップ、集計、パス、および近傍はこのエイリアスを参照します。 |
| `entity` | `string` | `Project`、`User`、`MergeRequest`、`File`、`Definition`などのオントロジーノードタイプ。 |
| `columns` | `string`または`array` | 返すプロパティ。すべての非制限プロパティには`"*"`を、名前の配列を使用します。省略した場合、GitLab Orbitはエンティティのデフォルト列を返します。 |
| `filters` | `object` | プロパティフィルター。 |
| `node_ids` | `array` | マッチする正確なID。整数または数字文字列を受け付けます。最大500個。 |
| `id_range` | `object` | `start`と`end`を持つ包括的なID範囲。 |
| `id_property` | `string` | `node_ids`と`id_range`で使用するプロパティ。デフォルト`id`。 |

グラフIDが既にわかっている場合は`node_ids`を使用し、`username`、`full_path`、`state`、`path`などの自然なプロパティがわかっている場合は`filters`を使用してください。

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
| `type` | `string`または`array` | リレーションシップタイプ（1つまたは複数）。有界クエリで任意のリレーションシップが必要な場合のみ`"*"`を使用してください。 |
| `from` | `string` | 開始ノードセレクターのエイリアス。 |
| `to` | `string` | 終了ノードセレクターのエイリアス。 |
| `direction` | `string` | `outgoing`、`incoming`、または`both`。デフォルト`outgoing`。 |
| `min_hops` | `integer` | 最小ホップ数。デフォルト1。最大3。 |
| `max_hops` | `integer` | 最大ホップ数。デフォルト1。最大3。 |
| `filters` | `object` | リレーションシッププロパティフィルター。最大5フィルター。 |

例えば、マージリクエストは`IN_PROJECT`でプロジェクトを指し、ユーザーは`AUTHORED`でマージリクエストを指します。

## フィルター {#filters}

フィルターは単純な等値比較を使用できます。

```json
{
  "filters": {
    "state": "merged"
  }
}
```

または演算子を使用できます。

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
| `in` | 値が配列内に含まれる。最大100個の値。 |
| `contains` | 文字列が部分文字列を含む。 |
| `starts_with` | 文字列がプレフィックスで始まる。 |
| `ends_with` | 文字列がサフィックスで終わる。 |
| `is_null` | 値がnull。`value`は指定しないでください。 |
| `is_not_null` | 値がnullでない。`value`は指定しないでください。 |
| `token_match` | テキストインデックスが1つのトークンを含む。 |
| `all_tokens` | テキストインデックスがすべてのトークンを含む。 |
| `any_tokens` | テキストインデックスがいずれかのトークンを含む。 |

トークン演算子はテキストインデックスを持つプロパティにのみ機能します。

### テキストインデックス付きプロパティ {#text-indexed-properties}

以下のプロパティは`token_match`、`all_tokens`、および`any_tokens`をサポートしています。
これらの演算子を他のプロパティに使用すると、完全な文字列スキャンにフォールバックするため、処理が遅くなります。

<!-- The table below is generated from the ontology's `text(...)` storage indexes. -->
<!-- Do not edit it by hand: run `mise run docs:query-language` and commit. CI fails on drift. -->
<!-- BEGIN GENERATED: text-indexed-properties -->

| エンティティ | テキストインデックス付きプロパティ |
|--------|------------------------|
| `Branch` | `name` |
| `Definition` | `file_path`、`fqn`、`name` |
| `Deployment` | `ref` |
| `Directory` | `name`、`path` |
| `Environment` | `environment_type`、`name` |
| `File` | `name`、`path` |
| `Finding` | `description`、`name` |
| `Group` | `description`、`name` |
| `ImportedSymbol` | `file_path`、`import_path` |
| `Job` | `name`、`ref` |
| `Label` | `description`、`title` |
| `MergeRequest` | `description`、`source_branch`、`target_branch`、`title` |
| `MergeRequestDiffFile` | `new_path`、`old_path` |
| `Milestone` | `description`、`title` |
| `Note` | `note` |
| `Pipeline` | `ref` |
| `Project` | `description`、`name` |
| `Runner` | `name` |
| `Stage` | `name` |
| `User` | `name`、`username` |
| `Vulnerability` | `description`、`title` |
| `VulnerabilityIdentifier` | `external_id`、`external_type`、`name` |
| `VulnerabilityOccurrence` | `description`、`name` |
| `VulnerabilityScanner` | `external_id`、`name` |
| `WorkItem` | `description`、`title` |

<!-- END GENERATED: text-indexed-properties -->

## 列と仮想列 {#columns-and-virtual-columns}

ほとんどの列はClickHouseのインデックス付きグラフテーブルから取得されます。一部の列は仮想列で、グラフクエリが返された後に別のサービスからGitLab Orbitがフェッチします。

仮想列は`columns`で明示的にリクエストしてください。`path_finding`と`neighbors`で使用される`dynamic_columns`オプションは、外部サービス呼び出しが必要になる可能性があるため、仮想列を除外します。

| エンティティ | 仮想列 | 返す内容 |
|--------|----------------|-----------------|
| `MergeRequest` | `diff` | マージリクエストの完全な統合差分。 |
| `MergeRequestDiff` | `patch` | 1つのマージリクエスト差分スナップショットの完全なパッチ。 |
| `MergeRequestDiffFile` | `diff` | ファイルごとの統合差分テキスト。`too_large`が`true`の場合は`null`を返します。 |
| `File` | `content` | ファイルのrawソーステキスト。 |
| `Definition` | `content` | 1つのインデックス付き定義のソーステキスト。 |

`content`列はソースコード用です。マージリクエストの差分テキストには、`MergeRequest.diff`、`MergeRequestDiff.patch`、または`MergeRequestDiffFile.diff`を使用してください。

## トラバーサルの例 {#traversal-examples}

完全な差分を含む1つのマージリクエストをフェッチする:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345],
    "columns": ["iid", "title", "state", "diff"]
  }],
  "limit": 1
}
```

差分スナップショットからファイルごとの差分コンテンツをフェッチする:

```json orbit-query
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

`HAS_DIFF`はマージリクエストがこれまでに持っていたすべての差分スナップショットを返します（`MergeRequestDiff.merge_request_id` FK）。`HAS_LATEST_DIFF`は最新のスナップショットのみを返します（`MergeRequest.latest_merge_request_diff_id` FK）。これは「マージリクエストが現在どのような状態か」を確認するのに便利ですが、過去の質問には適していません。「あるファイルに触れたすべてのマージリクエスト」を調べるには、すべてのスナップショットに対して`HAS_DIFF`をトラバースしてください。長期間存在するファイルに対して`HAS_LATEST_DIFF`を使用すると、過去のカバレッジに関する質問で大幅に過少カウントになる可能性があります。以前のリビジョンでファイルに触れたが最終差分では触れていないMRは、`HAS_LATEST_DIFF`では見えません。

`MergeRequestDiffFile.old_path`はファイルルックアップに推奨される列です。`new_path`はリネームの場合のみ`old_path`と異なります。`old_path`でフィルタリングおよびグループ化することで、MRの履歴全体で同じ行IDが維持されます。[`merge_request_diff_file.yaml`](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/config/ontology/nodes/code_review/merge_request_diff_file.yaml)のオントロジーフィールドの説明を参照してください。

ソースファイルのコンテンツをフェッチする:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "file",
    "entity": "File",
    "filters": {
      "path": {"op": "ends_with", "value": "app/models/project.rb"}
    },
    "columns": ["path", "language", "content"]
  }],
  "limit": 5
}
```

特定の関数またはクラス定義のソーステキストをフェッチする。`content`列はファイル全体ではなく、その定義のrawソーステキストのみを返します。完全一致には`fqn`（完全修飾名）を使用し、より広い検索には`name`と`contains`を使用してください。

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "d",
    "entity": "Definition",
    "filters": {
      "fqn": {"op": "eq", "value": "Gitlab::Auth::authenticate"}
    },
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"]
  }],
  "limit": 5
}
```

プロジェクト内のマージ済みマージリクエストを検索する:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"},
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

1つのマージリクエストに対して実行されたすべてのパイプラインを検索する。マージリクエストの**パイプライン**タブに表示される内容と一致させるために、常に`Pipeline.source = "merge_request_event"`でフィルタリングしてください。

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [{
    "id": "p",
    "entity": "Pipeline",
    "filters": {
      "merge_request_id": {"op": "eq", "value": 482908721},
      "source": {"op": "eq", "value": "merge_request_event"}
    },
    "columns": ["id", "status", "source", "sha", "ref", "created_at"]
  }],
  "order_by": "-p.created_at",
  "limit": 100
}
```

`merge_request_id`はマージリクエストの内部数値`id`であり、プロジェクトスコープの`iid`ではありません。まず`iid`と`project_id`でフィルタリングする`MergeRequest`トラバーサルで調べてから、その`id`を上記のクエリに使用してください。

`Pipeline.merge_request_id`と`MergeRequest --TRIGGERED--> Pipeline`エッジはどちらも、MRのコンテキストで起動されたすべてのCIパイプライン（トップレベルのMRパイプラインがトリガーするダウンストリームの子パイプライン（`source = "parent_pipeline"`）を含む）にMRをリンクします。`source = "merge_request_event"`フィルターなしでは、親子パイプラインのファンアウトを使用するMRで大幅に過剰カウントになり、MRの**パイプライン**タブに表示される内容と一致しません。マルチノードクエリで`MergeRequest --TRIGGERED--> Pipeline`をトラバースする場合も同じフィルターを適用してください。

`MergeRequest --HAS_HEAD_PIPELINE--> Pipeline`は別のエッジです。マージリクエストのソースブランチの先端に対して実行されている最新の単一パイプラインを指します。パイプラインの履歴ではなく、「現在実行中のもの」を確認する場合に使用してください。

## 集計 {#aggregation}

集計クエリは`aggregations`を使用します。

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `function` | `string` | `count`、`sum`、`avg`、`min`、または`max`。 |
| `target` | `string` | 集計するノードエイリアス。 |
| `property` | `string` | 集計するプロパティ。`sum`、`avg`、`min`、および`max`に必須。 |
| `alias` | `string` | 出力列の名前。 |

プロパティタイプのサポートは関数によって異なります。

| 関数 | `property`が必須 | サポートされるプロパティタイプ |
|----------|---------------------|--------------------------|
| `count` | いいえ | N/A |
| `sum` | はい | 数値のみ |
| `avg` | はい | 数値のみ |
| `min` | はい | 数値、文字列、ブール値、`Date`、または`DateTime` |
| `max` | はい | 数値、文字列、ブール値、`Date`、または`DateTime` |

`sum`と`avg`は`DateTime`プロパティをバリデーションエラーで拒否します。日付を集計するには`min`または`max`を使用してください。

トップレベルの`group_by`を使用して集計行をグループ化します。これはクエリ内のすべての集計に適用されます。個々の集計内にグループ化を入れないでください。

グループキーは以下の形式をサポートしています。

| グループキー | 形式 | 結果の値 |
|-----------|-------|--------------|
| ノード | `{"kind": "node", "node": "<node-id>", "alias": "<optional-name>"}` | 各行にネストされたエンティティオブジェクト。 |
| プロパティ | `{"kind": "property", "node": "<node-id>", "property": "<property>", "alias": "<optional-name>"}` | 各行のスカラーバケット値。 |

`alias`を省略した場合、ノードグループはノードIDを出力キーとして使用します。プロパティグループは、`group_by`リスト内で一意の場合はプロパティ名を使用し、曖昧さを避けるために必要な場合は`<node>_<property>`を使用します。グループまたは集計の出力名が重複している場合は拒否されます。

プロパティグループは、呼び出し元が使用を許可されている、実際のClickHouseバックエンドのフィルタリング可能なプロパティを参照する必要があります。仮想フィールドとフィルタリング不可能なフィールドはバリデーション中に拒否されます。

プロジェクトごとのマージ済みマージリクエスト数をカウントする:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "project",
      "entity": "Project",
      "filters": {"full_path": "your-group/your-project"}
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

重大度別に検出された脆弱性をカウントする:

```json orbit-query
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

集計レスポンスはテーブル形式です。`columns`は計算された集計値を、`group_columns`はグループ化キーを、`rows`はグループ値とメトリクス値を保持します。ノードグループ化された行はグループキーの下にグループ化されたエンティティを格納します。プロパティグループ化された行はグループキーの下にスカラーバケットを格納します。

`collect`は入力タイプに記載されていますが、現在はバリデーションで拒否されます。

## パス検索 {#path-finding}

パス検索クエリは`path`を使用します。

| フィールド | 型 | 説明 |
|-------|------|-------------|
| `type` | `string` | `shortest`。 |
| `from` | `string` | 開始ノードセレクターのエイリアス。 |
| `to` | `string` | 終了ノードセレクターのエイリアス。 |
| `max_depth` | `integer` | 最大パス長。最大3。 |
| `rel_types` | `array` | トラバースするリレーションシップタイプ。両方のエンドポイントが`node_ids`を使用する場合を除き必須。 |

両方のエンドポイントは`node_ids`、フィルター、または500以下のスパンを持つ`id_range`で有界である必要があります。いずれかのエンドポイントがフィルターまたは`id_range`を使用する場合は、`rel_types`を指定してください。

```json orbit-query
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

近傍クエリは1要素の`nodes`配列と`neighbors`オブジェクトを使用します。中心ノードは`node_ids`、フィルター、または狭い`id_range`で有界である必要があります。

```json orbit-query
{
  "query_type": "neighbors",
  "nodes": [{
    "id": "mr",
    "entity": "MergeRequest",
    "node_ids": [12345]
  }],
  "neighbors": {
    "direction": "both",
    "rel_types": ["AUTHORED", "IN_PROJECT", "HAS_DIFF"]
  },
  "options": {
    "dynamic_columns": "default"
  },
  "limit": 25
}
```

動的に検出された近傍またはパスノードのすべての非制限ClickHouseバックエンド列が必要な場合は、`options.dynamic_columns`を`"*"`に設定してください。仮想列はトラバーサルクエリでの明示的なリクエストが引き続き必要です。

## バリデーション制限 {#validation-limits}

GitLab OrbitはSQLをコンパイルする前に、広範または曖昧なクエリを拒否します。

| 制限 | 値 |
|-------|-------|
| クエリあたりのノード数 | 5 |
| クエリあたりのリレーションシップ数 | 5 |
| クエリあたりの集計数 | 10 |
| セレクターあたりの`node_ids`数 | 500 |
| `in`フィルターの値数 | 100 |
| ノードセレクターあたりの列数 | 50 |
| セレクターあたりのリレーションシップタイプ数 | 10 |
| リレーションシップホップ数 | 3 |
| パスの深さ | 3 |
| ノードあたりのフィルター数 | 10 |
| リレーションシップあたりのフィルター数 | 5 |

トラバーサルおよび集計クエリには、少なくとも1つの選択的なノード（`node_ids`、フィルター、または100,000以下のスパンを持つ`id_range`）が必要です。

単一ノードのトラバーサルも選択性が必要です。広範なエンティティを検査するには、フィルターを追加するか、IDを指定するか、狭い`id_range`を使用してください。

## オプション {#options}

| オプション | 説明 |
|--------|-------------|
| `dynamic_columns` | `path_finding`と`neighbors`のハイドレーション用。各エンティティのデフォルト列には`default`を、すべての非制限ClickHouseバックエンド列には`"*"`を使用します。デフォルト`default`。 |
| `include_debug_sql` | 呼び出し元が参照を許可されている場合、レスポンスメタデータにコンパイル済みClickHouse SQLを含めます。 |
