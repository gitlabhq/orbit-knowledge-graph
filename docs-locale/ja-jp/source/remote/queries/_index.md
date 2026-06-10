---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbitナレッジグラフにクエリを実行して、GitLabのデータ、コード、および関係性を検索します。
title: クエリ
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)を使用して、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

Orbitクエリは、グラフ操作を記述するJSONオブジェクトです。クエリでは、特定の種類のオブジェクトのフェッチ、オブジェクト間の関係のトラバース、一致するオブジェクトのカウント、パスの検索、またはノードの隣接要素の取得が可能です。

クエリはGitLabの認可を通じて実行されます。レスポンスには、現在のユーザーがGitLabで読み取り可能なデータのみが含まれます。

## クエリの形式を選択する {#choose-a-query-shape}

| ユースケース | クエリの形式 |
|----------|-------------|
| 1つのエンティティタイプの一致するノードをフェッチする | 単一ノードの`traversal` |
| 既知のエンティティタイプ間の関係をたどる | 複数ノードの`traversal` |
| グラフ結果のカウント、合計、平均、またはグループ化 | `aggregation` |
| 2つの限定されたエンドポイント間のパスを検索する | `path_finding` |
| 1つの限定されたノードに接続されているものを確認する | `neighbors` |

単一ノードの`traversal`は検索の形式です。Orbitには独立した`search`クエリタイプはありません。

## 例: マージリクエストの差分をフェッチする {#example-fetch-a-merge-request-diff}

`MergeRequest`の`diff`カラムを使用して、マージリクエストの完全な統合差分をフェッチします。仮想カラムは名前を明示的に指定してリクエストしてください。

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

マージリクエストの差分コンテンツには、いくつかの異なる形式があります。

| エンティティ | カラム | 返される内容 |
|--------|--------|-----------------|
| `MergeRequest` | `diff` | マージリクエストの完全な統合差分 |
| `MergeRequestDiff` | `patch` | 1つの差分スナップショットの完全なパッチ |
| `MergeRequestDiffFile` | `diff` | ファイルごとの統合差分テキスト |
| `File` | `content` | rawソースファイルテキスト |
| `Definition` | `content` | インデックス作成済みの定義のソーステキスト |

`content`カラムはソースコードノード用です。マージリクエストの差分テキストには、エンティティに応じて`diff`または`patch`を使用してください。

## 例: 差分スナップショットと変更ファイルをフェッチする {#example-fetch-diff-snapshots-and-changed-files}

`HAS_DIFF`を使用してマージリクエストからその差分スナップショットに移動し、次に`HAS_FILE`を使用してそれらのスナップショット内のファイルをフェッチします。

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

`too_large`が`true`の場合、`MergeRequestDiffFile.diff`は`null`になります。

## 例: ソースファイルのコンテンツをフェッチする {#example-fetch-source-file-content}

ソースコードエンティティには`content`を使用します。この例では、パスでインデックス作成済みのファイルを検索し、rawファイルテキストを返します。

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

完全な構文、利用可能なフィールド、および検証ルールについては、[Orbitクエリ言語](query-language.md)を参照してください。
