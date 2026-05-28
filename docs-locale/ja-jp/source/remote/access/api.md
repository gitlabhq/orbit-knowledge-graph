---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: REST APIを使用してOrbitナレッジグラフに直接クエリを実行します。認証要件とリクエスト例を含む4つのエンドポイントのリファレンスです。
title: REST API
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階にあります。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴をご参照ください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

Orbit REST APIを使用すると、スクリプト、CIパイプライン、またはカスタムツールからナレッジグラフに直接クエリを実行できます。

## 認証 {#authentication}

すべてのエンドポイントには、`read_api`スコープを持つGitLabパーソナルアクセストークンが必要です。Bearerトークンとして渡してください。

```shell
--header "Authorization: Bearer <your_token>"
```

結果は、トークンオーナーがGitLabでアクセスできるエンティティにスコープされます。

## 課金 {#billing}

APIコールはサブスクリプションのGitLab Creditsを消費します。`POST /api/v4/orbit/query`への各呼び出しはクレジットを使用します。その他のエンドポイントは無料です。

## エンドポイント {#endpoints}

| メソッド | エンドポイント | 説明 |
|--------|----------|-------------|
| `POST` | `/api/v4/orbit/query` | グラフクエリを実行する |
| `GET` | `/api/v4/orbit/schema` | 現在のスキーマをフェッチする |
| `GET` | `/api/v4/orbit/status` | インデックス作成のステータスを確認する |
| `GET` | `/api/v4/orbit/tools` | 利用可能なMCPツール定義を一覧表示する |

## クエリエンドポイント {#query-endpoint}

Orbitクエリ DSLを使用してグラフクエリを実行します。

リクエストボディには以下が含まれます。

- `query`: Orbitクエリオブジェクト。
- `format`: オプションのレスポンス形式。構造化されたJSONにはrawを、AIエージェント向けに最適化されたコンパクトなテキストには`llm`を使用します。デフォルト: `llm`。

例:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{"query": <query_json>, "format": "raw"}' \
  "https://gitlab.com/api/v4/orbit/query"
```

完全なDSLについては、[クエリ言語リファレンス](../queries/query-language.md)をご参照ください。

### リクエスト例 {#example-request}

例えば、パイプラインの失敗が最も多いプロジェクトを検索するリクエスト:

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{
    "query": {
      "query_type": "aggregation",
      "nodes": [
        {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
        {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
      ],
      "relationships": [
        {"type": "IN_PROJECT", "from": "pl", "to": "p"}
      ],
      "group_by": [{"kind": "node", "node": "p"}],
      "aggregations": [
        {
          "function": "count",
          "target": "pl",
          "alias": "failed_pipelines"
        }
      ],
      "aggregation_sort": {"column": "failed_pipelines", "direction": "DESC"},
      "limit": 10
    },
    "format": "raw"
  }' \
  "https://gitlab.com/api/v4/orbit/query"
```

レスポンス例:

```json
{
  "result": {
    "format_version": "2.0.0",
    "query_type": "aggregation",
    "nodes": [],
    "edges": [],
    "group_columns": [
      {
        "name": "p",
        "kind": "node",
        "node": "p",
        "entity": "Project"
      }
    ],
    "columns": [
      {
        "name": "failed_pipelines",
        "function": "count",
        "target": "pl"
      }
    ],
    "rows": [
      {
        "p": {
          "type": "Project",
          "id": "1",
          "properties": {
            "name": "payments-api",
            "full_path": "my-org/payments-api"
          }
        },
        "failed_pipelines": 47
      }
    ]
  },
  "query_type": "aggregation",
  "raw_query_strings": null,
  "row_count": 1
}
```

## スキーマエンドポイント {#schema-endpoint}

現在のオントロジー（すべてのノードタイプ、そのプロパティと型、およびすべてのリレーションシップタイプ）を返します。

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/schema"
```

クエリを記述する前に、利用可能なエンティティタイプとプロパティを確認するためにご使用ください。

## ステータスエンドポイント {#status-endpoint}

Orbitが有効になっているグループのインデックス作成ステータスを返します。

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

レスポンス例:

```json
{
  "status": "indexed",
  "domains": {
    "sdlc": {"indexed": true, "last_updated": "2026-05-05T14:22:00Z"},
    "code": {"indexed": true, "last_updated": "2026-05-05T14:18:00Z"}
  },
  "projects": {
    "total": 847,
    "indexed": 847
  }
}
```

## ツールエンドポイント {#tools-endpoint}

MCPクライアントと互換性のある形式で、`query_graph`と`get_graph_schema`のMCPツール定義を返します。

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/tools"
```
