---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: MCPツールquery_graphとget_graph_schemaを使用して、Claude Code、Codex、またはMCP対応のAIエージェントをOrbitに接続します。
title: MCP経由で接続する
---

{{< details >}}

- 階層: Premium, Ultimate
- 提供形態: GitLab.com
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 18.10で`knowledge_graph`という名前の[機能フラグ付き](https://docs.gitlab.com/administration/feature_flags/)で[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階にあります。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴をご参照ください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

OrbitはMCP対応のAIエージェントがGitLabのナレッジグラフをクエリできる2つのMCPツールを提供しています。Claude Code、OpenAI Codex、またはModel Context Protocolをサポートするその他のツールと組み合わせてご利用ください。

## 前提条件 {#prerequisites}

- Orbitが[グループで有効化されている](../getting-started.md)こと。
- GitLabへの認証が完了していること。`glab auth login`を実行してください（デフォルトではOAuthを使用。`read_api`スコープを持つパーソナルアクセストークンも使用可能）。
- クエリ対象のグループへのアクセス権限があること。

## MCPツール {#mcp-tools}

| ツール | 説明 |
|------|-------------|
| `query_graph` | Orbitクエリ DSLを使用してグラフクエリを実行します。型付きの結果を返します。 |
| `get_graph_schema` | 現在のスキーマ（すべてのノードタイプ、プロパティ、リレーションシップタイプ）を取得します。 |

## MCPクライアントを接続する {#connect-your-mcp-client}

MCPクライアントが`https://gitlab.com/api/v4/orbit/mcp`を指すように設定してください。

**Claude Code**は組み込みのHTTPトランスポートを介してOrbitエンドポイントをサポートしています。
次のコマンド1つで登録できます。

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

最初の`query_graph`または`get_graph_schema`の呼び出し時に、ブラウザが開いてGitLabの認証が行われます。JSONの設定ファイルを編集する必要はありません。

ローカルのstdio MCPサーバーのみをサポートするクライアントの場合は、[`mcp-remote`](https://www.npmjs.com/package/mcp-remote)を使用してOrbitエンドポイントをローカルコマンドとしてラップできます。

**Cursor、Codex、およびその他のJSON設定クライアント** — エージェントのMCP設定に以下を追加してください。

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "command": "npx",
      "args": ["mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

**opencode** — `~/.config/opencode/opencode.json`に以下を追加してください。

```json
{
  "mcp": {
    "gitlab-orbit": {
      "type": "local",
      "command": ["npx", "mcp-remote", "https://gitlab.com/api/v4/orbit/mcp"]
    }
  }
}
```

> [!note]
> opencodeでは`"type": "local"`が必要で、コマンドと引数を単一の配列にまとめて指定します。`args`フィールドを別途指定したり`type`を省略したりすると、`ConfigInvalidError`が発生します。

認証には既存の`glab auth login`セッションが使用されるため、トークンのコピーや貼り付けは不要です。対応クライアント: Claude Code、OpenCode、Cursor、Codex、Gemini CLI。

> [!note]
> 計画中の`glab orbit setup`サブコマンドを使用すると、OrbitスキルのインストールとこのMCP設定の書き込みを1ステップで行えるようになります。リリースまでは、上記の手順に従ってMCPクライアントを手動で設定してください。

### テストする {#test-it}

AIエージェントで次のように質問してください。

> 「Orbitを使用して、自分のグループで最近更新された5つのプロジェクトを一覧表示してください。」

プロジェクト名とパスを含む型付きの結果が返されるはずです。結果が返されれば接続成功です。返されない場合は、`glab auth status`を実行して認証状態を確認し、少なくとも1つのグループでOrbitが有効になっているかどうかを確認してください。

## 課金 {#billing}

MCP経由のクエリはGitLab クレジットを消費します。`query_graph`へのクエリ呼び出しはGitLabサブスクリプションのクレジットを使用します。`get_graph_schema`の呼び出しは無料です。

## ツールの使用方法 {#using-the-tools}

接続後、AIエージェントにOrbitツールを直接使用するよう指示してください。

スキーマを確認する:
> 「`get_graph_schema`を使用して、Orbitがインデックスするノードタイプを表示してください。」

クエリを実行する:
> 「`query_graph`を使用して、`gitlab-org`グループでオープンなマージリクエストが最も多い10件のプロジェクトを検索してください。」

影響範囲の分析:
> 「Orbitを使用して、このプロジェクト内で`AuthService`を直接または推移的にインポートしているすべてのファイルを検索してください。」

オンボーディング:
> 「Orbitを使用して、このグループの主要なサービス、使用言語、および依存しているプロジェクトをマップしてください。」

エージェントはJSONクエリ DSLを構成し、代わりに`query_graph`を呼び出します。結果を正確に制御したい場合は、rawのJSONクエリを直接渡すこともできます。

## 例: query_graphの手動呼び出し {#example-manual-querygraph-call}

```json
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]},
    {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "open_mrs"}
  ],
  "aggregation_sort": {"column": "open_mrs", "direction": "DESC"},
  "limit": 10
}
```
