```markdown
---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Claude Code、Codex、またはMCP対応のAIエージェントを、query_graphとget_graph_schemaの2つのMCPツールを使用してOrbitに接続します。
title: MCP経由で接続する
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト用に利用可能ですが、本番環境での使用には対応していません。

OrbitはMCP対応のAIエージェントがGitLabのナレッジグラフをクエリできる2つのMCPツールを公開しています。Claude Code、OpenAI Codex、またはModel Context Protocolをサポートするその他のツールと組み合わせて使用してください。

## 前提条件 {#prerequisites}

- Orbitが[グループで有効化されています](../getting-started.md)。
- GitLabに認証済みです。`glab auth login`を実行してください（デフォルトではOAuthを使用。`read_api`スコープを持つパーソナルアクセストークンも使用可能です）。
- 認証情報がクエリ対象のグループへのアクセス権を持っています。

## MCPツール {#mcp-tools}

| ツール | 説明 |
|------|-------------|
| `query_graph` | Orbitクエリ DSLを使用してグラフクエリを実行します。型付き結果を返します。 |
| `get_graph_schema` | 現在のスキーマ（すべてのノードタイプ、プロパティ、リレーションシップタイプ）をフェッチします。 |

## MCPクライアントを接続する {#connect-your-mcp-client}

MCPクライアントが`https://gitlab.com/api/v4/orbit/mcp`を指すように設定してください。

**Claude Code**は組み込みのHTTPトランスポートを介してOrbitエンドポイントをサポートしています。
1つのコマンドで登録できます：

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

最初の`query_graph`または`get_graph_schema`の呼び出し時に、ブラウザが開いてGitLabで認証が行われます。JSONの設定ファイルの編集は不要です。

一部のクライアントはローカルのstdio MCPサーバーのみをサポートしています。そのような場合は、[`mcp-remote`](https://www.npmjs.com/package/mcp-remote)がOrbitエンドポイントをローカルコマンドとしてラップします。

**Cursor、Codex、およびその他のJSON設定クライアント** — エージェントのMCP設定に追加してください：

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

**opencode** — `~/.config/opencode/opencode.json`に追加してください：

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
> opencodeは`"type": "local"`が必要で、コマンドと引数を単一の配列にまとめます。`args`フィールドを別に使用したり`type`を省略したりすると、`ConfigInvalidError`が発生します。

認証には既存の`glab auth login`セッションを使用します。トークンのコピーや貼り付けは不要です。サポートされているクライアント：Claude Code、OpenCode、Cursor、Codex、Gemini CLI。

> [!note]
> 計画中の`glab orbit setup`サブコマンドにより、OrbitスキルのインストールとこのMCP設定の書き込みが1ステップで行えるようになります。リリースされるまでは、上記の手順に従ってMCPクライアントを手動で設定してください。

### テストする {#test-it}

AIエージェントで次のように質問してください：

> 「Orbitを使用して、自分のグループで最近更新された5つのプロジェクトを一覧表示してください。」

プロジェクト名とパスを含む型付き結果が返されるはずです。返された場合は接続成功です。返されない場合は、`glab auth status`を実行して認証済みであることを確認し、少なくとも1つのグループでOrbitが有効になっていることを確認してください。

## 課金 {#billing}

MCP経由のクエリはGitLab Creditsを消費します。`query_graph`への各クエリ呼び出しはGitLabサブスクリプションのクレジットを使用します。`get_graph_schema`の呼び出しは無料です。

## ツールの使用 {#using-the-tools}

接続後、AIエージェントにOrbitツールを直接使用するよう指示してください：

スキーマを確認する：
> 「`get_graph_schema`を使用して、Orbitがインデックスするノードタイプを表示してください。」

クエリを実行する：
> 「`query_graph`を使用して、`gitlab-org`グループでオープンなマージリクエストが最も多い10のプロジェクトを検索してください。」

影響範囲の分析：
> 「Orbitを使用して、このプロジェクト内で`AuthService`を直接または推移的にインポートしているすべてのファイルを検索してください。」

オンボーディング：
> 「Orbitを使用して、このグループの主要なサービス、その言語、および依存しているプロジェクトをマップしてください。」

エージェントはJSONクエリ DSLを構成し、代わりに`query_graph`を呼び出します。
結果を正確に制御したい場合は、rawのJSONクエリを直接渡すこともできます。

## 例：手動でのquery_graph呼び出し {#example-manual-querygraph-call}

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
```
