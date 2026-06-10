---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Claude Code、Codex、またはMCP対応のAIエージェントを、query_graphとget_graph_schemaの2つのMCPツールを使用してOrbitに接続します。
title: MCPを使用してOrbitにアクセスする
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

OrbitはMCP対応のAIエージェントがGitLabのナレッジグラフをクエリできる2つのMCPツールを公開しています。Claude Code、OpenAI Codex、またはModel Context Protocolをサポートするその他のツールと組み合わせて使用できます。

## 前提条件 {#prerequisites}

- Orbitが[グループで有効化](../getting-started.md)されていること。
- GitLabへの認証が完了していること。`glab auth login`を実行してください（デフォルトではOAuthを使用。`read_api`スコープを持つパーソナルアクセストークンも使用可能）。
- クエリ対象のグループへのアクセス権限が認証情報に含まれていること。

## MCPツール {#mcp-tools}

| ツール | 説明 |
|------|-------------|
| `query_graph` | OrbitクエリDSLを使用してグラフクエリを実行します。型付きの結果を返します。 |
| `get_graph_schema` | 現在のスキーマ（すべてのノードタイプ、プロパティ、リレーションシップタイプ）を取得します。 |

## MCPクライアントを接続する {#connect-your-mcp-client}

MCPクライアントが`https://gitlab.com/api/v4/orbit/mcp`を指すように設定します。

**Claude Code**は組み込みのHTTPトランスポートを介してOrbitエンドポイントをサポートしています。
次のコマンドで登録できます。

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

最初の`query_graph`または`get_graph_schema`の呼び出し時にブラウザが開き、GitLabで認証が行われます。JSONの設定ファイルを編集する必要はありません。

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
> opencodeでは`"type": "local"`が必要で、コマンドと引数を単一の配列にまとめて指定します。`args`フィールドを別途指定したり、`type`を省略したりすると`ConfigInvalidError`が発生します。

認証には既存の`glab auth login`セッションが使用されるため、トークンのコピーや貼り付けは不要です。対応クライアント: Claude Code、OpenCode、Cursor、Codex、Gemini CLI。

> [!note]
> 計画中の`glab orbit setup`サブコマンドを使用すると、OrbitスキルのインストールとこのMCP設定の書き込みを1ステップで行えるようになります。リリースまでは、上記の手順に従ってMCPクライアントを手動で設定してください。

また、[Orbitスキルを手動でインストール](../../ai_coding_agents.md)することで、エージェントにクエリレシピ、DSLガイダンス、およびトラブルシューティング情報を提供できます。

### 動作確認 {#test-it}

AIエージェントに次のように質問してください。

> 「Orbitを使用して、自分のグループで最近更新された5つのプロジェクトを一覧表示してください。」

プロジェクト名とパスを含む型付きの結果が返されれば、接続は成功しています。結果が返されない場合は、`glab auth status`を実行して認証状態を確認し、少なくとも1つのグループでOrbitが有効になっていることを確認してください。

## 課金 {#billing}

MCP経由のクエリはGitLabクレジットを消費します。`query_graph`へのクエリ呼び出しはGitLabサブスクリプションのクレジットを使用します。`get_graph_schema`の呼び出しは消費対象外です。

## ツールの使用方法 {#using-the-tools}

接続後、AIエージェントにOrbitツールを直接使用するよう指示できます。

スキーマの確認:
> 「`get_graph_schema`を使用して、Orbitがインデックス作成しているノードタイプを表示してください。」

クエリの実行:
> 「`query_graph`を使用して、グループ内でオープンなマージリクエストが最も多い10件のプロジェクトを検索してください。」

影響範囲の分析:
> 「Orbitを使用して、このプロジェクト内で`AuthService`を直接または推移的にインポートしているすべてのファイルを検索してください。」

オンボーディング:
> 「Orbitを使用して、このグループの主要なサービス、使用言語、および依存プロジェクトをマッピングしてください。」

エージェントはJSONクエリDSLを構成し、代わりに`query_graph`を呼び出します。結果を正確に制御したい場合は、生のJSONクエリを直接渡すこともできます。

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
