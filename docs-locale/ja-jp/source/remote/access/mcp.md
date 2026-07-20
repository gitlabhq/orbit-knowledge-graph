---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: MCPツールlist_commandsとinvoke_commandを使用して、Claude Code、Codex、またはMCP対応のAIエージェントをGitLab Orbitに接続します。
title: MCPを使用してOrbitにアクセスする
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

GitLab OrbitはMCP対応のAIエージェントがGitLabのナレッジグラフに対してGitLab Orbitコマンドを検出・実行できる2つのMCPツールを公開しています。Claude Code、OpenAI Codex、またはModel Context Protocolをサポートするその他のツールと組み合わせて使用できます。

## 前提条件 {#prerequisites}

- GitLab Orbitが[グループで有効化](../getting-started.md)されていること。
- GitLabに認証済みであること。`glab auth login`を実行してください（デフォルトではOAuthを使用。`read_api`スコープを持つパーソナルアクセストークンも使用可能）。
- クエリ対象のグループへのアクセス権限があること。
- MCPクライアントが（`mcp-remote`を経由せず）ネイティブHTTPで直接接続する場合、OAuthリクエストに`mcp_orbit`スコープを含める必要があります。以下のGemini CLIの例を参照してください。

## MCPツール {#mcp-tools}

| ツール | 説明 |
|------|-------------|
| `list_commands` | 説明と入力スキーマを含む利用可能なGitLab Orbitコマンドを一覧表示します。 |
| `invoke_command` | 名前とパラメーターでコマンドを実行します。型付きの結果を返します。 |

`invoke_command`で利用可能なコマンド:

| コマンド | 説明 |
|---------|-------------|
| `query_graph` | OrbitクエリDSLを使用してグラフクエリを実行します。 |
| `get_graph_schema` | 現在のスキーマ（すべてのノードタイプ、プロパティ、リレーションシップタイプ）を取得します。 |
| `get_query_dsl` | `query_graph` JSONのDSL文法とバージョンを返します。 |
| `get_response_format` | `query_graph`レスポンスのJSONスキーマとバージョンを返します。 |

## MCPクライアントを接続する {#connect-your-mcp-client}

MCPクライアントが`https://gitlab.com/api/v4/orbit/mcp`を指すように設定します。

**Claude Code**は組み込みのHTTPトランスポートを使用してOrbitエンドポイントをサポートしています。
次のコマンドで登録します:

```shell
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

最初の`list_commands`または`invoke_command`の呼び出し時にブラウザが開き、GitLabで認証が行われます。JSONの設定ファイルを編集する必要はありません。

> [!note]
> Claude CodeはHTTPで直接接続します。Claude Codeで`npx mcp-remote`を使用しないでください。エンドポイントをstdioプロセスでラップするため、組み込みトランスポートと競合し、「Failed to connect」エラーが発生します。代わりに上記の`claude mcp add --transport http`コマンドを使用してください。

一部のクライアントはローカルのstdio MCPサーバーのみをサポートしています。そのような場合は、[`mcp-remote`](https://www.npmjs.com/package/mcp-remote)を使用してOrbitエンドポイントをローカルコマンドとしてラップします。

**Cursor、Codex、およびその他のJSON設定クライアント** — エージェントのMCP設定に以下を追加します:

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

**opencode** — `~/.config/opencode/opencode.json`に以下を追加します:

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

**Gemini CLI** — ネイティブHTTPトランスポートでOrbitエンドポイントをサポートしています。`~/.gemini/settings.json`に以下を追加します:

```json
{
  "mcpServers": {
    "gitlab-orbit": {
      "url": "https://gitlab.com/api/v4/orbit/mcp",
      "type": "http",
      "timeout": 5000,
      "oauth": {
        "enabled": true,
        "scopes": ["mcp_orbit"]
      }
    }
  }
}
```

`gemini mcp add gitlab-orbit https://gitlab.com/api/v4/orbit/mcp -t http -s user`で生成した後、`oauth.scopes`ブロックを手動で追加することもできます。

> [!note]
> ネイティブHTTP MCPクライアントは`mcp_orbit` OAuthスコープを明示的にリクエストする必要があります。
> `oauth.scopes: ["mcp_orbit"]`がない場合、GitLabに既にサインインしていても認証に失敗します。ネイティブHTTPトランスポートのクライアントで認証できない場合は、MCPサーバー設定にこのスコープを追加してください。
>
> 古いGemini CLIの設定では`url` + `type: "http"`の代わりに`httpUrl`が使用されている場合があります。
> `httpUrl`は引き続き機能しますが非推奨です。新しい設定では`url` + `type`を使用してください。

**Antigravity** — Antigravity IDEとCLIは`~/.gemini/config/mcp_config.json`にある同じMCP設定を読み込みます。Antigravityはリモートサーバーに対するMCP OAuthフローをまだサポートしていないため（ネイティブの`serverUrl`エントリはトークンなしで`initialize`を送信し、`Unauthorized`で失敗します）、`mcp-remote`でエンドポイントをラップします:

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

> [!note]
> ここでは`oauth`ブロックは不要です。`mcp-remote`がエンドポイントのOAuthメタデータから`mcp_orbit`スコープを検出し、初回使用時にブラウザを開いて認証を行います。

認証には既存の`glab auth login`セッションを使用します。トークンのコピーや貼り付けは不要です。サポートされているクライアント: Claude Code、OpenCode、Cursor、Codex、Gemini CLI、Antigravity。

> [!note]
> 計画中の`glab orbit setup`サブコマンドにより、GitLab OrbitスキルのインストールとこのMCP設定の書き込みを1ステップで行えるようになります。リリースまでは、上記の手順に従ってMCPクライアントを手動で設定してください。

また、[GitLab Orbitスキルを手動でインストール](../../ai_coding_agents.md)することで、エージェントにクエリレシピ、DSLガイダンス、トラブルシューティング情報を提供できます。

### 動作確認 {#test-it}

AIエージェントで次のように質問します:

> 「Orbitを使用して、グループ内で最近更新された5つのプロジェクトを一覧表示してください。」

プロジェクト名とパスを含む型付きの結果が返されれば、接続は成功しています。結果が返されない場合は、`glab auth status`を実行して認証状態を確認し、少なくとも1つのグループでGitLab Orbitが有効になっていることを確認してください。

## 課金 {#billing}

MCP経由のクエリはGitLabクレジットを消費します。`query_graph`を実行する`invoke_command`の呼び出しはGitLabサブスクリプションのクレジットを使用します。`list_commands`、`get_graph_schema`、`get_query_dsl`、`get_response_format`コマンドは消費対象外です。

## ツールの使用方法 {#using-the-tools}

接続後、AIエージェントにGitLab Orbitツールを直接使用するよう指示します:

コマンドとスキーマの確認:
> 「`list_commands`を使用して利用可能なGitLab Orbitコマンドを表示し、次に`get_graph_schema`コマンドを実行してGitLab Orbitがインデックス作成するノードタイプを確認してください。」

クエリの実行:
> 「`query_graph`コマンドを使用して、グループ内でオープンなマージリクエストが最も多い10件のプロジェクトを検索してください。」

影響範囲の分析:
> 「Orbitを使用して、このプロジェクト内で`AuthService`を直接または推移的にインポートしているすべてのファイルを検索してください。」

オンボーディング:
> 「Orbitを使用して、このグループの主要なサービス、使用言語、および依存プロジェクトをマップしてください。」

エージェントはJSONクエリDSLを構成し、代わりに`query_graph`コマンドを呼び出します。結果を正確に制御したい場合は、生のJSONクエリを直接渡すこともできます。

## 例: invoke_commandによるquery_graphの手動呼び出し {#example-manual-invokecommand-call-for-querygraph}

以下のクエリを`invoke_command`に`{"command_name": "query_graph", "parameters": {"query": ...}}`の形式で渡します:

```json orbit-query
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
  "aggregation_sort": "-open_mrs",
  "limit": 10
}
```

## トラブルシューティング {#troubleshooting}

### Claude Codeで「Failed to connect」が表示される {#failed-to-connect-in-claude-code}

Claude Codeには組み込みのHTTP MCPサポートがあります。`--transport http`の代わりに`npx mcp-remote`でOrbitを登録した場合、`mcp-remote`ラッパーがローカルのstdioプロセスを作成し、ネイティブトランスポートと競合します。

修正するには、壊れた登録を削除してHTTPトランスポートで再登録します:

```shell
claude mcp remove gitlab-orbit
claude mcp add --transport http gitlab-orbit https://gitlab.com/api/v4/orbit/mcp
```

### 初回使用時に「Needs authentication」が表示される {#needs-authentication-on-first-use}

これは想定された動作です。最初の`list_commands`または`invoke_command`の呼び出し時にブラウザが開き、GitLabでOAuth認証が完了します。ブラウザフローが起動しない場合は、セッションを確認してください:

```shell
glab auth status
```

セッションが期限切れの場合は、再認証してください:

```shell
glab auth login
```

### 接続後のクエリエラー {#query-errors-after-connecting}

クエリ時のエラー（検証の失敗、空の結果、レート制限）については、DSLガイダンス、クエリレシピ、終了コードの診断情報が含まれる[GitLab Orbitスキルのドキュメント](../../ai_coding_agents.md)を参照してください。インラインガイダンスのためにスキルをインストールします:

```shell
glab skills install --global orbit
```
