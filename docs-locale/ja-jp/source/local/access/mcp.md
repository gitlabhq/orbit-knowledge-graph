---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local MCPサーバーを使用して、AIエージェントをローカルグラフに接続します。
title: GitLab Orbit Local MCPサーバー
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: 実験的機能

{{< /details >}}

{{< history >}}

- GitLab 19.2で[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643)。

{{< /history >}}

GitLab Orbit Local [Model Context Protocol](https://modelcontextprotocol.io/)（MCP）サーバーを使用すると、AIツールやアプリケーションをローカルグラフに安全に接続できます。Claude Code、Codex、Cursor、OpenCodeなどのAIアシスタントは、グラフにアクセスしてSQLクエリを実行できます。

GitLab Orbit Local MCPサーバーはステートレスです。これは、サーバーが以下の特性を持つことを意味します。

- ローカルグラフを参照し、GitLabインスタンスは参照しません。
- 結果をキャッシュしたり、クエリ履歴を保持したりしません。
- 複数のクライアントに対して独立して応答できます。

## 前提条件 {#prerequisites}

- 以下のいずれかのツールをインストールします。
  - [GitLab Orbit CLI](./cli.md)（`orbit`）
  - [GitLab CLI](./glab.md)（`glab orbit`）

<!-- markdownlint-disable-next-line MD044 -->
## GitLab Orbit Local MCPサーバーへのクライアント接続 {#connect-a-client-to-the-gitlab-orbit-local-mcp-server}

GitLab Orbit Local MCPサーバーはstdioトランスポートをサポートしています。引数とコマンドは、MCPクライアントおよびローカル環境によって異なります。

### Claude Codeに接続する {#connect-claude-code}

Claude CodeはMCPサーバーの設定を3つのスコープのいずれかに保存します。ユースケースに合ったスコープを選択してください。

| スコープ | 利用可能範囲 | 保存先 |
| ----- | ------------ | --------- |
| `local`（デフォルト） | 現在のプロジェクトでのみ、自分のみ | `~/.claude.json` |
| `user` | すべてのプロジェクトで、自分のみ | `~/.claude.json` |
| `project` | リポジトリをチェックアウトした全員 | リポジトリルートの`.mcp.json` |

現在のプロジェクトにMCPサーバーを追加するには、次のコマンドを実行します。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

すべてのプロジェクトにサーバーを追加するには、次のコマンドを実行します。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local --scope user -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local --scope user -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

リポジトリをチェックアウトした全員にサーバーを追加するには、次のコマンドを実行します。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
claude mcp add orbit-local --scope project -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
claude mcp add orbit-local --scope project -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

`.mcp.json`ファイルを直接編集することもできます。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

MCPサーバーが接続されていることを確認するには、次のコマンドを実行します。

```shell
claude mcp list
```

接続後、[AIアシスタントをセットアップします](./cli.md)。

> [!note]
> プロジェクトスコープのコマンドの場合、Claude Codeは`mcp.json`ファイルを作成する前に承認を求めます。承認するまで、`claude mcp list`にはサーバーが`Pending approval`として表示されます。

### Codexに接続する {#connect-codex}

Codexは`~/.codex/config.toml`にMCPサーバーの設定を保存します。Codexは常にすべてのプロジェクトに対してサーバーを設定します。

Codexに接続するには、次のコマンドを実行します。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```shell
codex mcp add orbit-local -- orbit mcp serve
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```shell
codex mcp add orbit-local -- glab orbit local mcp serve
```

{{< /tab >}}

{{< /tabs >}}

サーバーが登録されていることを確認するには、次のコマンドを実行します。

```shell
codex mcp list
```

接続後、[AIアシスタントをセットアップします](./cli.md)。

### Cursorに接続する {#connect-cursor}

Cursorは`mcp.json`ファイルからMCPサーバーの設定を読み込みます。サーバーをどの範囲で利用可能にするかに応じてスコープを選択してください。

| スコープ | 利用可能範囲 | 保存先 |
| ----- | ------------ | --------- |
| プロジェクト | 現在のプロジェクトでのみ、自分のみ | リポジトリルートの`.cursor/mcp.json` |
| グローバル | すべてのプロジェクトで、自分のみ | `~/.cursor/mcp.json` |

Cursorに接続するには、使用するスコープの`mcp.json`ファイルを作成または編集します。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "mcpServers": {
    "orbit-local": {
      "type": "stdio",
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

サーバーが接続されていることを確認するには、次のコマンドを実行します。

```shell
agent mcp list
```

### OpenCodeに接続する {#connect-opencode}

リポジトリルートの`opencode.json`、またはすべてのプロジェクトで使用する場合は`~/.config/opencode/opencode.json`にMCPサーバーの設定を追加します。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["orbit", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["glab", "orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

サーバーが接続されていることを確認するには、次のコマンドを実行します。

```shell
opencode mcp list
```

接続後、[AIアシスタントをセットアップします](./cli.md)。

### その他のMCPクライアントに接続する {#connect-other-mcp-clients}

GitLab Orbit Local MCPサーバーは接続URLを公開していません。URLが必要なクライアントは接続できません。

{{< tabs >}}

{{< tab title="GitLab Orbit CLI (orbit)" >}}

GitLab Orbit CLIを使用して接続するには、クライアントのMCP設定ファイルを編集します。

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "orbit",
      "args": ["mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab orbit)" >}}

GitLab CLIを使用して接続するには、次の手順を実行します。

1. `glab orbit local --install`を実行します。このコマンドは`orbit`バイナリをダウンロードします。
1. 次に、クライアントのMCP設定ファイルを編集します。

```json
{
  "mcpServers": {
    "orbit-local": {
      "command": "glab",
      "args": ["orbit", "local", "mcp", "serve"]
    }
  }
}
```

{{< /tab >}}

{{< /tabs >}}

サーバーが接続されていることを確認するには、クライアントが`run_sql`、`get_graph_schema`、および`index`ツールを一覧表示していることを確認します。

## MCPツール {#mcp-tools}

GitLab Orbit Local MCPサーバーは、ローカルグラフと連携するツールセットを提供します。

### `index`

リポジトリ（またはリポジトリのディレクトリ）をローカルグラフにインデックス作成します。

例:

```plaintext
チェックアウト済みのプロジェクトをインデックス作成してください。
```

### `get_graph_schema`

スキーマをフェッチします。ローカルグラフに存在するテーブル名、カラム、データ型が含まれます。

例:

```plaintext
`get_graph_schema`ツールを使って、ローカルグラフに存在するテーブルを表示してください。
```

### `run_sql`

ローカルグラフに対して読み取り専用のSQLを実行します。ステートメントの配列を受け取り、同じインデックス位置にあるステートメントごとに1つのJSON行配列を返します。

1回の`run_sql`呼び出しで返されるデータは、呼び出し内のすべてのステートメントを合計して最大約1 MBです。それより大きい結果は失敗し、エージェントはより絞り込んだクエリで再試行します。エージェントが回復しない場合は、より少ない結果を要求してください。

例:

```plaintext
このリポジトリで最もよく使われているインポートを教えてください。
```
