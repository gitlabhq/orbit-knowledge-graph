---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Claude Code、Codex、またはMCP対応のAIエージェントをローカルのOrbitグラフに接続します。
title: MCPを使用してOrbit Localに接続する
---

{{< details >}}

- 階層: Free, Premium, Ultimate
- 提供形態: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

Orbit LocalはMCPサーバーとして動作し、Orbit Remoteと同じ2つのツール（`query_graph`、`get_graph_schema`）を公開しますが、GitLabインスタンスではなくローカルのDuckDBグラフを参照します。

> [!note]
> MCPサーバーは実験的な機能です。GAまでに機能や設定の形式が変更される場合があります。

## 前提条件 {#prerequisites}

- Orbit CLI（`orbit`）がインストールされていること。[Orbit CLIを直接使用する](cli.md)を参照してください。
- ローカルリポジトリがインデックス済みであること（`orbit index <path>` または `glab orbit local index <path>`）。

## MCPツール {#mcp-tools}

| ツール | 説明 |
|------|-------------|
| `run_sql` | ローカルのDuckDBグラフに対して読み取り専用のSQLクエリを実行します。 |
| `get_graph_schema` | スキーマ（ローカルのDuckDBに存在するテーブル名、カラム、データ型）をフェッチします。 |

Orbit Remote（JSON形式のクエリDSLを公開）とは異なり、Orbit LocalはrawのDuckDB SQLを使用します。エージェントはプロパティグラフのテーブルに対して直接SQLを組み立てます。

> [!note]
> 計画中の`glab orbit setup`サブコマンドにより、OrbitスキルのインストールとそのためのMCP設定の書き込みが自動化される予定です。リリースまでは、以下に示すようにMCPクライアントを手動で設定してください。

## 手動設定：Claude Code {#manual-config-claude-code}

`~/.claude/mcp_servers.json`またはプロジェクトの`.claude/mcp_servers.json`に以下を追加してください。

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

または、`glab`経由で実行する場合：

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

## 手動設定：その他のMCPクライアント {#manual-config-other-mcp-clients}

`orbit mcp serve`（または`glab orbit local mcp serve`）を実行することで、任意のMCPクライアントから接続できます。サーバーはstdio経由でMCPを使用し、`query_graph`と`get_graph_schema`を公開します。

## ツールの使用方法 {#using-the-tools}

接続後、AIエージェントに対してOrbitを直接使用するよう指示します。

スキーマを確認する場合：
> 「`get_graph_schema`を使用して、ローカルグラフ内のノードタイプを表示してください。」

関数の呼び出し元を検索する場合：
> 「Orbitを使用して、`parseConfig`をインポートしているすべてのファイルと、それを呼び出している関数を検索してください。」

モジュールをマップする場合：
> 「Orbitを使用して、`src/auth/`で宣言されているすべての定義とその種類を一覧表示してください。」

エージェントはSQLを組み立て、代わりに`run_sql`を呼び出します。

## ローカルグラフの内容 {#what-s-in-the-local-graph}

Orbit Localはコードのみをインデックス化します。対象は、サポートされている11言語すべてにわたるファイル、ディレクトリ、定義、およびインポートされたシンボルです。SDLCデータ（マージリクエスト、パイプライン、ユーザー、脆弱性）はローカルでは利用できません。これらには[Orbit Remote](../../remote/_index.md)が必要です。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべてのMCPトラフィックはローカルで処理されます。
