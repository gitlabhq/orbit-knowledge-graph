---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit LocalのMCPサーバー（計画中）。現時点では利用できません。
title: MCPを使用してOrbit Localに接続する
---

> [!warning]
> **現時点では利用できません。** Orbit LocalはまだMCPサーバーとして動作できません。`orbit mcp serve`および`glab orbit local mcp serve`コマンドは、Orbit CLIのいかなるリリース済みバージョンにも存在しません。以下の設定例は**計画中**のインターフェースを説明するものであり、現在使用できる機能ではありません。このMCP設定をエージェントに追加しても、サイレントに失敗します。サーバーは起動せず、エージェントは何もクエリできません。
>
> MCPサーバーがリリースされるまでは、以下の[回避策](#workaround-query-from-the-terminal)を使用してください。進捗状況は[マージリクエスト !1377](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1377)で確認できます。

リリース後、Orbit LocalはGitLabインスタンスではなくローカルのDuckDBグラフを参照し、stdio経由でステートレスなMCPサーバーとして動作します。Orbit Remote（JSON形式のクエリDSLを公開）とは異なり、Orbit LocalはDuckDB SQLをそのまま使用します。エージェントはプロパティグラフのテーブルに対して直接SQLを組み立てます。

## 前提条件 {#prerequisites}

- Orbit CLI（`orbit`）がインストールされていること。[Orbit CLIを直接使用する](cli.md)を参照してください。
- ローカルリポジトリのインデックスが作成されていること（`orbit index <path>` または `glab orbit local index <path>`）。

## 回避策: ターミナルからクエリを実行する {#workaround-query-from-the-terminal}

MCPサーバーがリリースされるまでは、`glab orbit local`を使用してターミナルから直接ローカルグラフをクエリしてください。これが現時点でOrbit Localを使用するためのサポートされた方法です。

ローカルのDuckDBグラフに対して読み取り専用のSQLクエリを実行する:

```shell
glab orbit local sql "SELECT name, definition_type FROM gl_definition LIMIT 10"
```

グラフスキーマ（テーブル名、カラム、データ型）を確認する。デフォルトのテーブル表示ではなくJSONで出力するには`--raw`を追加します:

```shell
glab orbit local schema
```

これらのコマンドの出力をコンテキストとしてAIエージェントに貼り付けることができます。また、今すぐ[Orbitスキルを手動でインストール](../../ai_coding_agents.md)することで、エージェントにクエリレシピ、SQLガイダンス、トラブルシューティング情報を提供できます。

## 計画中のインターフェース {#planned-interface}

> [!note]
> このセクションの内容はすべて計画中のMCPサーバーについて説明しています。これらのコマンドや設定ブロックはまだ動作しません。機能を実装するコントリビューター向けの仕様として、また今後の予定を確認できるよう、ここに記載しています。

### 計画中のMCPツール {#planned-mcp-tools}

| ツール | 説明 |
|------|-------------|
| `run_sql` | ローカルのDuckDBグラフに対して読み取り専用のSQLクエリを実行します。JSON形式の行データを返します。 |
| `get_graph_schema` | スキーマ（ローカルのDuckDBに存在するテーブル名、カラム、データ型）をフェッチします。 |
| `index` | リポジトリ（またはリポジトリのディレクトリ）をローカルグラフにインデックス作成します。 |

### 計画中の設定: Claude Code {#planned-config-claude-code}

計画中のインターフェースでは、`~/.claude/mcp_servers.json`またはプロジェクトの`.claude/mcp_servers.json`に以下を追加できるようになります。

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

`glab`経由で実行する場合は、以下を使用します。

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

### 計画中の設定: その他のMCPクライアント {#planned-config-other-mcp-clients}

`orbit mcp serve`（または`glab orbit local mcp serve`）を実行することで、任意のMCPクライアントから接続できるようになります。サーバーはstdio経由でMCPプロトコルを使用し、`run_sql`、`get_graph_schema`、`index`を公開します。

### 計画中の使用方法 {#planned-usage}

接続後、AIエージェントに対してOrbitを直接使用するよう指示します。

スキーマを確認する:
> "`get_graph_schema`を使用して、ローカルグラフに含まれるテーブルを表示してください。"

種類別に定義を検索する:
> "Orbitを使用して、このリポジトリ内の定義を種類別に集計し、最も大きいクラスを10件一覧表示してください。"

モジュールをマップする:
> "Orbitを使用して、`src/auth/`で宣言されているすべての定義とその種類を一覧表示してください。"

## ローカルグラフの内容 {#what-s-in-the-local-graph}

Orbit Localはコードのみをインデックス作成します。対象は、サポートされている11言語すべてにわたるファイル、ディレクトリ、定義、およびインポートされたシンボルです。SDLCデータ（マージリクエスト、パイプライン、ユーザー、脆弱性）はローカルでは利用できません。これらのデータには[Orbit Remote](../../remote/_index.md)が必要です。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべてのローカルトラフィックはマシン上で処理されます。
