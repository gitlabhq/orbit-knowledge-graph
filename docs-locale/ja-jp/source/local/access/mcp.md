---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Claude Code、Codex、OpenCode、またはMCP対応のAIエージェントをローカルのOrbitグラフに接続します。
title: MCPを使用してOrbit Localに接続する
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: 実験的機能

{{< /details >}}

{{< history >}}

- GitLab 19.2で[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/643)。

{{< /history >}}

Orbit Localは、GitLabインスタンスではなくローカルのDuckDBグラフを参照し、stdio経由でステートレスなMCPサーバーとして動作します。Orbit Remote（JSON形式のクエリDSLを公開）とは異なり、Orbit LocalはDuckDB SQLをそのまま使用します。エージェントはプロパティグラフのテーブルに対して直接SQLを組み立てます。

> [!note]
> MCPサーバーは実験的機能です。GAリリース前に、機能や設定の形式が変更される場合があります。

## 前提条件 {#prerequisites}

- Orbit CLI（`orbit`）がインストールされていること。[Orbit CLIを直接使用する](cli.md)を参照してください。
- ローカルリポジトリのインデックスが作成されていること（`orbit index <path>` または `glab orbit local index <path>`）。エージェントは`index` MCPツールを通じてインデックスを作成することもできます。

## MCPツール {#mcp-tools}

| ツール | 説明 |
|------|-------------|
| `run_sql` | ローカルのDuckDBグラフに対して読み取り専用のSQLクエリを実行します。ステートメントの配列を受け取り、同じインデックス位置にあるステートメントごとに1つのJSON行配列を返します。 |
| `get_graph_schema` | スキーマ（ローカルのDuckDBに存在するテーブル名、カラム、データ型）をフェッチします。 |
| `index` | リポジトリ（またはリポジトリのディレクトリ）をローカルグラフにインデックス作成します。 |

サーバーはステートレスです。ツールを呼び出すたびにDuckDBファイルをオンデマンドで開き、返却前に解放するため、複数のエディタが同じグラフに対してそれぞれ1つのサーバープロセスを実行できます。

`run_sql`の結果が大きすぎる場合（Arrowデータ約1 MB）、シリアライズ前に拒否され、`LIMIT`を追加するか射影を絞り込むよう求めるエラーが返されます。これにより、`SELECT *`の暴走でエディタがフリーズすることを防ぎます。

## Claude Codeに接続する {#connect-claude-code}

```shell
claude mcp add orbit-local -- orbit mcp serve
```

またはプロジェクトの`.mcp.json`に以下を追加します。

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

## Codexに接続する {#connect-codex}

```shell
codex mcp add orbit-local -- orbit mcp serve
```

## OpenCodeに接続する {#connect-opencode}

`opencode.json`（プロジェクトまたはグローバル）に以下を追加します。

```json
{
  "mcp": {
    "orbit-local": {
      "type": "local",
      "command": ["orbit", "mcp", "serve"],
      "enabled": true
    }
  }
}
```

## その他のMCPクライアントに接続する {#connect-other-mcp-clients}

`orbit mcp serve`（または`glab orbit local mcp serve`）をstdioサーバーとして実行することで、任意のMCPクライアントから接続できます。Cursorの場合は、上記の`.mcp.json`ブロックを`.cursor/mcp.json`に使用してください。

## ツールの使用方法 {#using-the-tools}

接続後、AIエージェントに対してOrbitを直接使用するよう指示します。

スキーマを確認する:
> "`get_graph_schema`を使用して、ローカルグラフに含まれるテーブルを表示してください。"

種類別に定義を検索する:
> "Orbitを使用して、このリポジトリ内の定義を種類別に集計し、最も大きいクラスを10件一覧表示してください。"

モジュールをマップする:
> "Orbitを使用して、`src/auth/`で宣言されているすべての定義とその種類を一覧表示してください。"

`_orbit_manifest`テーブルにはインデックス作成済みのリポジトリが一覧表示されているため、「ローカルグラフにどのリポジトリがあるか」は`run_sql`を1回呼び出すだけで確認できます。

## ローカルグラフの内容 {#what-s-in-the-local-graph}

Orbit Localはコードのみをインデックス作成します。対象は、サポートされている11言語すべてにわたるファイル、ディレクトリ、定義、およびインポートされたシンボルです。SDLCデータ（マージリクエスト、パイプライン、ユーザー、脆弱性）はローカルでは利用できません。これらのデータには[Orbit Remote](../../remote/_index.md)が必要です。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべてのローカルトラフィックはマシン上で処理されます。
