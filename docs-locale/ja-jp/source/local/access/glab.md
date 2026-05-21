---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: glab orbit localサブコマンドとglab orbit setupは、将来のglabリリースで提供予定です。リリースまでは、ソースからビルドしてorbitバイナリを直接使用してください。
title: GitLab CLI（`glab`）でOrbit Localを使用する
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

> [!disclaimer]

[GitLab CLI（`glab`）](https://docs.gitlab.com/cli/)は、Orbit Localのインストール、実行、およびAIエージェントとの統合に推奨される標準的な方法です。`glab orbit local`は`glab orbit remote`をミラーしているため、GitLabインスタンスまたはローカルマシンのどちらをクエリする場合でも、同じパターンが使用できます。

> [!note]
> `glab orbit local`と`glab orbit setup`はいずれも将来のglabリリースで提供予定であり、現時点では利用できません。このページに記載されているすべてのコマンドは将来の形であり、現在の形ではありません。リリースまでは、ソースからビルドしてください。[`orbit`を直接使用する](cli.md)を参照してください。

トップレベルコマンド（いずれも提供予定で、まだリリースされていません）：

- `glab orbit setup`：Orbitスキルをインストールし、AIエージェントをローカルグラフに向けます。
- `glab orbit local`：`orbit`バイナリをラップする型付きサブコマンド。Orbit LocalをMCPサーバーとして実行する`glab orbit local mcp serve`を含みます。

## 前提条件 {#prerequisites}

- `glab`がインストールされ、認証されていること：

  ```shell
  glab auth login
  ```

- インデックスを作成するローカルGitリポジトリ。

バイナリがインストールされていれば、`glab orbit local`の使用にGitLabアカウントやネットワーク接続は不要です。

## AIエージェントのセットアップ {#set-up-your-ai-agent}

> [!note]
> `glab orbit setup`は提供予定であり、まだリリースされていません。リリースまでは、[MCPクライアントを手動で設定してください](mcp.md#manual-config-claude-code)。

リリース後、`glab orbit setup`は1つのコマンドでOrbitスキルをインストールし、MCP設定を書き込みます。**ローカル**または**リモート**を選択するプロンプトが表示され、エージェントを自動検出します。

```shell
glab orbit setup
# MCPの設定をローカルグラフに向けるには、プロンプトで「Local」を選択してください。
```

対応エージェント：Claude Code、OpenCode、Cursor、Codex、Gemini CLI。

| フラグ | 目的 |
|------|---------|
| `--agent=<name>` | 自動検出をオーバーライドします。 |
| `--skill-only` | スキルファイルのみをインストールし、MCP設定をスキップします。 |
| `--mcp-only` | MCP設定のみを書き込み、スキルのインストールをスキップします。 |
| `--dry-run` | 何も書き込まずに変更内容を出力します。 |

MCP設定はリモートエンドポイントの代わりに`orbit mcp serve`を指します。エージェントはローカルのDuckDBグラフに対して`query_graph`と`get_graph_schema`を呼び出すことができます。

## リポジトリのインデックス作成 {#index-a-repository}

```shell
glab orbit local index /path/to/your/repo
```

| フラグ | 目的 |
|------|---------|
| `--threads` | ワーカースレッド数。`0`（デフォルト）はCPUコア数から自動検出します。 |
| `--stats` | JSON出力に詳細な統計情報を含めます。 |
| `--verbose` | stderrへの詳細ログ出力。 |

## グラフに対するSQLの実行 {#run-sql-against-the-graph}

```shell
glab orbit local sql 'SELECT count(*) FROM gl_definition'
echo 'SELECT name FROM gl_definition LIMIT 3' | glab orbit local sql -
```

## スキーマの確認 {#inspect-the-schema}

```shell
glab orbit local schema
glab orbit local schema --raw
```

## MCPサーバーとして実行 {#run-as-an-mcp-server}

ローカルグラフをMCP対応のAIエージェントに公開します：

```shell
glab orbit local mcp serve
```

これにより、`~/.orbit/graph.duckdb`に対してMCPプロトコル経由で`query_graph`と`get_graph_schema`が提供されます。完全なエージェント統合ガイドについては、[MCPで接続する](mcp.md)を参照してください。

## インデックス済みリポジトリの一覧表示 {#list-indexed-repositories}

```shell
glab orbit local status
```

ローカルグラフに存在するリポジトリ、そのインデックス作成状態、およびデータベースパスを表示します。

## 終了コード {#exit-codes}

`glab orbit local`はエラーを安定した終了コードにマップするため、スクリプトやエージェントが分岐処理を行えます。

| ステータス | 終了コード | 意味 |
|--------|-----------|---------|
| 成功 | `0` | コマンドが完了しました。 |
| グラフなし | `2` | `~/.orbit/graph.duckdb`が見つかりません。先に`index`を実行してください。 |
| 不正なクエリ | `4` | クエリDSLの検証またはコンパイルに失敗しました。 |
| その他 | `1` | 非構造化エラー。詳細はstderrを参照してください。 |

## 課金 {#billing}

Orbit LocalはGitLab Creditsを消費しません。すべての処理はローカルで行われます。
