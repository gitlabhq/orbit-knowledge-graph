---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab CLIのglab orbit localとglab orbit setupを使用して、Orbit Localのインストール、インデックス作成、クエリを実行します。
title: GitLab CLI（`glab`）でOrbit Localを使用する
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

> [!disclaimer]

[GitLab CLI（`glab`）](https://docs.gitlab.com/cli/)は、Orbit Localのインストール、実行、AIエージェントとの統合に推奨される標準的な方法です。`glab orbit local`は`glab orbit remote`と同じ構造を持つため、GitLabインスタンスへのクエリとローカルマシンへのクエリで同じパターンが使用できます。

> [!note]
> `glab orbit local`と`glab orbit setup`は、`glab` 1.94以降で現在利用可能です。

トップレベルのコマンドは2つあります。

- `glab orbit local`: 管理された`orbit`バイナリをラップし、ローカルグラフのインデックス作成とクエリを実行します。
- `glab orbit setup`: アクセスの確認、Orbitスキルのインストール、ローカルバイナリのインストールをガイド付きで行うオンボーディングコマンドです。

## 前提条件 {#prerequisites}

- `glab` 1.94以降がインストールされていること。
- インデックス作成対象のローカルGitリポジトリがあること。

バイナリのインストール後は、`glab orbit local`の使用にGitLabアカウントやネットワーク接続は不要です。

## インストール {#install}

管理された`orbit`バイナリをインストールします。

```shell
glab orbit local --install
```

`glab`がバイナリをダウンロードし、チェックサムを検証して、最新の状態に保ちます。
インストールを確認するには、次のコマンドを実行します。

```shell
glab orbit local help
```

## AIエージェントをセットアップする {#set-up-your-ai-agent}

`glab orbit setup`はガイド付きオンボーディングを実行します。Orbitへの接続確認、AIコーディングエージェントが検出できるようにするOrbitスキルのインストール、ローカル`orbit`バイナリのインストールを行います。

```shell
glab orbit setup
```

| フラグ | 説明 |
|------|---------|
| `--yes` | すべてのプロンプトを承認します（非インタラクティブモード）。 |
| `--global` | 現在のリポジトリではなく、ユーザースコープ（`~/.agents/skills/`）にスキルをインストールします。 |
| `--path` | 指定したディレクトリにスキルをインストールします。 |
| `--skip-skill` | スキルのインストール手順をスキップします。 |
| `--skip-local` | ローカルバイナリのインストール手順をスキップします。 |
| `--upgrade` | スキルを再取得し、バイナリをその場で更新します。 |

スキルは`orbit`バイナリを直接駆動します。MCPクライアントをローカルグラフに接続する場合は、[MCPを使用してOrbitにアクセスする](mcp.md)を参照してください。

`glab skills install --global orbit`を使用して、[Orbitスキルを手動でインストールする](../../ai_coding_agents.md)こともできます。

## リポジトリのインデックスを作成する {#index-a-repository}

```shell
glab orbit local index /path/to/your/repo
```

| フラグ | 説明 |
|------|---------|
| `--threads` | ワーカースレッド数。`0`（デフォルト）はCPUコア数から自動検出します。 |
| `--stats` | JSON出力に詳細な統計情報を含めます。 |
| `--verbose` | stderrへの詳細ログを有効にします。 |

## グラフに対してSQLを実行する {#run-sql-against-the-graph}

```shell
glab orbit local sql 'SELECT count(*) FROM gl_definition'
echo 'SELECT name FROM gl_definition LIMIT 3' | glab orbit local sql -
```

## スキーマを確認する {#inspect-the-schema}

`glab orbit local schema`は、ローカルDuckDBグラフ内のすべてのテーブルとカラムを一覧表示します。

```shell
glab orbit local schema
```

テーブル名を位置引数として渡すと、出力を絞り込めます。

```shell
glab orbit local schema gl_definition              # scoped to one table
glab orbit local schema gl_definition gl_edge      # scoped to two tables
```

| フラグ | 説明 |
|------|---------|
| `--raw` | デフォルトのテーブル表示ではなくJSON形式で出力します。 |
| `--db` | DuckDBのパスを上書きします。デフォルトは`~/.orbit/graph.duckdb`です。 |

## MCPサーバーとして実行する {#run-as-an-mcp-server}

ローカルグラフをMCP対応のAIエージェントに公開します。

```shell
glab orbit local mcp serve
```

MCPプロトコルを通じて`~/.orbit/graph.duckdb`に対して`run_sql`、`get_graph_schema`、`index`を提供します。エージェント統合の詳細については、[MCPを使用してOrbitにアクセスする](mcp.md)を参照してください。

## 終了コード {#exit-codes}

`glab orbit local`は成功時に`0`を返し、失敗時にはゼロ以外の終了コードをstderrの詳細とともに返します。スクリプトやエージェントは成功または失敗に応じて処理を分岐できます。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。
