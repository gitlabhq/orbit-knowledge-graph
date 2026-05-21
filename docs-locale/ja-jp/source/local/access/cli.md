---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit CLI（orbit）バイナリを使用して、ローカルコードグラフをビルドおよびクエリします。GitLabアカウントやネットワーク接続は不要です。
title: Orbit CLIでOrbit Localを使用する（`orbit`）
---

{{< details >}}

- ティア: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

Orbit CLI（`orbit`）は、任意のローカルリポジトリのコードグラフをビルドし、
ローカルのDuckDBファイルに対してクエリを実行します。GitLab接続は不要です。

> [!note]
> Orbit Localは実験的な機能です。パッケージ化されたバイナリが提供されるまでは、
> ソースからビルドする必要があります。パッケージ化されたインストールパスは`glab orbit local`になる予定です。

## 前提条件 {#prerequisites}

- [Rustツールチェーン](https://rustup.rs/)（stable）
- ツール管理用の[`mise`](https://mise.jdx.dev/)
- インデックスを作成するローカルのGitリポジトリ

## インストール {#install}

ソースからビルドします:

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

コンパイルされたバイナリは`target/release/orbit`にあります。`PATH`に追加するか、
直接実行してください。

## リポジトリのインデックスを作成する {#index-a-repository}

```shell
orbit index /path/to/your/repo
```

Orbitはリポジトリを解析し、DuckDBグラフを`~/.orbit/graph.duckdb`に書き込みます。
複数のリポジトリのインデックスを作成できます。各リポジトリは、マニフェストテーブル内でプロジェクトIDとブランチによってスコープが設定されます。

| フラグ | 目的 |
|------|---------|
| `--threads` | ワーカースレッド数。`0`（デフォルト）はCPUコア数から自動検出します。 |
| `--stats` | JSON出力に詳細な統計情報を含めます。 |
| `--verbose` | stderrへの詳細ログ出力。 |

## スキーマを確認する {#inspect-the-schema}

```shell
orbit schema
orbit schema --raw
```

`orbit schema`はローカルのDuckDBから`information_schema.columns`を読み取り、
すべてのテーブルとカラムを出力します。JSON出力には`--raw`を渡してください。

## ローカルグラフに対してSQLを実行する {#run-sql-against-the-local-graph}

```shell
orbit sql 'SELECT count(*) FROM gl_definition'
orbit sql -F json 'SELECT name FROM gl_definition LIMIT 3'
echo 'SELECT 1+1' | orbit sql -
orbit sql --file query.sql
```

| フラグ | 目的 |
|------|---------|
| `-F`、`--format` | `table`（デフォルト）、`json`、`ndjson`、または`csv`。 |
| `-f`、`--file` | ファイルからSQLを読み取ります。 |
| `--db` | DuckDBのパスを上書きします。デフォルトは`~/.orbit/graph.duckdb`です。 |

## ストレージ {#storage}

グラフは`~/.orbit/graph.duckdb`に保存されます。複数のリポジトリが同じデータベースを共有します。最初からやり直すにはファイルを削除してください。

## 課金 {#billing}

Orbit LocalはGitLab Creditsを消費しません。すべての処理はローカルで行われます。

## 次に試すこと {#what-to-try-next}

- [MCPで接続する](mcp.md) - ローカルグラフをClaude CodeまたはCodexに公開します。
- [glabでOrbit Localを使用する](glab.md) - `glab orbit local`を通じてCLIを呼び出します。
- [スキーマリファレンス](../../remote/schema.md) - 利用可能なノードタイプとプロパティ。
- [Cookbook](../../remote/cookbook.md) - 一般的なユースケース向けのコピー＆ペーストクエリ。
