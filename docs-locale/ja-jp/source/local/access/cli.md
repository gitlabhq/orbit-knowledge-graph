---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit CLI（orbit）バイナリを使用して、ローカルのコードグラフを構築・クエリします。GitLabアカウントやネットワーク接続は不要です。
title: Orbit CLIでOrbit Localを使用する（`orbit`）
---

{{< details >}}

- 階層: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)されました。

{{< /history >}}

Orbit CLI（`orbit`）は、任意のローカルリポジトリのコードグラフを構築し、
ローカルのDuckDBファイルに対してクエリを実行します。GitLabへの接続は不要です。

> [!note]
> Orbit Localは実験的な機能です。パッケージ化されたバイナリが提供されるまでは、
> ソースからビルドする必要があります。パッケージ化されたインストールパスは`glab orbit local`になる予定です。

## 前提条件 {#prerequisites}

- [Rustツールチェーン](https://rustup.rs/)（stable）
- ツール管理用の[`mise`](https://mise.jdx.dev/)
- インデックス対象のローカルGitリポジトリ

## インストール {#install}

ソースからビルドします。

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

コンパイルされたバイナリは`target/release/orbit`に生成されます。`PATH`に追加するか、
直接実行してください。

## リポジトリのインデックス作成 {#index-a-repository}

```shell
orbit index /path/to/your/repo
```

Orbitはリポジトリを解析し、DuckDBグラフを`~/.orbit/graph.duckdb`に書き込みます。
複数のリポジトリをインデックス化できます。各リポジトリは、マニフェストテーブル内でプロジェクトIDとブランチによってスコープが設定されます。

| フラグ | 用途 |
|------|---------|
| `--threads` | ワーカースレッド数。`0`（デフォルト）はCPUコア数から自動検出します。 |
| `--stats` | JSON出力に詳細な統計情報を含めます。 |
| `--verbose` | stderrへの詳細ログを有効にします。 |

## スキーマの確認 {#inspect-the-schema}

```shell
orbit schema
orbit schema --raw
```

`orbit schema`はローカルのDuckDBから`information_schema.columns`を読み取り、
すべてのテーブルとカラムを出力します。JSON出力には`--raw`を指定してください。

## ローカルグラフに対するSQLの実行 {#run-sql-against-the-local-graph}

```shell
orbit sql 'SELECT count(*) FROM gl_definition'
orbit sql -F json 'SELECT name FROM gl_definition LIMIT 3'
echo 'SELECT 1+1' | orbit sql -
orbit sql --file query.sql
```

| フラグ | 用途 |
|------|---------|
| `-F`、`--format` | `table`（デフォルト）、`json`、`ndjson`、または`csv`。 |
| `-f`、`--file` | ファイルからSQLを読み込みます。 |
| `--db` | DuckDBのパスを上書きします。デフォルトは`~/.orbit/graph.duckdb`です。 |

## ストレージ {#storage}

グラフは`~/.orbit/graph.duckdb`に保存されます。複数のリポジトリが同じデータベースを共有します。最初からやり直す場合はファイルを削除してください。

## 課金 {#billing}

Orbit LocalはGitLab Creditsを消費しません。すべての処理はローカルで行われます。

## 次のステップ {#what-to-try-next}

- [MCPで接続する](mcp.md) - ローカルグラフをClaude CodeまたはCodexに公開します。
- [glabでOrbit Localを使用する](glab.md) - `glab orbit local`を通じてCLIを呼び出します。
- [スキーマリファレンス](../../remote/schema.md) - 利用可能なノードタイプとプロパティ。
- [Cookbook](../../remote/cookbook.md) - 一般的なユースケース向けのコピー＆ペーストクエリ。
