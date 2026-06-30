---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit CLI（orbit）バイナリを使用して、ローカルコードグラフを構築・クエリします。GitLabアカウントやネットワーク接続は不要です。
title: Orbit CLIでOrbit Localを使用する（`orbit`）
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

Orbit CLI（`orbit`）は、任意のローカルリポジトリのコードグラフを構築し、ローカルのDuckDBファイルに対してクエリを実行します。GitLabへの接続は不要です。

## インストール {#install}

ワンラインインストーラーでスタンドアロンの`orbit`バイナリをインストールします。

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

これにより`orbit`が`PATH`に追加されます。新しいターミナルを開き、インストールを確認します。

```shell
orbit help
```

GitLab CLI（`glab`）をすでに使用している場合は、`glab orbit local --install`でマネージドバイナリをインストールすることもできます。そのバイナリは`orbit`を直接使用するのではなく、`glab orbit local <command>`として実行します。詳細は[glabでOrbit Localを使用する](glab.md)を参照してください。

### ソースからビルドする {#build-from-source}

Orbitにコントリビュートする場合や、未リリースのビルドを実行する場合は、バイナリを自分でコンパイルします。

前提条件:

- [Rustツールチェーン](https://rustup.rs/)（stable）
- ツール管理用の[`mise`](https://mise.jdx.dev/)

```shell
git clone https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
cd knowledge-graph
mise install
mise run build:cli
```

コンパイルされたバイナリは`target/release/orbit`にあります。`PATH`に追加するか、直接実行してください。

## リポジトリをインデックス作成する {#index-a-repository}

```shell
orbit index /path/to/your/repo
```

Orbitはリポジトリを解析し、DuckDBグラフを`~/.orbit/graph.duckdb`に書き込みます。複数のリポジトリをインデックス作成できます。各リポジトリはマニフェストテーブル内でプロジェクトIDとブランチによってスコープが設定されます。

| フラグ | 説明 |
|------|---------|
| `--threads` | ワーカースレッド数。`0`（デフォルト）はCPUコア数から自動検出します。 |
| `--stats` | JSON出力に詳細な統計情報を含めます。 |
| `--verbose` | stderrへの詳細ログを有効にします。 |
| `--db` | DuckDBのパスを上書きします。デフォルトは`~/.orbit/graph.duckdb`です。 |

## スキーマを確認する {#inspect-the-schema}

`orbit schema`はローカルDuckDBグラフのすべてのテーブルとカラムを一覧表示します。

```shell
orbit schema
```

テーブル名を位置引数として渡すと、出力をスコープできます。

```shell
orbit schema gl_definition              # scoped to one table
orbit schema gl_definition gl_edge      # scoped to two tables
```

| フラグ | 説明 |
|------|---------|
| `--raw` | デフォルトのテーブルビューではなくJSONで出力します。 |
| `--db` | DuckDBのパスを上書きします。デフォルトは`~/.orbit/graph.duckdb`です。 |

## ローカルグラフに対してSQLを実行する {#run-sql-against-the-local-graph}

```shell
orbit sql 'SELECT count(*) FROM gl_definition'
orbit sql -F json 'SELECT name FROM gl_definition LIMIT 3'
echo 'SELECT 1+1' | orbit sql -
orbit sql --file query.sql
```

| フラグ | 説明 |
|------|---------|
| `-F`、`--format` | `table`（デフォルト）、`json`、`ndjson`、または`csv`。 |
| `-f`、`--file` | ファイルからSQLを読み込みます。 |
| `--db` | DuckDBのパスを上書きします。デフォルトは`~/.orbit/graph.duckdb`です。 |

## インデックス済みリポジトリを一覧表示する {#list-indexed-repositories}

グラフには複数のリポジトリを保持できます。内容を確認するには次のコマンドを実行します。

```shell
orbit list
orbit list -F json
```

各行にはリポジトリのパス、ブランチ、コミット、インデックス作成ステータス、および最終インデックス作成日時が表示されます。

```plaintext
+------------------------+--------+------------+---------+---------------------+
| repo_path              | branch | commit_sha | status  | last_indexed_at     |
+------------------------+--------+------------+---------+---------------------+
| /home/dev/workspace/kg | main   | 9606ae8... | indexed | 2026-05-18 10:14:02 |
| /tmp/cli-test          | main   | 654f3a6... | indexed | 2026-05-18 10:13:55 |
+------------------------+--------+------------+---------+---------------------+
```

| フラグ | 説明 |
|------|---------|
| `-F`、`--format` | `table`（デフォルト）、`json`、`ndjson`、または`csv`。 |
| `--db` | DuckDBのパスを上書きします。デフォルトは`~/.orbit/graph.duckdb`です。 |

まだ何もインデックス作成されていない場合、`orbit list`は`0`で終了します。テーブルビューには何も表示されず、構造化フォーマットは有効な空の出力（`json`の場合は`[]`、`ndjson`の場合はレコードなし）を返すため、`orbit list -F json | jq`のようなパイプラインも正常に動作します。

## MCPサーバーとして実行する {#run-as-an-mcp-server}

stdioを通じてローカルグラフをMCP対応のAIエージェントに公開します。

```shell
orbit mcp serve
```

`~/.orbit/graph.duckdb`に対して`run_sql`、`get_graph_schema`、`index`を提供します。クライアントごとの設定については[MCPで接続する](mcp.md)を参照してください。

## ストレージ {#storage}

グラフは`~/.orbit/graph.duckdb`に保存されます。複数のリポジトリが同じデータベースを共有します。最初からやり直すにはファイルを削除してください。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。

## 次のステップ {#what-to-try-next}

- [MCPで接続する](mcp.md) - Claude Code、Codex、その他のエージェントをローカルグラフに接続します。
- [glabでOrbit Localを使用する](glab.md) - `glab orbit local`を通じてCLIを呼び出します。
- [スキーマリファレンス](../../remote/schema.md) - 利用可能なノードタイプとプロパティ。
- [Cookbook](../../remote/cookbook.md) - 一般的なユースケース向けのコピー＆ペーストクエリ。
