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
- ステータス: ベータ

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

## スキーマを確認する {#inspect-the-schema}

`orbit schema`には`--ontology`または`--query`のいずれかが必要です。

```shell
orbit schema --ontology
orbit schema --query
```

- `--ontology`はグラフオントロジー（エンティティ、エッジ、プロパティ）を表示します。
- `--query`はクエリDSLスキーマ（構造化クエリの記述方法）を表示します。

デフォルトのLLM向け出力ではなくJSONで出力するには、いずれかのオプションに`--raw`を追加します。

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

## ストレージ {#storage}

グラフは`~/.orbit/graph.duckdb`に保存されます。複数のリポジトリが同じデータベースを共有します。最初からやり直すにはファイルを削除してください。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。

## 次のステップ {#what-to-try-next}

- [MCPで接続する](mcp.md) - ローカルグラフをClaude CodeまたはCodexに公開します。
- [glabでOrbit Localを使用する](glab.md) - `glab orbit local`を通じてCLIを呼び出します。
- [スキーマリファレンス](../../remote/schema.md) - 利用可能なノードタイプとプロパティ。
- [Cookbook](../../remote/cookbook.md) - 一般的なユースケース向けのコピー＆ペーストクエリ。
