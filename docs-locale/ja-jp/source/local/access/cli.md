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

`npm install -g @gitlab/orbit`でnpmからインストールすることもできます。

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

各行にはリポジトリのパス、ブランチ、コミット、インデックス作成ステータス、最終インデックス作成日時、およびステータスが`error`の場合はエラーメッセージが表示されます。

```plaintext
+------------------------+--------+------------+---------+---------------------+---------------+
| repo_path              | branch | commit_sha | status  | last_indexed_at     | error_message |
+------------------------+--------+------------+---------+---------------------+---------------+
| /home/dev/workspace/kg | main   | 9606ae8... | indexed | 2026-05-18 10:14:02 |               |
| /tmp/cli-test          | main   | 654f3a6... | indexed | 2026-05-18 10:13:55 |               |
+------------------------+--------+------------+---------+---------------------+---------------+
```

インデックス作成に失敗したリポジトリは`status = error`と`error_message`に理由が記録されます。これにより、失敗したリポジトリや処理できないリポジトリが一覧から消えることなく確認できます。

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

## AIアシスタントをセットアップする {#set-up-your-ai-assistant}

`orbit setup`は、AIコーディングアシスタントがgrepを使用する前にグラフを参照するよう設定します。設定するアシスタントの名前を指定します。

```shell
orbit setup claude
```

サポートされているアシスタントは`claude`、`codex`、`opencode`、`pi`です。デフォルトでは、ガイダンスはリモートのGitLab Orbitグラフを参照します。ローカルグラフを参照するよう変更するには、`--local`を渡します。

```shell
orbit setup claude --local
```

### 変更内容 {#what-it-changes}

このコマンドはユーザー自身のファイルを変更します。単独では実行されず、ユーザーが呼び出したときのみ実行されます。

指定したアシスタントごとに、`orbit setup`は以下を実行します。

- `CLAUDE.md`や`AGENTS.md`などのアシスタントの指示ファイルにブロックを追加します。ブロックは`<!-- orbit:setup:begin -->`と`<!-- orbit:setup:end -->`マーカーの間に配置され、マーカーの外側はそのまま保持されます。コマンドを再度実行すると、2つ目のコピーを追加するのではなく、既存のブロックが置き換えられます。
- アシスタントがサポートしている場合、アシスタントのJSON設定にエントリを追加します。Claude Codeの場合は`settings.json`内の`PreToolUse`フック、OpenCodeの場合はプラグインファイルとその登録です。エントリには`orbit`マーカーが付与され、マーカー付きのエントリのみが置き換えまたは削除されます。

デフォルトでは`~/.claude/CLAUDE.md`などのユーザーグローバル設定に書き込みます。現在のプロジェクトに書き込むには`--project`を渡し、特定のプロジェクトディレクトリを対象にするには`--dir <path>`を渡します。

`orbit setup`が既存のファイルを初めて変更する前に、元のファイルを同じ場所に`<name>.orbit-backup`としてコピーします。元の状態に戻したい場合は、そのコピーを手動で復元してください。既存のバックアップは上書きされないため、コピーには常に`orbit`適用前のバージョンが保持されます。

`--project`の使用には注意が必要です。プロジェクトの指示ファイルは通常バージョン管理にコミットされるため、変更が`git status`に表示され、チームメンバーに影響する可能性があります。デフォルトのユーザーグローバルスコープはユーザー自身にのみ影響します。

### 設定を削除する {#remove-it}

変更を元に戻すには、次のコマンドを実行します。

```shell
orbit setup claude --remove
```

これにより、マーカーで区切られたブロックとマーカー付きのJSONエントリが削除され、各ファイルの残りの部分はそのまま保持されます。ファイルに`orbit`エントリのみが含まれていた場合、そのファイルは削除されます。アシスタント名を省略すると、すべてのアシスタントのセットアップが削除されます。バックアップファイルは削除されません。

`orbit setup`によるファイルの変更を避けたい場合は、このコマンドをスキップし、同じ指示ブロックとフックを手動で追加してください。

## ストレージ {#storage}

グラフは`~/.orbit/graph.duckdb`に保存されます。複数のリポジトリが同じデータベースを共有します。最初からやり直すにはファイルを削除してください。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。

## 次のステップ {#what-to-try-next}

- [MCPで接続する](mcp.md) - Claude Code、Codex、その他のエージェントをローカルグラフに接続します。
- [glabでOrbit Localを使用する](glab.md) - `glab orbit local`を通じてCLIを呼び出します。
- [スキーマリファレンス](../../remote/schema.md) - 利用可能なノードタイプとプロパティ。
- [Cookbook](../../remote/cookbook.md) - 一般的なユースケース向けのコピー＆ペーストクエリ。
