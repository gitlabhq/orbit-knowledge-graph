I'll follow the translation workflow systematically.

## Step 1: Check for existing translations

The target file would be at `docs/source/local/how-it-works.md` in the ja-jp locale. Since this appears to be a new translation request with no existing file mentioned, I'll proceed.

## Step 2: Query Translation Memory

Loading the specialist roster and checking git history for specialist edits on this file. Since this appears to be a new file with no prior ja-jp version, I'll proceed to resource loading.

## Step 3: Load Resources

Loading ja-jp termbase and translation rules as specified.

## Step 4: Translate

Applying ja-jp termbase, translation rules, markdown parsing rules, and website translation rules.

Key decisions:
- `Orbit Local`, `Orbit Remote`, `orbit index`, `orbit sql`, `DuckDB`, `rust-analyzer`, `tree-sitter` → preserve verbatim (product/tool names, commands)
- `~/.orbit/graph.duckdb` → preserve verbatim (file path)
- `.gitignore` → preserve verbatim (filename)
- `SDLC` → preserve verbatim (technical acronym)
- `NDJSON`, `CSV`, `JSON` → preserve verbatim
- Tier/Offering/Status values: `Free`, `Premium`, `Ultimate` per termbase; `GitLab.com`, `GitLab Self-Managed`, `GitLab Dedicated` per termbase
- `experiment` → 実験 per termbase
- `merge requests`, `pipelines` → マージリクエスト、パイプライン per termbase
- `repository` → リポジトリ per termbase
- `branch` → ブランチ per termbase
- `module` → モジュール per termbase
- `import` → インポート per termbase
- `indexing` → インデックス作成 per termbase
- `pipeline` → パイプライン per termbase
- `query`/`queries` → クエリ/クエリする per termbase
- `read-only` → 読み取り専用 per termbase
- `language` → 言語 per termbase
- `processing` → 処理 per termbase
- Heading anchors: preserve English anchors from source
- History block: translate display text, preserve URLs

---

```markdown
---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit LocalがOrbit CLIとDuckDBを使用してマシン上でコードグラフを構築およびクエリする方法。
title: Orbit Localの仕組み
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: Experiment

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

> [!note]
> Orbit Localは実験的な機能です。GAリリース前に、機能および
> コマンドの形式が変更される場合があります。

## インデックス作成パイプライン {#indexing-pipeline}

`orbit index`を実行すると、Orbit Localは次の処理を行います。

1. `.gitignore`を考慮しながら、リポジトリのディレクトリツリーを走査します。
1. 各ソースファイルを言語固有のパーサー（rust-analyzer、tree-sitter、または言語に応じたカスタムパーサー）に渡します。
1. 定義（関数、クラス、モジュール）、インポート宣言、およびクロスファイルシンボル参照を抽出します。
1. 結果をノードとエッジとして`~/.orbit/graph.duckdb`のローカルDuckDBファイルに書き込みます。

v2パイプラインはすべての言語パーサーを並列で実行します。中規模のリポジトリのインデックス作成は、通常数秒で完了します。

## グラフモデル {#the-graph-model}

Orbit Localはコードのみのグラフを構築します。GitLabへの接続がないため、SDLCデータ（マージリクエスト、パイプライン、ユーザー）にはアクセスできません。

ローカルグラフのノード:

- **File** - リポジトリ内のソースファイル
- **Directory** - リポジトリ内のディレクトリ
- **Definition** - 関数、クラス、モジュール、またはその他の名前付きシンボル
- **ImportedSymbol** - 別のファイルまたはパッケージからインポートされたシンボル

エッジは、ファイルとその定義、ファイルとそのインポート、および定義とファイル間で参照するシンボルを接続します。

## クエリの実行 {#query-execution}

Orbit LocalはグラフをDuckDBデータベースとして公開します。`orbit sql`を使用して、読み取り専用のSQLを実行できます。

1. `orbit sql`は`~/.orbit/graph.duckdb`を読み取り専用で開きます。
1. SQLはグラフテーブルに対して直接実行されます。DSLのコンパイルや認可レイヤーは不要です。
1. 結果はテーブル、JSON、NDJSON、またはCSV形式でストリーミングされます。

グラフ内のすべてのデータは、CLIを実行するユーザーがアクセスできます。

## ストレージ {#storage}

グラフは`~/.orbit/graph.duckdb`の単一のDuckDBファイルに保存されます。複数のリポジトリが同じデータベースを共有します。各リポジトリは、マニフェストテーブル内のプロジェクトIDとブランチによってスコープが設定されます。

## サポートされている言語 {#supported-languages}

Orbit Remoteでサポートされている11の言語はすべてローカルでもサポートされています:
Ruby、Java、Kotlin、Python、TypeScript、JavaScript、Rust、Go、C#、C、C++。

完全な言語サポート表については、[Orbitがインデックスするもの](../remote/indexing.md#supported-languages)を参照してください。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。
```
