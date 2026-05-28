---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit LocalがOrbit CLIとDuckDBを使用してマシン上でコードグラフを構築およびクエリする方法。
title: Orbit Localの仕組み
---

{{< details >}}

- 階層: Free, Premium, Ultimate
- 提供形態: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

> [!note]
> Orbit Localは実験的な機能です。GAリリース前に、機能およびコマンドの形式が
> 変更される場合があります。

## インデックス作成パイプライン {#indexing-pipeline}

`orbit index`を実行すると、Orbit Localは以下の処理を行います。

1. `.gitignore`を考慮しながら、リポジトリのディレクトリツリーを走査します。
1. 各ソースファイルを言語固有のパーサー（rust-analyzer、tree-sitter、または言語に応じたカスタムパーサー）に渡します。
1. 定義（関数、クラス、モジュール）、インポート宣言、およびクロスファイルシンボル参照を抽出します。
1. 結果をノードとエッジとして`~/.orbit/graph.duckdb`のローカルDuckDBファイルに書き込みます。

v2パイプラインはすべての言語パーサーを並列で実行します。中規模のリポジトリのインデックス作成は、通常数秒で完了します。

## グラフモデル {#the-graph-model}

Orbit Localはコードのみのグラフを構築します。GitLabとの接続がないため、SDLCデータ（マージリクエスト、パイプライン、ユーザー）にはアクセスできません。

ローカルグラフのノード:

- **File** - リポジトリ内のソースファイル
- **Directory** - リポジトリ内のディレクトリ
- **Definition** - 関数、クラス、モジュール、またはその他の名前付きシンボル
- **ImportedSymbol** - 別のファイルまたはパッケージからインポートされたシンボル

エッジは、ファイルとその定義、ファイルとそのインポート、そしてファイルをまたいで参照するシンボルと定義を結びつけます。

## クエリの実行 {#query-execution}

Orbit LocalはグラフをDuckDBデータベースとして公開します。`orbit sql`を使用して、読み取り専用のSQLを実行できます。

1. `orbit sql`は`~/.orbit/graph.duckdb`を読み取り専用で開きます。
1. SQLはグラフテーブルに対して直接実行されます。DSLのコンパイルや認可レイヤーは不要です。
1. 結果はテーブル、JSON、NDJSON、またはCSV形式でストリーミングされます。

グラフ内のすべてのデータは、CLIを実行するユーザーがアクセスできます。

## ストレージ {#storage}

グラフは`~/.orbit/graph.duckdb`の単一のDuckDBファイルに保存されます。複数のリポジトリが同じデータベースを共有し、各リポジトリはマニフェストテーブル内のプロジェクトIDとブランチによってスコープが設定されます。

## サポートされている言語 {#supported-languages}

Orbit Remoteがサポートする11の言語はすべてローカルでもサポートされています:
Ruby、Java、Kotlin、Python、TypeScript、JavaScript、Rust、Go、C#、C、C++。

完全な言語サポート表については、[Orbitがインデックスする対象](../remote/indexing.md#supported-languages)を参照してください。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。
