---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Localがマシン上でインデックスする内容、サポートされている言語、およびローカルコードグラフの境界について説明します。
title: Orbit Localのインデックス対象
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
> Orbit Localは実験的な機能です。GAリリース前に、機能およびコマンドの形式が変更される場合があります。

Orbit Localは、ローカルリポジトリからコードのみのグラフを構築します。GitLabへの接続は行わず、SDLCデータのインデックス作成も行いません。

## スコープ {#scope}

Orbit Localは、指定した任意のローカルリポジトリのワークツリーをインデックスします。グループ、プロジェクト、ブランチの概念はなく、インデックスは`orbit index`に渡されたディレクトリにスコープされます。

複数のリポジトリを同一のDuckDBファイルにインデックスできます。各リポジトリは絶対パスによって個別に追跡されます。

## ソースコード {#source-code}

Orbit Localがインデックスする対象：

- ファイルおよびディレクトリ（`.gitignore`を考慮）
- 関数、クラス、メソッド、モジュールの定義（開始・終了行番号およびソースコードの全内容を含む）
- インポート宣言およびクロスファイルシンボル参照

インデックス作成は、現在ディスク上に存在するファイルに対して実行されます。デフォルトブランチの概念はなく、チェックアウトされている内容がインデックスの対象となります。

### サポートされている言語 {#supported-languages}

Orbit Localは、Orbit Remoteと同じ11言語をサポートしており、クロスファイル参照の解決にも完全対応しています。

| 言語 | 定義 | クロスファイル参照 |
|----------|-------------|----------------------|
| Ruby | あり | あり |
| Java | あり | あり |
| Kotlin | あり | あり |
| Python | あり | あり |
| TypeScript | あり | あり |
| JavaScript | あり | あり |
| Rust | あり | あり |
| Go | あり | あり |
| C# | あり | あり |
| C | あり | あり |
| C++ | あり | あり |

現在インデックス対象外の言語：Swift、COBOL、Terraform、YAML。

## インデックス対象外の項目 {#what-is-not-indexed}

Orbit LocalはGitLabへの接続を持たないため、以下の情報は利用できません：

- グループ、プロジェクト、またはユーザー
- マージリクエスト、コメント、またはレビュアー
- パイプライン、ジョブ、またはステージ
- 作業アイテム、マイルストーン、またはラベル
- 脆弱性またはセキュリティの検出結果

SDLCを考慮したクエリには、[Orbit Remote](../remote/indexing.md)をご利用ください。

Orbit Localでインデックスされないその他の項目：

- バイナリファイル
- `.gitignore`に一致するファイル
- インデックス作成時にチェックアウトされているブランチ以外のブランチ

## 認可 {#authorization}

Orbit Localには認可レイヤーがありません。グラフ内のすべてのデータは、CLIを実行するユーザーがアクセスできます。`~/.orbit/graph.duckdb`にあるグラフファイルは、オペレーティングシステムのファイルパーミッションによって保護されています。

## 課金 {#billing}

Orbit LocalはGitLab Creditsを消費しません。すべての処理はローカルで実行されます。
