---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Localがマシン上でインデックス作成する対象、サポートされている言語、およびローカルコードグラフの範囲について説明します。
title: Orbit Localのインデックス作成対象
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: ベータ

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)されました。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)されました。

{{< /history >}}

> [!note]
> Orbit Localは実験的機能です。GAリリース前に、機能およびコマンドの形式が変更される場合があります。

Orbit Localはローカルリポジトリからコードのみのグラフを構築します。GitLabへの接続は行わず、SDLCデータのインデックス作成も行いません。

## スコープ {#scope}

Orbit Localは、指定した任意のローカルリポジトリのワークツリーをインデックス作成します。
グループ、プロジェクト、ブランチの概念はなく、インデックスは`orbit index`に渡されたディレクトリにスコープされます。

複数のリポジトリを同一のDuckDBファイルにインデックス作成できます。各リポジトリは絶対パスによって個別に追跡されます。

## ソースコード {#source-code}

Orbit Localがインデックス作成する対象:

- ファイルおよびディレクトリ（`.gitignore`を考慮）
- 関数、クラス、メソッド、モジュールの定義（開始・終了行番号およびソースコード全体を含む）
- インポート宣言およびクロスファイルシンボル参照

インデックス作成は、現在ディスク上に存在するファイルに対して実行されます。デフォルトブランチの概念はなく、チェックアウトされている内容がインデックス作成の対象となります。

### サポートされている言語 {#supported-languages}

Orbit LocalはOrbit Remoteと同じ11言語をサポートしており、クロスファイル参照の解決にも完全対応しています。

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

現在インデックス作成の対象外となっている言語: Swift、COBOL、Terraform、YAML。

## インデックス作成されない対象 {#what-is-not-indexed}

Orbit LocalはGitLabに接続しないため、以下の情報は利用できません:

- グループ、プロジェクト、またはユーザー
- マージリクエスト、コメント、またはレビュアー
- パイプライン、ジョブ、またはステージ
- 作業アイテム、マイルストーン、またはラベル
- 脆弱性またはセキュリティの検出結果

SDLCを考慮したクエリには、[Orbit Remote](../remote/indexing.md)を使用してください。

また、Orbit Localでインデックス作成されない対象:

- バイナリファイル
- `.gitignore`に一致するファイル
- インデックス作成時にチェックアウトされているブランチ以外のブランチ

## 認可 {#authorization}

Orbit Localには認可レイヤーがありません。グラフ内のすべてのデータは、CLIを実行するユーザーがアクセスできます。`~/.orbit/graph.duckdb`にあるグラフファイルは、オペレーティングシステムのファイルパーミッションによって保護されています。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。
