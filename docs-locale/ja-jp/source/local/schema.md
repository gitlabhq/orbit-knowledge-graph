---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Localコードグラフの4つのノードタイプとその接続方法に関するリファレンス。
title: スキーマリファレンス
---

{{< details >}}

- 階層: Free, Premium, Ultimate
- 提供形態: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)されました。

{{< /history >}}

> [!note]
> Orbit Localは実験的な機能です。GAリリース前に、機能およびコマンドの形式が
> 変更される場合があります。

Orbit Localは4つのノードタイプをインデックス化します。これらはすべてソースコードドメインに属します。Orbit LocalはGitLabに接続しないため、SDLCレイヤーは存在しません。

ライブのDuckDBスキーマをいつでも確認するには、次のコマンドを実行します。

```shell
orbit schema
```

## ソースコード {#source-code}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `Directory` | インデックス化されたリポジトリ内のディレクトリ | `id`, `path`, `name` |
| `File` | ソースコードファイル | `id`, `path`, `name`, `extension`, `language`, `content` |
| `Definition` | 関数、クラス、メソッド、またはモジュールの定義 | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `ImportedSymbol` | インポートまたはクロスファイルシンボルの参照 | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## リレーションシップ {#relationships}

ローカルグラフのエッジは以下を接続します。

- ディレクトリと、そのディレクトリに含まれるファイルおよびサブディレクトリ
- ファイルと、そのファイルが宣言する定義
- ファイルと、そのファイルがインポートするシンボル
- インポートされたシンボルと、他のファイル内でそのシンボルが解決される定義

## Orbit Remoteとの違い {#differences-from-orbit-remote}

[Orbit Remote](../remote/schema.md)は6つのドメインにわたる24のノードタイプをインデックス化します。Orbit Localはソースコードドメインのみを対象とします。GitLabのデータ（マージリクエスト、パイプライン、ユーザー、脆弱性、作業アイテム）を必要とする機能は利用できません。

## 注意事項 {#notes}

- 定義IDは、ファイルパスごとにスコープされたコンテンツハッシュ整数です。インデックス化された2つのリポジトリに同じ関数が存在する場合、それぞれ異なるIDが割り当てられます。
- `Definition`ノードおよび`File`ノードの`content`フィールドには、完全なソーステキストが含まれます。これらは、エージェントツールが個別のファイル読み取りなしにコードをハイドレートできるよう、入力された状態になっています。
- 認可レイヤーは存在しません。Orbit Localはユーザーごとのアクセス制御を適用しません。`~/.orbit/graph.duckdb`にあるグラフファイルは、ファイルシステムのパーミッションによってのみ保護されています。
