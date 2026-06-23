---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Localコードグラフの4つのノードタイプとその接続方法に関するリファレンス。
title: スキーマリファレンス
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

Orbit Localはソースコードドメインに属する4つのノードタイプをインデックス作成します。Orbit LocalはGitLabに接続しないため、SDLCレイヤーはありません。

ライブのDuckDBスキーマをいつでも確認するには、次のコマンドを実行します。

```shell
orbit schema
```

## ソースコード {#source-code}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `Directory` | インデックス作成済みリポジトリ内のディレクトリ | `id`、`path`、`name` |
| `File` | ソースコードファイル | `id`、`path`、`name`、`extension`、`language`、`content` |
| `Definition` | 関数、クラス、メソッド、またはモジュールの定義 | `id`、`file_path`、`fqn`、`name`、`definition_type`、`start_line`、`end_line`、`content` |
| `ImportedSymbol` | インポートまたはクロスファイルのシンボル参照 | `id`、`file_path`、`import_type`、`import_path`、`identifier_name` |

## リレーションシップ {#relationships}

ローカルグラフのエッジは以下を接続します。

- ディレクトリと、そのディレクトリに含まれるファイルおよびサブディレクトリ
- ファイルと、そのファイルで宣言された定義
- ファイルと、そのファイルがインポートするシンボル
- インポートされたシンボルと、他のファイル内で解決される定義

## Orbit Remoteとの違い {#differences-from-orbit-remote}

[Orbit Remote](../remote/schema.md)は6つのドメインにわたる28のノードタイプをインデックス作成します。Orbit Localはソースコードドメインのみを対象としており、GitLabのデータ（マージリクエスト、パイプライン、ユーザー、脆弱性、作業アイテム）を必要とする機能は利用できません。

## 注意事項 {#notes}

- 定義IDは、ファイルパスごとにスコープされたコンテンツハッシュ整数です。2つのインデックス作成済みリポジトリに同じ関数が存在する場合、それぞれ異なるIDが割り当てられます。
- `Definition`ノードおよび`File`ノードの`content`フィールドには、完全なソーステキストが含まれます。これらのフィールドは、エージェントツールが個別のファイル読み取りなしにコードをハイドレートできるよう、値が入力されています。
- 認可レイヤーはありません。Orbit Localはユーザーごとのアクセス制御を適用しません。`~/.orbit/graph.duckdb`にあるグラフファイルは、ファイルシステムのパーミッションによってのみ保護されています。
