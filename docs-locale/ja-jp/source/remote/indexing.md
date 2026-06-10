---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbitがインデックス作成するデータ、コードインデックス作成でサポートされている言語、およびインデックス作成のスコープについて説明します。
title: Orbitのインデックス作成対象
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ

{{< /details >}}

{{< history >}}

- GitLab 18.10で`knowledge_graph`という名前の[機能フラグ付き](https://docs.gitlab.com/administration/feature_flags/)で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

## スコープ {#scope}

Orbitはトップレベルグループのみをインデックス作成します。トップレベルグループでOrbitを有効にすると、そのサブグループとプロジェクトがすべて自動的にインデックス作成されます。サブグループや個別のプロジェクトでOrbitを有効にすることはできません。

## SDLCデータ {#sdlc-data}

Orbitは以下のGitLabオブジェクトとその関係をインデックス作成します。

| ドメイン | インデックス作成されるオブジェクト |
|--------|----------------|
| コア | グループ、プロジェクト、ユーザー、ノート（コメント） |
| コードレビュー | マージリクエスト、マージリクエストの差分、変更ファイル |
| CI/CD | パイプライン、ステージ、ジョブ |
| プランニング | 作業アイテム（イシュー、エピック、タスク、インシデント）、マイルストーン、ラベル |
| セキュリティ | 脆弱性、セキュリティ検出結果、セキュリティスキャン、スキャナー、CVE/CWE識別子 |

SDLCデータは変更データキャプチャによって継続的に更新されます。GitLabインスタンスでの変更は数分以内にOrbitに反映されます。

## ソースコード {#source-code}

Orbitはリポジトリからソースコードをインデックス作成し、その上にコードグラフを構築します。

インデックス作成される内容:

- ファイルおよびディレクトリ
- 関数、クラス、モジュールの定義（開始・終了行とソースコード全体を含む）
- ファイル間のインポートおよびクロスファイル参照の関係

コードはデフォルトブランチのみからインデックス作成されます。デフォルトブランチが変更されると、Orbitは自動的に再インデックス作成を行います。

### サポートされている言語 {#supported-languages}

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
| PHP | あり | あり |

現在インデックス作成されていない言語: Swift、COBOL、Terraform、YAML。

## インデックス作成されないもの {#what-is-not-indexed}

- デフォルトブランチ以外のブランチ
- バイナリファイル
- アーカイブ済みプロジェクト内のファイル（アーカイブ済みプロジェクトのSDLCメタデータは引き続きインデックス作成されます）
- リクエストユーザーがアクセス権を持たないプライベートコンテンツ（認可はクエリ時に適用されます）

## 認可 {#authorization}

Orbitはクエリ時にGitLabのアクセス制御を適用します。クエリはGitLabでリクエストユーザーがアクセスできるエンティティのみを返します。Orbit独自の権限モデルはありません。

Orbitを有効にしたグループのオーナーは、他のユーザーにGitLabで既に持っている以上のアクセス権を付与することはありません。
