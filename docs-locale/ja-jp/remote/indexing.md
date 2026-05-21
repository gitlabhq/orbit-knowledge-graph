---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbitがインデックスするデータ、コードインデックスでサポートされる言語、およびインデックスのスコープについて説明します。
title: Orbitがインデックスするもの
---

{{< details >}}

- 階層: Premium, Ultimate
- 提供形態: GitLab.com
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 18.10で`knowledge_graph`という名前の[機能フラグ付き](https://docs.gitlab.com/administration/feature_flags/)で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階です。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

## スコープ {#scope}

Orbitはトップレベルグループのみをインデックスします。トップレベルグループでOrbitを有効にすると、そのすべてのサブグループおよびプロジェクトが自動的にインデックスされます。サブグループや個別のプロジェクトでOrbitを有効にすることはできません。

## SDLCデータ {#sdlc-data}

Orbitは以下のGitLabオブジェクトとその関係をインデックスします。

| ドメイン | インデックスされるオブジェクト |
|--------|----------------|
| コア | グループ、プロジェクト、ユーザー、ノート（コメント） |
| コードレビュー | マージリクエスト、マージリクエストの差分、変更ファイル |
| CI/CD | パイプライン、ステージ、ジョブ |
| プランニング | 作業アイテム（イシュー、エピック、タスク、インシデント）、マイルストーン、ラベル |
| セキュリティ | 脆弱性、セキュリティ検出結果、セキュリティスキャン、スキャナー、CVE/CWE識別子 |

SDLCデータは変更データキャプチャによって継続的に更新されます。GitLabインスタンスでの変更は数分以内にOrbitに反映されます。

## ソースコード {#source-code}

Orbitはリポジトリからソースコードをインデックスし、その上にコードグラフを構築します。

インデックスされる内容:

- ファイルおよびディレクトリ
- 関数、クラス、モジュールの定義（開始/終了行および完全なソースコンテンツを含む）
- ファイル間のインポートおよびクロスファイル参照の関係

コードはデフォルトブランチのみからインデックスされます。デフォルトブランチが変更されると、Orbitは自動的に再インデックスします。

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

現在インデックスされていない言語: Swift、COBOL、Terraform、YAML。

## インデックスされないもの {#what-is-not-indexed}

- デフォルトブランチ以外のブランチ
- バイナリファイル
- アーカイブされたプロジェクト内のファイル（アーカイブされたプロジェクトのSDLCメタデータは引き続きインデックスされます）
- リクエストユーザーがアクセス権を持たないプライベートコンテンツ（認可はクエリ時に適用されます）

## 認可 {#authorization}

Orbitはクエリ時にGitLabのアクセス制御を適用します。クエリはGitLabでリクエストユーザーがアクセスできるエンティティのみを返します。Orbit独自の権限モデルはありません。

Orbitを有効にしたグループのオーナーは、GitLabですでに持っているアクセス権を超えた権限を他のユーザーに付与することはありません。
