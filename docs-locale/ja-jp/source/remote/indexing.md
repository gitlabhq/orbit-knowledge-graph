---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbitがインデックスするデータ、コードインデックスでサポートされている言語、およびインデックスのスコープについて説明します。
title: Orbitがインデックスするもの
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階にあります。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴をご参照ください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

## スコープ {#scope}

Orbitはトップレベルグループのみをインデックスします。トップレベルグループでOrbitを有効にすると、そのすべてのサブグループとプロジェクトが自動的にインデックスされます。サブグループや個別のプロジェクトでOrbitを有効にすることはできません。

## SDLCデータ {#sdlc-data}

Orbitは以下のGitLabオブジェクトとその関係をインデックスします。

| ドメイン | インデックスされるオブジェクト |
|--------|----------------|
| コア | グループ、プロジェクト、ユーザー、ノート（コメント） |
| コードレビュー | マージリクエスト、マージリクエストの差分、変更されたファイル |
| CI/CD | パイプライン、ステージ、ジョブ |
| プランニング | 作業アイテム（イシュー、エピック、タスク、インシデント）、マイルストーン、ラベル |
| セキュリティ | 脆弱性、セキュリティ検出結果、セキュリティスキャン、スキャナー、CVE/CWE識別子 |

SDLCデータは変更データキャプチャによって継続的に更新されます。GitLabインスタンスでの変更は数分以内にOrbitに反映されます。

## ソースコード {#source-code}

Orbitはリポジトリからソースコードをインデックスし、その上にコードグラフを構築します。

インデックスされる内容：

- ファイルおよびディレクトリ
- 関数、クラス、モジュールの定義（開始・終了行および完全なソースコンテンツを含む）
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

現在インデックスされていない言語：Swift、COBOL、Terraform、YAML。

## インデックスされないもの {#what-is-not-indexed}

- デフォルトブランチ以外のブランチ
- バイナリファイル
- アーカイブされたプロジェクト内のファイル（アーカイブされたプロジェクトのSDLCメタデータは引き続きインデックスされます）
- リクエストしたユーザーがアクセス権を持たないプライベートコンテンツ（認可はクエリ時に適用されます）

## 認可 {#authorization}

Orbitはクエリ時にGitLabのアクセス制御を適用します。クエリはGitLabでリクエストしたユーザーがアクセスできるエンティティのみを返します。Orbit独自の権限モデルは存在しません。

Orbitを有効にしたグループのオーナーは、GitLabですでに持っている以上のアクセス権を他のユーザーに付与することはありません。
