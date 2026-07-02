---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: 6つのドメインにわたる27のOrbitノードタイプの完全なリファレンス（プロパティとその型を含む）。
title: スキーマリファレンス
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

Orbitは6つのドメインにわたる27のノードタイプのインデックスを作成します。クエリのエンティティ名としてこれらを使用してください。

ライブスキーマをいつでもフェッチするには:

```shell
glab orbit remote schema
```

## コア {#core}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `Group` | GitLabグループまたはサブグループ | `id`, `full_path`, `name`, `visibility`, `traversal_path` |
| `Project` | GitLabプロジェクトおよびリポジトリ | `id`, `full_path`, `name`, `visibility`, `archived`, `star_count` |
| `User` | GitLabユーザーアカウント | `id`, `username`, `email`, `name`, `state`, `is_admin` |
| `Note` | GitLabオブジェクトに対するコメントまたは注釈 | `id`, `note`, `noteable_type`, `noteable_id`, `internal`, `confidential` |

## ソースコード {#source-code}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `Branch` | Gitブランチ | `id`, `project_id`, `name`, `is_default` |
| `Definition` | 関数、クラス、メソッド、またはモジュールの定義 | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `Directory` | リポジトリ内のディレクトリ | `id`, `project_id`, `path`, `name` |
| `File` | ソースコードファイル | `id`, `path`, `name`, `extension`, `language`, `content` |
| `ImportedSymbol` | インポートまたはクロスファイルシンボル参照 | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## コードレビュー {#code-review}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `MergeRequest` | マージリクエスト | `id`, `iid`, `title`, `description`, `source_branch`, `target_branch`, `state`, `draft`, `squash` |
| `MergeRequestDiff` | MR内の変更のスナップショット | `id`, `merge_request_id`, `commits_count`, `files_count` |
| `MergeRequestDiffFile` | MR差分で変更されたファイル | `id`, `new_path`, `old_path`, `new_file`, `renamed_file`, `deleted_file` |

## CI/CD {#ci-cd}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `Pipeline` | CI/CDパイプラインの実行 | `id`, `sha`, `ref`, `status`, `source`, `duration`, `failure_reason` |
| `Stage` | パイプラインステージ | `id`, `name`, `status`, `position` |
| `Job` | CI/CDジョブ | `id`, `name`, `status`, `ref`, `allow_failure`, `environment`, `failure_reason` |
| `Deployment` | コミットのCI/CDデプロイ | `id`, `iid`, `status`, `ref`, `sha`, `environment_id` |
| `Environment` | CI/CDデプロイターゲット | `id`, `name`, `state`, `tier`, `external_url` |
| `Runner` | CI/CD Runner | `id`, `runner_type`, `name`, `active`, `locked` |

## プランニング {#planning}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `WorkItem` | イシュー、エピック、タスク、インシデント、またはその他の作業アイテム | `id`, `iid`, `title`, `description`, `state`, `work_item_type`, `due_date`, `weight` |
| `Milestone` | マイルストーン | `id`, `title`, `state`, `due_date`, `start_date` |
| `Label` | 作業を分類するためのラベル | `id`, `title`, `color` |

## セキュリティ {#security}

| ノードタイプ | 説明 | 主要プロパティ |
|-----------|-------------|----------------|
| `Finding` | `security_findings`からのセキュリティスキャン検出結果 | `id`, `uuid`, `name`, `description`, `severity`, `deduplicated` |
| `SecurityScan` | パイプライン内のセキュリティスキャン実行 | `id`, `scan_type`, `status`, `latest` |
| `Vulnerability` | 確認済みまたは潜在的なセキュリティ脆弱性 | `id`, `title`, `state`, `severity`, `report_type`, `resolved_on_default_branch` |
| `VulnerabilityIdentifier` | CVE、CWE、またはその他の外部参照 | `id`, `external_type`, `external_id`, `name`, `url` |
| `VulnerabilityOccurrence` | 脆弱性の特定の発生箇所（Railsでは`Vulnerabilities::Finding`） | `id`, `uuid`, `severity`, `report_type`, `detection_method`, `cve`, `location` |
| `VulnerabilityScanner` | セキュリティスキャナー | `id`, `external_id`, `name`, `vendor` |

## 注記 {#notes}

- 定義IDは、プロジェクトおよびブランチごとにスコープされたコンテンツハッシュ整数です。異なるプロジェクトで同じシンボルを定義した場合、関数名やファイルパスが同一であっても、IDは異なります。
- すべてのエンティティIDは、基となる値が整数であっても、クエリレスポンスでは文字列として返されます。これにより、`Number.MAX_SAFE_INTEGER`を超える値に対してJavaScriptクライアントでの精度損失を防ぎます。
- `Definition`および`File`ノードの`content`フィールドには、定義またはファイルの完全なソーステキストが含まれます。これらのフィールドは、GitLabへの個別のAPIコールを行わずにファイルコンテンツをハイドレートする必要があるエージェントツールで利用できます。
- すべてのノードには、認可フィルタリングに使用される`traversal_path`プロパティが含まれています。クエリ結果は、リクエストを行うユーザーがアクセスできるエンティティに自動的にスコープされます。
