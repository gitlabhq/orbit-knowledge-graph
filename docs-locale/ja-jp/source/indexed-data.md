---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit LocalおよびGitLab Orbit Remoteでインデックス作成できるデータについて説明します。
title: GitLab Orbitでデータのインデックスを作成する
---

GitLab Orbitは、コードリポジトリやトップレベルグループに紐づくSDLCデータなど、複数のソースからデータのインデックスを作成します。GitLab Orbitがデータのインデックスを作成すると、コードベース全体の関係性、依存関係、および構造的なコンテキストをクエリできるナレッジグラフが構築されます。

GitLab Orbit Remoteを使用して、トップレベルグループとそのサブグループおよびプロジェクトのデータのインデックスを作成します。

GitLab Orbit Localを使用して、任意のローカルリポジトリのワークツリーからデータのインデックスを作成します。

<!-- markdownlint-disable-next-line MD044 -->
## GitLab Orbitがインデックス作成するデータ {#what-data-gitlab-orbit-indexes}

GitLab Orbit LocalとGitLab Orbit Remoteは、異なる種類のデータのインデックスを作成します。以下のセクションでは、各機能がインデックス作成する内容を示します。

GitLab Orbit RemoteおよびGitLab Orbit Localは、以下のデータのインデックスを作成しません。

- バイナリファイル
- チェックアウト済みブランチ以外のブランチ（GitLab Orbit Localの場合）またはデフォルトブランチ以外のブランチ（GitLab Orbit Remoteの場合）

### ソースコード {#source-code}

| コード構造 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------|--------|
| ファイルとディレクトリ | {{< yes >}} | {{< yes >}} |
| 関数、クラス、メソッド、モジュールの定義 | {{< yes >}} | {{< yes >}} |
| インポート宣言 | {{< yes >}} | {{< yes >}} |
| クロスファイルシンボル参照 | {{< yes >}} | {{< yes >}} |

### グループ、プロジェクト、ユーザー {#groups-projects-and-users}

| グループ、プロジェクト、ユーザー | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| グループ | {{< no >}} | {{< yes >}} |
| プロジェクト | {{< no >}} | {{< yes >}} |
| ユーザー | {{< no >}} | {{< yes >}} |
| ノートとコメント | {{< no >}} | {{< yes >}} |

### コードレビュー {#code-review}

| コードレビュー | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| マージリクエスト | {{< no >}} | {{< yes >}} |
| マージリクエストの差分 | {{< no >}} | {{< yes >}} |
| 変更されたファイル | {{< no >}} | {{< yes >}} |

### CI/CDパイプライン {#ci-cd-pipelines}

| CI/CD | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| パイプライン | {{< no >}} | {{< yes >}} |
| ステージ | {{< no >}} | {{< yes >}} |
| ジョブ | {{< no >}} | {{< yes >}} |

### コード計画 {#code-planning}

| コード計画 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| イシュー | {{< no >}} | {{< yes >}} |
| エピック | {{< no >}} | {{< yes >}} |
| タスク | {{< no >}} | {{< yes >}} |
| インシデント | {{< no >}} | {{< yes >}} |
| マイルストーン | {{< no >}} | {{< yes >}} |
| ラベル | {{< no >}} | {{< yes >}} |

### セキュリティ {#security}

| セキュリティ | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| 脆弱性 | {{< no >}} | {{< yes >}} |
| セキュリティの検出結果 | {{< no >}} | {{< yes >}} |
| セキュリティスキャン | {{< no >}} | {{< yes >}} |
| スキャナー | {{< no >}} | {{< yes >}} |
| CVE識別子 | {{< no >}} | {{< yes >}} |
| CWE識別子 | {{< no >}} | {{< yes >}} |

### 作業スコープ {#work-scope}

| スコープ | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| ワークツリー（チェックアウト済みコード） | {{< yes >}} | {{< no >}} |
| デフォルトブランチのみ | {{< no >}} | {{< yes >}} |
| 複数のリポジトリ | {{< yes >}} | {{< no >}} |
| ブランチの選択 | {{< no >}} | {{< no >}} |

## サポートされている言語 {#supported-languages}

GitLab Orbit RemoteおよびLocalは、以下の言語のデータのインデックスを作成します。

| 言語 | 定義 | クロスファイル参照 |
|----------|-------------|----------------------|
| Ruby | {{< yes >}} | {{< yes >}} |
| Java | {{< yes >}} | {{< yes >}} |
| Kotlin | {{< yes >}} | {{< yes >}} |
| Python | {{< yes >}} | {{< yes >}} |
| TypeScript | {{< yes >}} | {{< yes >}} |
| JavaScript | {{< yes >}} | {{< yes >}} |
| Rust | {{< yes >}} | {{< yes >}} |
| Go | {{< yes >}} | {{< yes >}} |
| C# | {{< yes >}} | {{< yes >}} |
| C | {{< yes >}} | {{< yes >}} |
| C++ | {{< yes >}} | {{< yes >}} |
| PHP | {{< yes >}} | {{< yes >}} |
| Bash/Shell | {{< yes >}} | {{< no >}} |
