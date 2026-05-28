---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Local - ご自身のマシン上でコードグラフを構築・クエリできます。GitLabインスタンスは不要です。
title: Orbit Local
---

{{< details >}}

- Tier: Free, Premium, Ultimate
- Offering: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- Status: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

Orbit Localはお使いのマシン上で完全に動作します。任意のローカルリポジトリのコードグラフを構築し、Orbit Remoteと同じクエリ言語を使用してクエリできます。GitLabアカウントもネットワーク接続も不要です。

- インデックス対象：ファイル、定義、クロスファイル参照を含むコードのみ
- ストレージ：DuckDB（`~/.orbit/graph.duckdb`のローカルファイル）

[Orbit Localを始める](getting-started.md)

## このセクションの内容 {#in-this-section}

| ページ | 説明 |
|---|---|
| [はじめに](getting-started.md) | アクセス方法を選択して最初のクエリを実行する |
| [仕組み](how-it-works.md) | インデックス作成パイプライン、グラフモデル、クエリ実行 |
| [Orbit Localのインデックス対象](indexing.md) | コードカバレッジ、言語サポート、スコープ |
| [スキーマリファレンス](schema.md) | ローカルコードグラフの4つのノードタイプ |

## アクセス方法 {#access-methods}

| 方法 | 説明 |
|---|---|
| [Orbit CLI（`orbit`）](access/cli.md) | `orbit`バイナリを直接実行してインデックス作成とクエリを行う |
| [GitLab CLI（`glab`）](access/glab.md) | `glab orbit local`でOrbit Localを操作する（予定） |
| [MCP](access/mcp.md) | ローカルグラフをClaude Code、Codex、その他のエージェントに公開する |

## 課金 {#billing}

Orbit LocalはGitLab Creditsを消費しません。すべての処理はローカルで行われます。
