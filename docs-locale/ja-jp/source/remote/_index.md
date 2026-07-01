---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLabがホストするインフラストラクチャ上で動作するOrbit
title: Orbit Remote
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 18.10で`knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

Orbit RemoteはGitLabがホストするインフラストラクチャ上で動作します。トップレベルグループで有効にすると、グループ、プロジェクト、ユーザー、マージリクエスト、パイプライン、脆弱性、ソースコードなど、SDLC全体とコードをClickHouseプロパティグラフに自動的にインデックス作成します。

- インデックス作成対象: SDLC全体 + コードグラフ
- ストレージ: ClickHouse（マネージド、セットアップ不要）

[Orbit Remoteを使ってみる](getting-started.md)

## このセクションの内容 {#in-this-section}

| ページ | 説明 |
|---|---|
| [はじめに](getting-started.md) | Orbitを有効にして最初のクエリを実行する |
| [仕組み](how-it-works.md) | インデックス作成パイプライン、グラフモデル、クエリ実行 |
| [Orbitのインデックス作成対象](indexing.md) | SDLCカバレッジ、言語サポート、インデックス作成スコープ |
| [セキュリティ](security.md) | クエリに必要なロール、認可モデル、プログラムによるアクセス |
| [スキーマリファレンス](schema.md) | 6つのドメインにわたる28種類のノードタイプ |
| [Cookbook](cookbook.md) | 一般的なユースケース向けのコピー&ペーストクエリ |
| [クエリ言語](queries/) | クエリDSLの完全なリファレンス |

## アクセス方法 {#access-methods}

| 方法 | 説明 |
|---|---|
| [GitLab Duo Agent Platform](access/duo.md) | GitLab UIを通じた自然言語による質問 |
| [MCP](access/mcp.md) | Claude Code、Codex、その他のエージェントと接続する |
| [GitLab CLI（`glab`）](access/glab.md) | スクリプティングと探索のための`glab orbit remote`（`glab` 1.94以降で利用可能） |
| [REST API](access/api.md) | スクリプト、CIパイプライン、またはカスタムツールからクエリを実行する |

## 課金 {#billing}

MCPおよびREST APIのクエリはGitLabクレジットを消費します。GitLab Duo Agent Platformのクエリは消費対象外です。
