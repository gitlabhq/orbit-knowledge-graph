---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLabホスト型インフラストラクチャで動作するOrbit
title: Orbit Remote
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: 実験

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階にあります。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

Orbit RemoteはGitLabホスト型インフラストラクチャで動作します。トップレベルグループで有効にすると、グループ、プロジェクト、ユーザー、マージリクエスト、パイプライン、脆弱性、ソースコードなど、SDLC全体とコードをClickHouseプロパティグラフに自動的にインデックス作成します。

- インデックス対象: SDLC全体 + コードグラフ
- ストレージ: ClickHouse（マネージド、セットアップ不要）

[Orbit Remoteを使ってみる](getting-started.md)

## このセクションの内容 {#in-this-section}

| ページ | 説明 |
|---|---|
| [はじめに](getting-started.md) | Orbitを有効にして最初のクエリを実行する |
| [仕組み](how-it-works.md) | インデックス作成パイプライン、グラフモデル、クエリ実行 |
| [Orbitのインデックス対象](indexing.md) | SDLCカバレッジ、言語サポート、認可 |
| [スキーマリファレンス](schema.md) | 6つのドメインにわたる24種類のノードタイプ |
| [Cookbook](cookbook.md) | 一般的なユースケース向けのコピー&ペーストクエリ |
| [クエリ言語](queries/) | クエリDSLの完全なリファレンス |

## アクセス方法 {#access-methods}

| 方法 | 説明 |
|---|---|
| [GitLab Duo Agent Platform](access/duo.md) | GitLab UIを通じた自然言語による質問 |
| [MCP](access/mcp.md) | Claude Code、Codex、その他のエージェントと接続 |
| [GitLab CLI（`glab`）](access/glab.md) | スクリプティングと検索のための`glab orbit remote`（`glab` 1.94以降で利用可能） |
| [REST API](access/api.md) | スクリプト、CIパイプライン、またはカスタムツールからクエリを実行 |

## 課金 {#billing}

MCPおよびREST APIのクエリはGitLabクレジットを消費します。GitLab Duo Agent Platformのクエリは無料です。
