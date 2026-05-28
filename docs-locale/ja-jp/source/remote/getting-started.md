---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab.comでOrbit Remoteを有効にして、最初のクエリを実行します。
title: Orbit Remoteを使ってみる
---

{{< details >}}

- 階層：Premium、Ultimate
- 提供形態：GitLab.com
- ステータス：実験

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階にあります。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴をご参照ください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

## 前提条件 {#prerequisites}

- Orbitを有効にするトップレベルグループのオーナーロール

Orbitはトップレベルグループのみをインデックス作成します。サブグループとプロジェクトは自動的にインデックス作成を継承します。

## ステップ1：Orbitを有効にする {#step-1-enable-orbit}

1. 左サイドバーで**自分の作業**を展開します。
1. **Orbit** > **設定**を選択します。
1. **インデックス**リストでトップレベルグループを見つけます。
1. **有効にする**を切り替えます。

Orbitはすぐにインデックス作成を開始します。小規模なグループでは初期インデックス作成に数分かかり、数千のプロジェクトを持つグループでは最大30分かかります。

インデックス作成のステータスはいつでも確認できます：

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

## ステップ2：最初のクエリを実行する {#step-2-run-your-first-query}

Orbit Remoteは同じグラフを3つのインターフェースで公開しています。クエリを実行するユーザーに合ったものをお選びください：

| 方法 | 最適な用途 | セットアップ | 課金 |
|---|---|---|---|
| **GitLab Duo Agent Platform** | GitLab UIのエンドユーザー | 不要 | 無料 |
| **MCP** | Claude Code、Codex、その他のAIエージェント | エージェントの初回設定 | GitLab Credits |
| **REST API** | スクリプト、ダッシュボード、カスタムツール | APIトークン | GitLab Credits |

### GitLab Duo Agent Platform（セットアップ不要） {#gitlab-duo-agent-platform-no-setup-required}

OrbitはGitLab Duo Agent Platformに組み込まれています。GitLab Duo Agent、Planner Agent、Security Analyst Agent、Data Analyst Agent、CI Expert Agent、Developer Flowは、グラフトラバーサルで回答するのが最適な質問に対して、Orbitの`query_graph`ツールと`get_graph_schema`ツールを自動的に呼び出します。ツールの選択や設定は不要です。

たとえば、`deploy_user`メソッドの名前変更を依頼する作業アイテムを登録すると、Developer FlowはOrbitを使用してそのメソッドを呼び出しているすべてのサービスを特定し、それぞれを更新するMRを作成します。

GitLab Duoのクエリは無料で、GitLab Creditsを消費しません。

### MCP（Claude Code、Codex、その他のエージェント） {#mcp-claude-code-codex-other-agents}

セットアップについては[MCPを使用したOrbitの利用](access/mcp.md)をご参照ください。設定が完了すると、`query_graph`と`get_graph_schema`の2つのツールが利用できます。
<!-- markdownlint-disable-next-line MD044 -->
### REST API {#rest-api}

`your-group`をOrbitを有効にしたトップレベルグループのパスに置き換えてください。`full_path`フィルターはクエリのスコープを絞り込み、Orbitの選択性検証を通過させます。

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data '{
    "query": {
      "query_type": "traversal",
      "node": {
        "id": "p",
        "entity": "Project",
        "columns": ["name", "full_path"],
        "filters": {
          "full_path": {"op": "starts_with", "value": "your-group/"}
        }
      },
      "limit": 10
    },
    "format": "raw"
  }' \
  "https://gitlab.com/api/v4/orbit/query"
```

## 次に試すこと {#what-to-try-next}

- [Orbitがインデックス作成する対象](indexing.md) - クエリを作成する前にカバレッジを理解する
- [スキーマリファレンス](schema.md) - 24種類のノードタイプとそのプロパティを確認する
- [Cookbook](cookbook.md) - 一般的なユースケース向けのコピー＆ペーストクエリ
- [Orbit Localを使ってみる](../local/getting-started.md) - ローカルリポジトリをオフラインでクエリする
