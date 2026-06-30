---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab.comでOrbit Remoteを有効にして、最初のクエリを実行します。
title: Orbit Remoteを使ってみる
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 18.10で`knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

## 前提条件 {#prerequisites}

- Orbitを有効にするトップレベルグループのオーナーロール

Orbitはトップレベルグループのみをインデックス作成します。サブグループとプロジェクトは自動的にインデックス作成を継承します。

## ステップ1: Orbitを有効にする {#step-1-enable-orbit}

1. 左サイドバーで**マイワーク**を展開します。
1. **Orbit** > **設定**を選択します。
1. **インデックス**リストでトップレベルグループを見つけます。
1. **有効にする**を切り替えます。

Orbitはすぐにインデックス作成を開始します。初回のインデックス作成は、小規模なグループでは数分、数千のプロジェクトを持つグループでは最大30分かかります。

インデックス作成のステータスはいつでも確認できます。

```shell
glab orbit remote status
```

## ステップ2: 最初のクエリを実行する {#step-2-run-your-first-query}

Orbit Remoteは同じグラフを3つのインターフェースで公開しています。クエリを実行するユーザーに合ったものを選択してください。

| 方法 | 最適な用途 | セットアップ | 課金 |
|---|---|---|---|
| **GitLab Duo Agent Platform** | GitLab UIのエンドユーザー | 不要 | 消費対象外 |
| **MCP** | Claude Code、Codex、その他のAIエージェント | エージェントの初回設定 | GitLab Credits |
| **REST API** | スクリプト、ダッシュボード、カスタムツール | APIトークン | GitLab Credits |

### GitLab Duo Agent Platform（セットアップ不要） {#gitlab-duo-agent-platform-no-setup-required}

OrbitはGitLab Duo Agent Platformに組み込まれています。GitLab Duo Agent、Planner Agent、Security Analyst Agent、Data Analyst Agent、CI Expert Agent、Developer Flowは、グラフトラバーサルで回答するのが最適な質問に対して、Orbitの`query_graph`ツールと`get_graph_schema`ツールを自動的に呼び出します。ツールの選択や設定は不要です。

たとえば、`deploy_user`メソッドの名前変更を依頼する作業アイテムを登録すると、Developer FlowはOrbitを使用してそのメソッドを呼び出しているすべてのサービスを特定し、それぞれを更新するMRを作成します。

GitLab Duoのクエリは消費対象外であり、GitLab Creditsを消費しません。

### MCP（Claude Code、Codex、その他のエージェント） {#mcp-claude-code-codex-other-agents}

セットアップについては[MCPを使用してOrbitにアクセスする](access/mcp.md)を参照してください。設定が完了すると、`query_graph`と`get_graph_schema`の2つのツールが使用できます。

### AIエージェント向けOrbitスキルをインストールする {#install-the-orbit-skill-for-ai-agents}

OrbitスキルはAIエージェントにクエリレシピ、DSLガイダンス、トラブルシューティングを提供し、初回から正しいOrbitクエリを作成できるようにします。

```shell
glab skills install --global orbit
```

プロジェクトスコープのインストール、アップデート手順、スキルの内容については、[Orbitスキルを使用してAIコーディングエージェントをセットアップする](../ai_coding_agents.md)を参照してください。
<!-- markdownlint-disable-next-line MD044 -->
### REST API {#rest-api}

`your-group`をOrbitを有効にしたトップレベルグループのパスに置き換えてください。`full_path`フィルターはクエリのスコープを絞り込み、Orbitの選択性検証を通過させます。

リクエストボディを`request.json`に保存してください。

```json orbit-query
{
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
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  "https://gitlab.com/api/v4/orbit/query"
```

## 次に試すこと {#what-to-try-next}

- [Orbitのインデックス対象](indexing.md) - クエリを作成する前にカバレッジを理解する
- [スキーマリファレンス](schema.md) - 28種類のノードタイプとそのプロパティを確認する
- [Cookbook](cookbook.md) - 一般的なユースケース向けのコピー&ペーストクエリ
- [Orbit Localを使ってみる](../local/getting-started.md) - ローカルリポジトリをオフラインでクエリする
