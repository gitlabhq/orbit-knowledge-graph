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

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)を使用して、GitLab 18.10で[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階です。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

## 前提条件 {#prerequisites}

- Orbitを有効にするトップレベルグループのオーナーロール

Orbitはトップレベルグループのみをインデックス作成します。サブグループとプロジェクトは自動的にインデックス作成を継承します。

## ステップ1：Orbitを有効にする {#step-1-enable-orbit}

1. 左サイドバーで、**自分の作業**を展開します。
1. **Orbit** > **設定**を選択します。
1. **インデックス**リストでトップレベルグループを見つけます。
1. **有効にする**を切り替えます。

Orbitはすぐにインデックス作成を開始します。初回のインデックス作成は、小規模なグループでは数分、数千のプロジェクトを持つグループでは最大30分かかります。

インデックス作成のステータスはいつでも確認できます：

```shell
curl --header "Authorization: Bearer <your_token>" \
  "https://gitlab.com/api/v4/orbit/status"
```

## ステップ2：最初のクエリを実行する {#step-2-run-your-first-query}

Orbit Remoteは3つのインターフェースを通じて同じグラフを公開しています。クエリを実行するユーザーに合ったものを選択してください：

| 方法 | 最適な用途 | セットアップ | 課金 |
|---|---|---|---|
| **GitLab Duo Agent Platform** | GitLab UIのエンドユーザー | 不要 | 無料 |
| **MCP** | Claude Code、Codex、その他のAIエージェント | エージェントの1回限りの設定 | GitLab Credits |
| **REST API** | スクリプト、ダッシュボード、カスタムツール | APIトークン | GitLab Credits |

### GitLab Duo Agent Platform（セットアップ不要） {#gitlab-duo-agent-platform-no-setup-required}

OrbitはGitLab Duo Agent Platformに組み込まれています。GitLab Duo Agent、Planner Agent、Security Analyst Agent、Data Analyst Agent、CI Expert Agent、Developer Flowは、質問がグラフトラバーサルで最もよく回答できる場合に、Orbitの`query_graph`ツールと`get_graph_schema`ツールを自動的に呼び出します。ツールの選択や設定は不要です。

たとえば、`deploy_user`メソッドの名前変更を依頼する作業アイテムを登録します。Developer FlowはOrbitを使用してそれを呼び出しているすべてのサービスを特定し、それぞれを更新するMRを作成します。

GitLab Duoのクエリは無料で、GitLab Creditsを消費しません。

### MCP（Claude Code、Codex、その他のエージェント） {#mcp-claude-code-codex-other-agents}

セットアップについては、[MCPを使用してOrbitを利用する](access/mcp.md)を参照してください。設定が完了すると、`query_graph`と`get_graph_schema`の2つのツールが使用できます。

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

- [Orbitがインデックス作成する内容](indexing.md) - クエリを作成する前にカバレッジを理解する
- [スキーマリファレンス](schema.md) - 24種類のノードタイプとそのプロパティを調べる
- [Cookbook](cookbook.md) - 一般的なユースケース向けのコピー＆ペーストクエリ
- [Orbit Localを使ってみる](../local/getting-started.md) - ローカルリポジトリをオフラインでクエリする
