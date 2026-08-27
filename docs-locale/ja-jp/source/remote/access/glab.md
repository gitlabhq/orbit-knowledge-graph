---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: glab orbit remoteを使用してコマンドラインからGitLab Orbitにクエリを実行できます。glab 1.94以降で利用可能です。glab orbit setupヘルパーは将来のglabリリースで提供予定です。
title: GitLab CLI（`glab`）でOrbitを使用する
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

<!-- -->

> [!disclaimer]

[GitLab CLI（`glab`）](https://docs.gitlab.com/cli/)は、コマンドラインからGitLab Orbitをセットアップおよびクエリするための標準的な方法です。

`glab orbit`は管理された`orbit`バイナリを実行します。各コマンドをバイナリに転送し、`glab`がダウンロード、検証、および最新状態の維持を自動で行います。バイナリ独自のコマンドリファレンスは`glab orbit remote <command> --help`で確認できます。

- `glab orbit remote`: GitLab Orbit Remote REST APIをクエリします。`glab`がGitLabの認証情報を自動的に注入します。`glab` 1.94以降で利用可能です。
- `glab orbit setup`: GitLab OrbitスキルのインストールとAIエージェントの設定をガイド付きオンボーディングで行います。

## 前提条件 {#prerequisites}

- GitLab Orbitが[グループで有効化されている](../getting-started.md)こと。
- `glab`がインストールされ、認証済みであること:

  ```shell
  glab auth login
  ```

- ユーザーがGitLab Orbitを有効にした少なくとも1つのトップレベルグループにアクセスできること。

## AIエージェントをセットアップする {#set-up-your-ai-agent}

`glab orbit setup`は、AIコーディングエージェント（Claude Code、OpenCode、Cursor、Codex、Gemini CLI）がグラフを参照できるよう設定し、GitLab Orbitスキルをインストールします。

```shell
glab orbit setup
```

MCPクライアントを接続する場合は、[手動で設定してください](mcp.md#connect-your-mcp-client)。

<!-- markdownlint-disable-next-line MD044 -->
## コマンドラインからGitLab Orbitにクエリを実行する {#query-gitlab-orbit-from-the-command-line}

`glab orbit remote`を使用して、GitLab Orbit Remote APIを直接呼び出します。スクリプト作成、デバッグ、クエリ作成前のスキーマ調査に役立ちます。`glab` 1.94以降が必要です。

`glab`が認証情報を解決してバイナリに渡すため、追加の認証手順は不要です。`--hostname`で特定のGitLabインスタンスを指定し、`--yes`でスクリプト内のワンタイム実行確認をスキップできます。

| サブコマンド | エンドポイント | 目的 |
|------------|----------|---------|
| `glab orbit remote status` | `GET orbit/status` | クラスターの正常性確認。 |
| `glab orbit remote schema [node...]` | `GET orbit/schema` | グラフオントロジー。位置引数で特定のノードを展開します。 |
| `glab orbit remote dsl` | `GET orbit/schema/dsl` | クエリDSL JSONスキーマ。クエリボディの形式に関する信頼できる情報源です。 |
| `glab orbit remote tools` | `GET orbit/tools` | 完全なDSL JSONスキーマを含むMCPツールマニフェスト。 |
| `glab orbit remote query [file\|-]` | `POST orbit/query` | ファイルまたは標準入力からクエリを実行します。 |
| `glab orbit remote graph-status` | `GET orbit/graph_status` | ネームスペース、プロジェクト、またはフルパスのインデックス作成の進捗状況。 |

### スキーマを確認する {#discover-the-schema}

```shell
glab orbit remote status
glab orbit remote schema
glab orbit remote schema MergeRequest Project
glab orbit remote dsl
glab orbit remote tools
```

### クエリを実行する {#run-a-query}

`your-group`を実際のグループパスに置き換えてください。このクエリはそのグループの最初の5つのプロジェクトを返します。

リクエストボディを`query.json`に記述します。

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 5
  }
}
```

```shell
glab orbit remote query query.json
```

`--response-format`フラグはリクエストボディの`response_format`にマップされます。

- `--response-format llm` - AIエージェントの処理に最適化されたコンパクトなテキスト形式。
- `--response-format raw` - `jq`へのパイプに適した構造化されたJSON形式。

`--response-format`が未設定の場合、ボディの`response_format`が優先され、最終的なフォールバックとして`llm`が使用されます。

### インデックス作成の進捗状況を確認する {#check-indexing-progress}

スコープフラグをいずれか1つ指定してください。

```shell
glab orbit remote graph-status --full-path your-group/your-project
glab orbit remote graph-status --namespace-id 24
glab orbit remote graph-status --project-id 2
```

## 終了コード {#exit-codes}

`glab orbit remote`はHTTPエラーを安定した終了コードにマップするため、スクリプトやエージェントはstderrを解析せずに分岐処理を行えます。

| ステータス | 終了コード | 意味 |
|--------|-----------|---------|
| `200` | `0` | 成功。 |
| `404` | `2` | `knowledge_graph`機能フラグがオフ、またはパスのタイポ。 |
| `401` | `3` | トークンが存在しないか期限切れ。 |
| `403` | `4` | Knowledge Graphが有効なネームスペースが存在しない。 |
| `429` | `5` | レート制限。`Retry-After`を確認してバックオフしてください。 |
| その他 | `1` | 非構造化エラー。レスポンスボディがある場合は含まれます。 |

## 課金 {#billing}

`glab orbit remote query`はMCPクエリと同様にGitLabクレジットを消費します。`status`、`schema`、`tools`、`graph-status`の呼び出しは無料です。
