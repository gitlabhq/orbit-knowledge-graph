---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: glab orbit remoteを使用してコマンドラインからOrbitにクエリを実行できます。glab 1.94以降で利用可能です。glab orbit setupヘルパーは将来のglabリリースで提供予定です。
title: GitLab CLI（`glab`）でOrbitを使用する
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- 機能フラグ`knowledge_graph`を使用して、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました（[機能フラグあり](https://docs.gitlab.com/administration/feature_flags/)）。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階にあります。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴をご参照ください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

<!-- -->

> [!disclaimer]

[GitLab CLI（`glab`）](https://docs.gitlab.com/cli/)は、コマンドラインからOrbitをセットアップおよびクエリするための標準的な方法です。

トップレベルコマンドは2つあります。

- `glab orbit remote`: Orbit Remote REST APIを呼び出す型付きサブコマンドです。
  `glab` 1.94以降で利用可能です。
- `glab orbit setup`: OrbitスキルとMCP設定をAIエージェントに一括インストールするコマンドです。将来の`glab`リリースで提供予定です。リリースまでの間は、[MCPクライアントを手動で設定](mcp.md#connect-your-mcp-client)してください。

## 前提条件 {#prerequisites}

- Orbitが[グループで有効化](../getting-started.md)されていること。
- `glab`がインストールされ、認証済みであること:

  ```shell
  glab auth login
  ```

- ユーザーがOrbitを有効にした少なくとも1つのトップレベルグループにアクセスできること。

## AIエージェントをセットアップする {#set-up-your-ai-agent}

`glab orbit setup`は将来の`glab`リリースで提供予定です。リリース後は、1つのコマンドでOrbitスキルのインストールとAIエージェント（Claude Code、OpenCode、Cursor、Codex、Gemini CLI）向けのMCP設定の書き込みが行えるようになります。

リリースまでの間は、[MCPクライアントを手動で設定](mcp.md#connect-your-mcp-client)してください。

## コマンドラインからOrbitにクエリを実行する {#query-orbit-from-the-command-line}

`glab orbit remote`（または`r`エイリアス）を使用して、Orbit Remote APIを直接呼び出します。スクリプト作成、デバッグ、クエリ作成前のスキーマ探索に役立ちます。`glab` 1.94以降が必要です。

| サブコマンド | エンドポイント | 目的 |
|------------|----------|---------|
| `glab orbit remote status` | `GET orbit/status` | クラスターの正常性確認。 |
| `glab orbit remote schema [node...]` | `GET orbit/schema` | グラフオントロジー。位置引数で特定のノードを展開します。 |
| `glab orbit remote tools` | `GET orbit/tools` | 完全なDSL JSONスキーマを含むMCPツールマニフェスト。 |
| `glab orbit remote query [file\|-]` | `POST orbit/query` | ファイルまたは標準入力からクエリを実行します。 |
| `glab orbit remote graph-status` | `GET orbit/graph_status` | ネームスペース、プロジェクト、またはフルパスのインデックス作成の進捗状況。 |

### スキーマを確認する {#discover-the-schema}

```shell
glab orbit remote status
glab orbit remote schema
glab orbit remote schema MergeRequest Project
glab orbit remote tools
```

### クエリを実行する {#run-a-query}

```shell
echo '{"query":{"query_type":"traversal","node":{"id":"p","entity":"Project","filters":{"full_path":{"op":"starts_with","value":"your-group/"}}},"limit":5}}' \
  | glab orbit remote query -
```

`--format`フラグはリクエストボディの`response_format`にマップされます。

- `--format llm` - AIエージェントの利用に最適化されたコンパクトなテキスト形式。
- `--format raw` - `jq`へのパイプ処理に適した構造化されたJSON形式。

`--format`が未設定の場合、ボディの`response_format`が優先され、最終的なフォールバックとして`llm`が使用されます。

### インデックス作成の進捗状況を確認する {#check-indexing-progress}

スコープフラグを1つだけ指定してください。

```shell
glab orbit remote graph-status --full-path gitlab-org/gitlab
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
| その他 | `1` | 非構造化エラー。レスポンスボディが存在する場合は含まれます。 |

## 課金 {#billing}

`glab orbit remote query`はMCPクエリと同様にGitLab Creditsを消費します。`status`、`schema`、`tools`、`graph-status`の呼び出しは無料です。
