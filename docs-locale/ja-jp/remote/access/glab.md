I'll follow the translation workflow:

1. First, check for existing translations
2. Load resources
3. Check translation memory
4. Translate

Let me start by checking if a translated version already exists and loading the required resources.

**Step 1: Check for existing translation**

The target file would be at: `docs/source/remote/access/glab.md` in the `ja-jp` locale directory. Since this appears to be a new translation request with no locale directory prefix shown, I'll proceed to load resources.

**Step 2 & 3: Load resources and check translation memory**

I'll now load the Japanese translation rules and termbase, then proceed with the translation.

Based on the loaded resources (termbase, translation rules, markdown parsing rules), here is the complete translated file:

---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: glab orbit remoteを使用してコマンドラインからOrbitをクエリします。glab 1.94以降で利用可能です。glab orbit setupヘルパーは将来のglabリリースで提供予定です。
title: GitLab CLI（`glab`）でOrbitを使用する
---

{{< details >}}

- Tier: Premium, Ultimate
- Offering: GitLab.com
- Status: Experiment

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)を使用して、GitLab 18.10で[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階です。

{{< /history >}}

> [!flag]
> この機能の利用可能性は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト用に利用可能ですが、本番環境での使用には対応していません。

<!-- -->

> [!disclaimer]

[GitLab CLI（`glab`）](https://docs.gitlab.com/cli/)は、コマンドラインからOrbitをセットアップおよびクエリするための標準的な方法です。

トップレベルコマンドは2つあります：

- `glab orbit remote`：Orbit Remote REST APIを呼び出す型付きサブコマンドです。
  `glab` 1.94以降で利用可能です。
- `glab orbit setup`：OrbitスキルとMCP設定をAIエージェントに一括インストールするコマンドです。
  将来の`glab`リリースで提供予定です。リリースまでの間は、
  [MCPクライアントを手動で設定してください](mcp.md#connect-your-mcp-client)。

## 前提条件 {#prerequisites}

- Orbitが[グループで有効化されています](../getting-started.md)。
- `glab`がインストールされ、認証済みです：

  ```shell
  glab auth login
  ```

- ユーザーがOrbitが有効化されたトップレベルグループに少なくとも1つアクセスできます。

## AIエージェントをセットアップする {#set-up-your-ai-agent}

`glab orbit setup`は将来の`glab`リリースで提供予定です。リリース後は、1つのコマンドでOrbitスキルをインストールし、AIエージェント（Claude Code、OpenCode、Cursor、Codex、Gemini CLI）のMCP設定を書き込むことができます。

リリースまでの間は、[MCPクライアントを手動で設定してください](mcp.md#connect-your-mcp-client)。

## コマンドラインからOrbitをクエリする {#query-orbit-from-the-command-line}

`glab orbit remote`（または`r`エイリアス）を使用して、Orbit Remote APIを直接呼び出します。
スクリプト作成、デバッグ、クエリを書く前のスキーマ探索に役立ちます。
`glab` 1.94以降が必要です。

| サブコマンド | エンドポイント | 目的 |
|------------|----------|---------|
| `glab orbit remote status` | `GET orbit/status` | クラスターの正常性。 |
| `glab orbit remote schema [node...]` | `GET orbit/schema` | グラフオントロジー。位置引数で特定のノードを展開します。 |
| `glab orbit remote tools` | `GET orbit/tools` | 完全なDSL JSONスキーマを含むMCPツールマニフェスト。 |
| `glab orbit remote query [file\|-]` | `POST orbit/query` | ファイルまたは標準入力からクエリを実行します。 |
| `glab orbit remote graph-status` | `GET orbit/graph_status` | ネームスペース、プロジェクト、またはフルパスのインデックス作成の進捗状況。 |

### スキーマを探索する {#discover-the-schema}

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

`--format`フラグはボディの`response_format`にマップされます：

- `--format llm` - AIエージェントの利用に最適化されたコンパクトなテキスト。
- `--format raw` - `jq`へのパイプに適した構造化されたJSON。

`--format`が未設定の場合、ボディの`response_format`が優先され、最終的なフォールバックとして`llm`が使用されます。

### インデックス作成の進捗状況を確認する {#check-indexing-progress}

スコープフラグを1つだけ指定してください：

```shell
glab orbit remote graph-status --full-path gitlab-org/gitlab
glab orbit remote graph-status --namespace-id 24
glab orbit remote graph-status --project-id 2
```

## 終了コード {#exit-codes}

`glab orbit remote`はHTTPエラーを安定した終了コードにマップするため、スクリプトやエージェントはstderrを解析せずに分岐できます。

| ステータス | 終了コード | 意味 |
|--------|-----------|---------|
| `200` | `0` | 成功。 |
| `404` | `2` | `knowledge_graph`機能フラグがオフ、またはパスのタイポ。 |
| `401` | `3` | トークンが存在しないか期限切れです。 |
| `403` | `4` | Knowledge Graphが有効なネームスペースが利用できません。 |
| `429` | `5` | レート制限。`Retry-After`を確認してバックオフしてください。 |
| その他 | `1` | 非構造化エラー。レスポンスボディがある場合は含まれます。 |

## 課金 {#billing}

`glab orbit remote query`はMCPクエリと同様にGitLab Creditsを消費します。
`status`、`schema`、`tools`、`graph-status`の呼び出しは無料です。
