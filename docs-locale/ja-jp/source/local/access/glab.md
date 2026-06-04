---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: glab orbit localサブコマンドとglab orbit setupは、将来のglabリリースで提供予定です。リリースまでの間は、ソースからビルドしてorbitバイナリを直接使用してください。
title: GitLab CLI（`glab`）でOrbit Localを使用する
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)されました。

{{< /history >}}

> [!disclaimer]

[GitLab CLI（`glab`）](https://docs.gitlab.com/cli/)は、Orbit Localのインストール、実行、およびAIエージェントとの統合に推奨される標準的な方法です。`glab orbit local`は`glab orbit remote`と同じ構造を持つため、GitLabインスタンスまたはローカルマシンのどちらに対してクエリを実行する場合でも、同じパターンを使用できます。

> [!note]
> `glab orbit local`と`glab orbit setup`はいずれも将来のglabリリースで提供予定であり、現時点では利用できません。このページに記載されているコマンドは将来の仕様であり、現在の仕様ではありません。リリースまでの間は、ソースからビルドしてください。詳細は[`orbit`を直接使用する](cli.md)を参照してください。

トップレベルコマンドは2つあります（いずれも予定中で、未リリース）:

- `glab orbit setup`: Orbitスキルをインストールし、AIエージェントをローカルグラフに接続します。
- `glab orbit local`: `orbit`バイナリをラップする型付きサブコマンドです。`glab orbit local mcp serve`を使用してOrbit LocalをMCPサーバーとして実行できます。

## 前提条件 {#prerequisites}

- `glab`がインストールされ、認証済みであること:

  ```shell
  glab auth login
  ```

- インデックスを作成するローカルGitリポジトリ。

バイナリがインストールされていれば、`glab orbit local`の使用にGitLabアカウントやネットワーク接続は不要です。

## AIエージェントを設定する {#set-up-your-ai-agent}

> [!note]
> `glab orbit setup`は予定中であり、まだリリースされていません。リリースまでの間は、[MCPクライアントを手動で設定](mcp.md#manual-config-claude-code)してください。

リリース後、`glab orbit setup`は1つのコマンドでOrbitスキルをインストールし、MCP設定を書き込みます。**Local**または**Remote**を選択するプロンプトが表示され、エージェントが自動検出されます。

```shell
glab orbit setup
# ローカルグラフにMCP設定を向けるには、プロンプトで「Local」を選択してください。
```

対応エージェント: Claude Code、OpenCode、Cursor、Codex、Gemini CLI。

| フラグ | 説明 |
|------|---------|
| `--agent=<name>` | 自動検出を上書きします。 |
| `--skill-only` | スキルファイルのみをインストールし、MCP設定をスキップします。 |
| `--mcp-only` | MCP設定のみを書き込み、スキルのインストールをスキップします。 |
| `--dry-run` | 何も書き込まずに変更内容を表示します。 |

MCP設定はリモートエンドポイントではなく`orbit mcp serve`を指します。エージェントはローカルのDuckDBグラフに対して`query_graph`と`get_graph_schema`を呼び出せます。

また、`glab skills install --global orbit`を使用して、今すぐ[Orbitスキルを手動でインストール](../../ai_coding_agents.md)することもできます。

## リポジトリのインデックスを作成する {#index-a-repository}

```shell
glab orbit local index /path/to/your/repo
```

| フラグ | 説明 |
|------|---------|
| `--threads` | ワーカースレッド数。`0`（デフォルト）はCPUコア数から自動検出します。 |
| `--stats` | JSON出力に詳細な統計情報を含めます。 |
| `--verbose` | stderrへの詳細ログを有効にします。 |

## グラフに対してSQLを実行する {#run-sql-against-the-graph}

```shell
glab orbit local sql 'SELECT count(*) FROM gl_definition'
echo 'SELECT name FROM gl_definition LIMIT 3' | glab orbit local sql -
```

## スキーマを確認する {#inspect-the-schema}

```shell
glab orbit local schema
glab orbit local schema --raw
```

## MCPサーバーとして実行する {#run-as-an-mcp-server}

ローカルグラフをMCP対応のAIエージェントに公開します:

```shell
glab orbit local mcp serve
```

これにより、`~/.orbit/graph.duckdb`に対してMCPプロトコル経由で`query_graph`と`get_graph_schema`が提供されます。エージェントとの完全な統合ガイドについては、[MCPで接続する](mcp.md)を参照してください。

## インデックス済みリポジトリを一覧表示する {#list-indexed-repositories}

```shell
glab orbit local status
```

ローカルグラフに存在するリポジトリ、そのインデックス状態、およびデータベースパスが表示されます。

## 終了コード {#exit-codes}

`glab orbit local`はエラーを安定した終了コードにマッピングするため、スクリプトやエージェントで分岐処理が可能です。

| ステータス | 終了コード | 意味 |
|--------|-----------|---------|
| 成功 | `0` | コマンドが完了しました。 |
| グラフなし | `2` | `~/.orbit/graph.duckdb`が見つかりません。先に`index`を実行してください。 |
| クエリエラー | `4` | クエリDSLの検証またはコンパイルに失敗しました。 |
| その他 | `1` | 非構造化エラー。詳細はstderrを確認してください。 |

## 課金 {#billing}

Orbit LocalはGitLab Creditsを消費しません。すべての処理はローカルで行われます。
