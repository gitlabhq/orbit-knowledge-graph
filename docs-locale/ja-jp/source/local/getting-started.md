---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: アクセス方法を選択して、最初のローカルOrbitグラフを構築します。
title: Orbit Localを使ってみる
---

{{< details >}}

- ティア: Free, Premium, Ultimate
- 提供形態: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

Orbit Localはお使いのマシン上で動作します。`orbit`バイナリをインストールし、作業スタイルに合ったアクセス方法を選択して、最初のクエリを実行してください。

## インストール {#install}

`orbit`バイナリをワンラインインストーラーで直接インストールするか、すでにGitLab CLI（`glab`）を使用している場合はそちらを通じてインストールできます。

{{< tabs >}}

{{< tab title="macOSおよびLinux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

新しいターミナルを開いて、インストールを確認します。

```shell
orbit help
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
```

新しいターミナルを開いて、インストールを確認します。

```shell
orbit help
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab)" >}}

[`glab`](https://gitlab.com/gitlab-org/cli)がすでにインストールされている場合:

```shell
glab orbit local --install
```

確認:

```shell
glab orbit local help
```

詳細については、[`glab orbit local`リファレンス](https://docs.gitlab.com/cli/orbit/local/)を参照してください。

{{< /tab >}}

{{< /tabs >}}

## アクセス方法の選択 {#pick-an-access-method}

| 方法 | 最適な用途 | セットアップ |
|---|---|---|
| [Orbit CLI（`orbit`）](access/cli.md) | CLIの直接使用、スクリプト作成、インデックス作成タスク | ソースからバイナリをビルド |
| [GitLab CLI（`glab`）](access/glab.md) | `glab`をすでに使用しているユーザー、ワンコマンドのAIエージェントセットアップ | `glab orbit local`（予定）- 現時点では`orbit`を直接使用 |
| [MCP](access/mcp.md) | Claude Code、Codex、その他のAIエージェント | 手動MCPの設定、`glab orbit setup`は予定中 |

クエリ言語は3つすべてで同一です。1つで学んだことはそのまま他にも応用でき、[Orbit Remote](../remote/_index.md)にも活用できます。

## 60秒クイックスタート {#60-second-quickstart}

> [!note]
> `glab orbit local`は予定されているパッケージングパスです。リリースされるまでは、`orbit`バイナリを直接使用してください。[`orbit` CLIを直接使用する](access/cli.md)を参照してください。
> 以下に示す形式は、`glab orbit local`がサポートする予定のものです。

リポジトリをインデックス化して、Orbitが検出した内容を確認します。

```shell
glab orbit local index /path/to/your/repo
glab orbit local schema
```

これにより、`~/.orbit/graph.duckdb`にローカルDuckDBグラフが構築され、ノードタイプ（`Definition`、`File`、`Directory`、`ImportedSymbol`）が出力されます。

次のステップ:

- 実際のクエリを実行する: [glabでOrbit Localを使用する](access/glab.md)。
- AIエージェントに接続する: 手動設定については[MCPで接続する](access/mcp.md)を参照してください。（`glab orbit setup`による自動化は予定中です。）
- テーブルレイアウトを確認する: [スキーマリファレンス](schema.md)。

## 課金 {#billing}

Orbit LocalはGitLab Creditsを消費しません。すべての処理はローカルで行われます。

## 次に試すこと {#what-to-try-next}

- [Orbit Localがインデックス化する内容](indexing.md) - 言語とカバレッジの範囲。
- [スキーマリファレンス](schema.md) - ローカルグラフの4つのノードタイプ。
- [Cookbook](../remote/cookbook.md) - コピー＆ペーストで使えるクエリ（コードのみのものはLocalにも適用可能）。
- [Orbit Remoteを使ってみる](../remote/getting-started.md) - GitLabインスタンス全体をクエリする。
