---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: アクセス方法を選択して、最初のローカルOrbitグラフを構築します。
title: Orbit Localを使ってみる
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

Orbit Localはお使いのマシン上で動作します。`orbit`バイナリをインストールし、作業スタイルに合ったアクセス方法を選択して、最初のクエリを実行してください。

## インストール {#install}

`orbit`バイナリは、ワンラインインストーラーを使って直接インストールするか、npmから、またはすでに使用している場合はGitLab CLI（`glab`）経由でインストールできます。

Linuxでは、インストーラーはデフォルトでglibcアーカイブを使用し、Alpineなどのmuslベースのディストリビューションでは完全静的なmuslアーカイブを自動的に選択します。静的Linuxアーカイブを強制するには、`--libc musl`を渡してください。

{{< tabs >}}

{{< tab title="macOS and Linux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

静的muslバイナリを明示的にインストールする場合（例: glibcシステム上）:

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --libc musl
```

新しいターミナルを開いて、確認します:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
```

新しいターミナルを開いて、確認します:

```shell
orbit help
```

エンドポイントポリシーによりリモートスクリプトの実行が制限されている場合は、スクリプトを実行せずにインストールできます。Windowsリリースアーカイブには、GitLab Inc.によって署名された単一の自己完結型`orbit.exe`が含まれているため、アプリケーション許可リストポリシーで発行者による承認が可能です:

1. [最新リリース](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases)から、`orbit-local-windows-x86_64.zip`とその`.sha256`ファイルをダウンロードします。
1. チェックサムを確認します:

   ```powershell
   (Get-FileHash .\orbit-local-windows-x86_64.zip -Algorithm SHA256).Hash
   ```

   出力は`.sha256`ファイルのハッシュと一致する必要があります。比較は大文字・小文字を区別しません: `Get-FileHash`は大文字を返し、`.sha256`ファイルは小文字で保存されます。
1. Mark of the Webをクリアします。ブラウザがダウンロード時に付加し、展開時に`orbit.exe`に引き継がれるため、初回実行時にSmartScreenのプロンプトが表示されたり、ポリシーによってブロックされたりする場合があります:

   ```powershell
   Unblock-File .\orbit-local-windows-x86_64.zip
   ```

1. アーカイブを展開し、ターゲットディレクトリを作成して、`orbit.exe`を移動します。この例では、インストーラーが使用するデフォルトと同じ`$env:LOCALAPPDATA\Programs\orbit`を使用します:

   ```powershell
   Expand-Archive -Path .\orbit-local-windows-x86_64.zip -DestinationPath .
   New-Item -ItemType Directory -Force -Path "$env:LOCALAPPDATA\Programs\orbit"
   Move-Item .\orbit.exe "$env:LOCALAPPDATA\Programs\orbit\orbit.exe"
   ```

1. そのディレクトリがまだ`PATH`に含まれていない場合は、ユーザー用に追加します:

   ```powershell
   [Environment]::SetEnvironmentVariable("PATH", "$env:LOCALAPPDATA\Programs\orbit;$([Environment]::GetEnvironmentVariable('PATH', 'User'))", "User")
   ```

管理者権限は不要です。

新しいターミナルを開いて、確認します:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="npm" >}}

任意のプラットフォームでnpmからインストールすることもできます:

```shell
npm install -g @gitlab/orbit
```

[`@gitlab/orbit`](https://www.npmjs.com/package/@gitlab/orbit)パッケージは、お使いのプラットフォーム向けのビルド済みバイナリをインストールします。Linuxでは、glibcとmuslの両方のディストリビューションで動作する完全静的なmuslバイナリが常に使用されます。

確認します:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab)" >}}

[`glab`](https://gitlab.com/gitlab-org/cli)がすでにインストールされている場合:

```shell
glab orbit local --install
```

確認します:

```shell
glab orbit local help
```

詳細については、[`glab orbit local`リファレンス](https://docs.gitlab.com/cli/orbit/local/)を参照してください。

{{< /tab >}}

{{< /tabs >}}

## アクセス方法を選択する {#pick-an-access-method}

| 方法 | 最適な用途 | セットアップ |
|---|---|---|
| [Orbit CLI（`orbit`）](access/cli.md) | CLIの直接使用、スクリプト作成、インデックス作成タスク | ワンラインインストーラーまたは`glab orbit local --install` |
| [GitLab CLI（`glab`）](access/glab.md) | すでに`glab`を使用している方 | `glab orbit local --install` |
| [MCP](access/mcp.md) | Claude Code、Codex、その他のAIエージェント | `claude mcp add orbit-local -- orbit mcp serve` |

3つすべてが同じローカルグラフを参照します。Orbit LocalはDuckDB SQLでクエリを実行します。構造化JSONクエリDSLは[Orbit Remote](../remote/_index.md)専用です。

## 60秒クイックスタート {#60-second-quickstart}

> [!note]
> `glab orbit local`は管理された`orbit`バイナリをラップします。バイナリは初回使用時にダウンロードされ、チェックサムで検証され、最新の状態に保たれます。`glab` 1.94以降が必要です。バイナリを直接実行する場合は、[`orbit` CLIを直接使用する](access/cli.md)を参照してください。

リポジトリのインデックスを作成して、Orbitが検出した内容を確認します:

```shell
glab orbit local index /path/to/your/repo
glab orbit local schema
```

これにより、`~/.orbit/graph.duckdb`にローカルDuckDBグラフが構築され、すべてのテーブルとカラムが表示されます: `gl_definition`、`gl_file`、`gl_directory`、`gl_imported_symbol`、`gl_edge`、および`_orbit_manifest`管理テーブル。

次のステップ:

- 実際のクエリを実行する: [glabでOrbit Localを使用する](access/glab.md)。
- AIエージェントに接続する: `glab orbit setup`を実行してOrbitスキルをインストールするか、手動設定については[MCPで接続する](access/mcp.md)を参照してください。
- テーブルレイアウトを確認する: [スキーマリファレンス](schema.md)。

## 課金 {#billing}

Orbit LocalはGitLabクレジットを消費しません。すべての処理はローカルで行われます。

## 次に試すこと {#what-to-try-next}

- [Orbit Localのインデックス対象](indexing.md) - 対応言語とカバレッジの範囲。
- [スキーマリファレンス](schema.md) - ローカルグラフの4つのノードタイプ。
- [Cookbook](../remote/cookbook.md) - コピー＆ペーストで使えるクエリ集（コードのみのクエリはLocalにも適用可能）。
- [Orbit Remoteを使ってみる](../remote/getting-started.md) - GitLabインスタンス全体をクエリする。
