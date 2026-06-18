---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit LocalとOrbit Remoteの一般的なエラーのトラブルシューティング。
title: Orbitのトラブルシューティング
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 19.1で[導入](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/661)されました。

{{< /history >}}

このページでは、[Orbit Local](local/_index.md)または[Orbit Remote](remote/_index.md)で発生する可能性のあるエラーのトラブルシューティング方法を説明します。

## Orbit Local {#orbit-local}

Orbit Localのエラーは、`orbit`バイナリを直接実行するか、`glab orbit local`を通じて実行する際に発生します。

### `no local graph found`

**症状:**

```plaintext
Error: no local graph found at ~/.orbit/graph.duckdb. Run `orbit index` first.
```

**原因:** リポジトリがまだインデックス作成されていないか、指定した`--db`パスが存在しません。古いバージョンのOrbit Localでは、このエラーは`Table 'Definition' does not exist`として報告されていました。

**解決策:** 最初にリポジトリのインデックスを作成してください:

```shell
glab orbit local index /path/to/your/repo
```

### `IO Error: Could not set lock on file`

**症状:** コマンドが一時的に停止したように見えた後、`Could not set lock on file`を含むエラーで失敗します。

**原因:** 別の`orbit`プロセスがすでに実行中で、DuckDBの書き込みロックを保持しています。Orbitは指数バックオフで自動的に再試行しますが、リトライウィンドウ内にロックが解放されない場合は失敗します。

**解決策:** 他のプロセスが終了するまで待つか、停止してください:

```shell
pkill orbit
```

その後、コマンドを再試行してください。

### `list_contains source_tags`

**症状:** `list_contains source_tags`を含むエラーでクエリが失敗します。

**原因:** `source_tags`プロパティを含む特定のフィルターの組み合わせによって引き起こされる既知のバグです。

**解決策:** クエリから`source_tags`フィルターを削除して再試行してください。

### `error: unrecognized subcommand 'mcp'`

**症状:**

```plaintext
error: unrecognized subcommand 'mcp'
```

**原因:** `orbit mcp serve`サブコマンドはまだ実装されていません。Orbit LocalのMCPサポートはロードマップに含まれていますが、現在のリリースでは利用できません。

**解決策:** [サポートされているアクセス方法](local/_index.md)のいずれかを使用してください。

## Orbit Remote {#orbit-remote}

Orbit Remoteのエラーは、`glab orbit remote`コマンドを実行する際に発生します。Orbit RemoteにはGitLab PremiumまたはUltimateと、インスタンスで有効化された`knowledge_graph`機能フラグが必要です。

### 終了コード2 {#exit-code-2}

**症状:** `glab orbit remote`コマンドがコード2で終了します。

**原因:** `knowledge_graph`機能フラグがネームスペースまたはインスタンスで有効化されていません。

**解決策:** GitLab管理者に連絡して、ネームスペースの`knowledge_graph`機能フラグを有効化してもらってください。

### 終了コード3 {#exit-code-3}

**症状:** `glab orbit remote`コマンドがコード3で終了します。

**原因:** GitLab CLIで認証されていません。

**解決策:** ログインしてください:

```shell
glab auth login
```

### MCPエンドポイントでの`insufficient_scope` {#on-the-mcp-endpoint}

**症状:** OrbitのMCPエンドポイントへの接続が`insufficient_scope`で失敗します。

**原因:** パーソナルアクセストークンまたはOAuthトークンに`mcp_orbit`スコープが含まれていません。MCPトランスポートには`read_api`スコープだけでは不十分です。

**解決策:** `mcp_orbit`スコープを持つ新しいトークンを作成するか、追加のスコープを付与するために再認証してください。
