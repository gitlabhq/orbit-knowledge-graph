---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit CLIやGitLab Orbit Local MCPサーバーなどのツールを使用して、ローカルグラフを操作します。
title: ツールを接続する
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。
- GitLab 19.1で[ベータ版](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更されました](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)。

{{< /history >}}

GitLab Orbit Localは、ローカルグラフへのアクセスやコーディングエージェントとの連携に使用できるツールを提供します。

<!-- markdownlint-disable-next-line MD044 -->
## GitLab Orbit CLIを使用したGitLab Orbit Local {#gitlab-orbit-local-with-the-gitlab-orbit-cli}

GitLab Orbit CLIを使用すると、GitLabインスタンスへの接続なしにローカルグラフをビルドしてクエリできます。

また、GitLab Orbit Local MCPサーバーを使用してグラフをAIエージェントに公開し、AIコーディングアシスタントがグラフを参照するよう設定することもできます。

<!-- markdownlint-disable-next-line MD044 -->
## GitLab CLIを使用したGitLab Orbit Local {#gitlab-orbit-local-with-the-gitlab-cli}

GitLab CLIを拡張して、GitLab Orbit CLIコマンドをインストールして実行します。GitLab CLIはアップデートの管理も行い、クエリレシピ、DSLガイダンス、トラブルシューティングヘルプでエージェントを更新するGitLab Orbitスキルをインストールします。

<!-- markdownlint-disable-next-line MD044 -->
## GitLab Orbit Local MCPサーバー {#gitlab-orbit-local-mcp-server}

GitLab Orbit Local MCPサーバーをAIクライアントに接続します。クライアントへの接続後、ローカルグラフと連携するようエージェントを設定できます。
