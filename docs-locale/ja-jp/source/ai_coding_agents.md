---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbitスキルをインストールして、AIコーディングエージェントにGitLab Orbit RemoteとGitLab Orbit Localの両方で使用できるクエリレシピ、DSLガイダンス、トラブルシューティングを提供します。
title: GitLab Orbitスキルを使用してAIコーディングエージェントをセットアップする
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: ベータ版

{{< /details >}}

GitLab OrbitスキルはAIコーディングエージェントに、GitLab Orbitグラフをクエリするための体系的なガイダンスを提供します。含まれる内容は以下のとおりです。

- **クエリレシピ** - よくある質問（影響範囲、パイプライン履歴、コントリビューターのパターン）に対応した、そのまま使えるJSONボディ。
- **DSLリファレンス** - エージェントが初回から正しいクエリを作成できるよう、クエリ言語の完全な仕様を収録。
- **トラブルシューティング** - 終了コード、空の結果の診断、よくある落とし穴。
- **リポジトリマップヘルパー** - ローカルチェックアウトまたはGitLab Orbit Remoteからコードベース構造を要約するスクリプト。

このスキルは[GitLab Orbit Remote](remote/_index.md)と[GitLab Orbit Local](local/_index.md)の両方に対応しています。

## 前提条件 {#prerequisites}

- [GitLab CLI（`glab`）](https://docs.gitlab.com/cli/) v1.95.0以降（`glab skills install`が導入されたバージョン）。サブコマンドが認識されない場合は、先に`glab`をアップデートしてください。

## スキルをインストールする {#install-the-skill}

グローバルにインストールする（すべてのプロジェクトで使用可能）:

```shell
glab skills install --global orbit
```

スキルは`~/.agents/skills/orbit`にインストールされます。

現在のプロジェクトのみにインストールする:

```shell
glab skills install orbit
```

スキルはプロジェクトルートの`.agents/skills/orbit`にインストールされます。

スキルがすでにインストールされている場合、`glab`は`SKILL.md`が存在することを報告し、上書きするには`--force`を使用するよう提案します。

<!-- markdownlint-disable-next-line MD044 -->
## GitLab Orbitスキルをアップデートする {#update-the-gitlab-orbit-skill}

最新バージョンにアップデートするには、`--force`を付けてインストールコマンドを再実行します。

```shell
glab skills install --global --force orbit
```
