---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbitスキルをインストールして、AIコーディングエージェントにOrbit RemoteとOrbit Localの両方に対応したすぐに使えるクエリレシピ、DSLガイダンス、トラブルシューティングを提供します。
title: Orbitスキルを使用してAIコーディングエージェントをセットアップする
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: 実験

{{< /details >}}

OrbitスキルはAIコーディングエージェントに対し、GitLab Knowledge Graphをクエリするための体系的なガイダンスを提供します。含まれる内容は以下のとおりです。

- **クエリレシピ** - よくある質問（ブラスト半径、パイプラインの履歴、コントリビューターのパターンなど）に対応した、そのまま貼り付けて使えるJSONボディ
- **DSLリファレンス** - エージェントが初回から有効なクエリを作成できるよう、クエリ言語の完全な仕様を収録
- **トラブルシューティング** - 終了コード、空の結果の診断、よくある落とし穴
- **リポジトリマップヘルパー** - ローカルチェックアウトまたはOrbit Remoteからコードベースの構造を要約するスクリプト

このスキルは[Orbit Remote](remote/_index.md)と[Orbit Local](local/_index.md)の両方に対応しています。

## 前提条件 {#prerequisites}

- [GitLab CLI（`glab`）](https://docs.gitlab.com/cli/) v1.95.0以降（`glab skills install`が導入されたバージョン）。サブコマンドが認識されない場合は、先に`glab`をアップデートしてください。

## スキルをインストールする {#install-the-skill}

グローバルにインストールする場合（すべてのプロジェクトで使用可能）:

```shell
glab skills install --global orbit
```

これにより、スキルが`~/.agents/skills/orbit`にインストールされます。

現在のプロジェクトのみにインストールする場合:

```shell
glab skills install orbit
```

これにより、スキルがプロジェクトルートの`.agents/skills/orbit`にインストールされます。

スキルがすでにインストールされている場合、`glab`は`SKILL.md`が存在することを報告し、上書きするには`--force`を使用するよう提案します。

## スキルをアップデートする {#update-the-skill}

最新バージョンにアップデートするには、`--force`オプションを付けてインストールコマンドを再実行します。

```shell
glab skills install --global --force orbit
```
