---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Duo Agent Platformを通じてOrbitを使用します。エージェントはOrbitのグラフツールを呼び出し、GitLab Duo Agent、Planner Agent、Security Analyst Agent、Data Analyst Agent、CI Expert Agent、Developer Flowにわたって、ライブのGitLabデータに基づいた回答を提供します。
title: GitLab Duo Agent PlatformでOrbitを使用する
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

OrbitはGitLab Duo Agent Platformに統合されています。エージェントは、クロスプロジェクトの依存関係、影響範囲、パイプラインの継承、脆弱性の系譜、コントリビューターのパターンなど、SDLCグラフをトラバースすることで最適に回答できる質問に対して、Orbitのグラフツール（`get_graph_schema`、`query_graph`）を自動的に呼び出します。Orbitが回答を持っていない場合、エージェントは既存のツールにフォールバックします。

## 前提条件 {#prerequisites}

- Orbitが[グループで有効化](../getting-started.md)されている。
- [GitLab Duo Agent Platform](https://docs.gitlab.com/user/duo_agent_platform/)へのアクセス権がある。

## Orbitが利用可能な場所 {#where-orbit-is-available}

Orbitは以下のGitLab Duo Agent Platformのエージェントおよびフローに組み込まれています。

| エージェントまたはフロー | 使用するタイミング |
|---|---|
| GitLab Duo Agent | 汎用開発アシスタント。コード、計画、セキュリティ、プロジェクト管理に関するサポートを提供します。グラフコンテキストから回答が得られる場合にOrbitを呼び出します。 |
| Planner Agent | イシューとマイルストーンの計画。作業アイテムのオーナーシップ、ブロッカー、コントリビューターの負荷、プロジェクト横断のマイルストーン進捗について質問できます。 |
| Security Analyst Agent | 脆弱性のトリアージ。重大度別のオープンな脆弱性、グループ全体のCVEカバレッジ、脆弱性の発生タイムラインについて質問できます。 |
| Data Analyst Agent | GLQLを活用したSDLCアナリティクス。パイプラインの健全性、MRのサイクルタイム、コントリビューターのパターン、デプロイ頻度について質問できます。 |
| CI Expert Agent | パイプラインのトリアージ。ジョブの失敗原因、パイプラインの継承、最も遅いジョブ、頻繁に失敗するプロジェクトについて質問できます。 |
| Developer Flow | UIで作業アイテムをドラフトMRに変換します。Orbitは依存関係、オーナーシップ、影響範囲など、ライブのSDLCグラフに基づいてエージェントの実装を補強します。 |

エージェントがOrbitを使用して質問に回答する場合、その回答はエージェントの一般的な知識ではなく、ライブグラフに基づいたものになります。

## 課金 {#billing}

GitLab Duo Agent Platformがお客様に代わってOrbitに対して実行するクエリは消費対象外です。GitLabクレジットを消費しません。

## プロンプトの例 {#example-prompts}

上記のいずれかのサーフェスで質問してください。エージェントが適切なツールを選択します。

コードベースの探索:

- 「グループ内で最近更新された10件のプロジェクトは何ですか？」
- 「最もオープンなマージリクエストが多いプロジェクトはどれですか？」
- 「マージされたマージリクエスト数でこのプロジェクトへの上位コントリビューターは誰ですか？」

影響範囲とインパクト:

- 「`payments-service`ライブラリをインポートしているプロジェクトはどれですか？」
- 「このプロジェクトで`UserAuthService`に依存しているファイルはどれですか？」
- 「この関数を非推奨にした場合、他のどのファイルがそれを参照していますか？」

CI/CDとパイプラインの健全性:

- 「パイプラインの失敗率が最も高いプロジェクトはどれですか？」
- 「このグループで最も一般的なジョブの失敗理由は何ですか？」
- 「実行に最も時間がかかるパイプラインはどれですか？」

セキュリティ:

- 「このグループのクリティカルおよび高重大度のオープンな脆弱性をすべて表示してください。」
- 「過去30日間に発生した未解決の脆弱性があるプロジェクトはどれですか？」
- 「プロジェクト全体に存在するCVEは何ですか？」

計画と作業アイテム:

- 「このグループの各ユーザーに割り当てられているオープンなイシューは何件ですか？」
- 「期限を過ぎているマイルストーンはどれですか？」
- 「このエピックをブロックしている作業アイテムは何ですか？」

## 制限事項 {#limitations}

- Orbitは、有効化されており、かつアクセス権を持つグループについてのみ回答します。
- 複雑な複数ステップの質問は、スコープを絞り込むためのフォローアップが必要になる場合があります。
- コードコンテンツ（ファイルのテキスト、関数の本体）は利用可能ですが、大きな結果に対してはデフォルトで返されない場合があります。明示的に質問してください：「この関数のソースを表示してください。」
