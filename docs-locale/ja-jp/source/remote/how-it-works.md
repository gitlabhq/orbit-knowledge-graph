---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Remoteがどのようにしてデータとソースコードをインデックス化し、ClickHouseにグラフを構築し、クエリ可能なAPIとして公開するかについて説明します。
title: Orbit Remoteの仕組み
---

{{< details >}}

- 階層: Premium, Ultimate
- 提供形態: GitLab.com
- ステータス: 実験

{{< /details >}}

{{< history >}}

- GitLab 18.10で`knowledge_graph`という名前の[機能フラグ付き](https://docs.gitlab.com/administration/feature_flags/)で[導入されました](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)。デフォルトでは無効です。この機能は[実験](https://docs.gitlab.com/policy/development_stages_support/#experiment)段階にあります。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴をご参照ください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

## インデックス作成パイプライン {#indexing-pipeline}

Orbitは2つのソースからデータをインデックス化し、単一のグラフに統合します。

### SDLCデータ {#sdlc-data}

GitLabは変更データキャプチャ（CDC）パイプラインを通じて変更イベントをストリーミングし、
[GitLab Data Insights Platform](https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/data_insights_platform/)に送信します。
このプラットフォームはClickHouseテーブルにレコードを書き込み、
OrbitはそのデータをもとにグラフをClickHouse上に構築します。

この処理は継続的に行われます。ユーザーがマージリクエストを開いたり、作業アイテムを作成したり、パイプラインを起動したりすると、変更は数分以内にOrbitグラフに反映されます。

### ソースコード {#source-code}

OrbitはGitLab Rails内部APIを呼び出し、リポジトリからソースファイルをフェッチします。
各ファイルを言語固有のパーサーで解析し、定義（関数、クラス、モジュール）とインポート参照を抽出して、ノードとエッジとしてグラフに書き込みます。

コードはデフォルトブランチのみからインデックス化されます。デフォルトブランチが変更されると、再インデックスが自動的に実行されます。

### グラフの構築 {#graph-construction}

SDLCデータとコードを読み込んだ後、Orbitは統合グラフをClickHouseに書き込みます。
各エンティティ（プロジェクト、ユーザー、関数定義）はノードになります。
各リレーションシップ（ユーザーがマージリクエストを作成した、ファイルがモジュールをインポートしたなど）は有向エッジになります。

クエリを送信すると、OrbitはJSON クエリDSLをClickHouse SQLにコンパイルして実行し、型付きの結果を返します。

## グラフモデル {#the-graph-model}

グラフは2つのレイヤーで構成されています。

- SDLCレイヤー: GitLabオブジェクトとそのリレーションシップ。プロジェクトはグループに属します。ユーザーはマージリクエストを作成します。パイプラインはプロジェクト上で実行されます。作業アイテムはユーザーに割り当てられます。
- コードレイヤー: ソースコードの構造とクロスファイル参照。関数はファイル内で定義されます。ファイルは他のファイルからシンボルをインポートします。定義はプロジェクトとブランチ内に存在します。

この2つのレイヤーは連携しています。マージリクエスト（SDLCレイヤー）はファイル（コードレイヤー）に影響します。ユーザー（SDLCレイヤー）は、含まれるファイルを最後に変更した場合、定義（コードレイヤー）のオーナーとなります。

## パフォーマンス {#performance}

Orbitは独立したKubernetesクラスターで動作し、GitLabインスタンスとコンピューティングやメモリを共有しません。

大規模なグループ（数千のプロジェクト、数百万行のコード）の初回インデックス作成は数分で完了します。変更後の増分再インデックスは、変更の規模に応じて数秒から数分で完了します。

## クエリの実行 {#query-execution}

すべてのクエリは同じパスを経由します。

1. OrbitはJSONクエリペイロードを受信します（REST、MCP、またはGitLab Duo Agent Platform経由）。
1. クエリエンジンは現在のスキーマに対してクエリを検証します。
1. OrbitはJSON DSLをClickHouse SQLにコンパイルします。
1. ClickHouseはグラフテーブルに対してクエリを実行します。
1. Orbitは認可フィルタリングを適用します。結果は、リクエストを行ったユーザーがGitLabでアクセス権を持つエンティティにスコープされます。
1. Orbitは型付きのJSON結果を返します。

`options.include_debug_sql: true`を設定することで、クエリレスポンスにコンパイル済みSQLを含めるようリクエストできます。このフィールドは、インスタンス管理者およびレポーター以上のアクセス権を持つGitLab組織の直接メンバーに対してのみ入力されます。
