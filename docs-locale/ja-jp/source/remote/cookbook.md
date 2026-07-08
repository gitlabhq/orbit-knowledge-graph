---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: AIエージェントをコードベース、パイプライン、依存関係、セキュリティの専門家に変える、すぐに使えるプロンプトのライブラリです。Orbitを使用します。
title: Cookbook
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)とともに、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ版](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

Orbitは、コード、マージリクエスト、パイプライン、依存関係、セキュリティなど、ソフトウェア開発ライフサイクル全体に関する質問に答えます。グラフクエリを手動で記述する必要はありません。平易な言葉でAIエージェントに質問するだけで、エージェントがOrbitを使用してグラフを走査し、回答します。

このページは、実際に機能するプロンプトのライブラリです。各プロンプトにより、エージェントが自分のプロジェクトの専門家になります。

## このページの使い方 {#how-to-use-this-page}

1. エージェントをOrbitに接続します。GitLab Duo Agent PlatformにはOrbitが組み込まれています。
   Claude CodeやCodexなどの外部エージェントは、[MCPまたは`glab` CLI](access/mcp.md)を通じて接続します。
1. 目的の結果を選択し、そのプロンプトをコピーします。
1. `<山括弧>`内の値を、自分のグループ、プロジェクト、ファイル、または期間に置き換えます。
1. プロンプトをエージェントに貼り付けて実行します。同じ会話内でフォローアップの質問をすることで、さらに深く掘り下げることができます。

各プロンプトには **「このプロンプトで実行されるOrbitクエリを確認する」** セクションもあります。開く必要はありませんが、エージェントが実行する正確なグラフクエリが表示されます。監査したい場合や[REST API](access/api.md)を直接呼び出したい場合に役立ちます。

## CIのコストをその原因となるコードに紐付ける {#attribute-your-ci-spend-to-the-code-that-causes-it}

CIのコンピューティングコストは高く、その大部分は繰り返し再試行される失敗に隠れています。このプロンプトは、組織全体の失敗をランク付けし、共有CI/CDテンプレートが原因のものを特定し、それぞれを実際に問題を起こしているファイルとコード定義まで追跡します。この最後のステップがコスト帰属チェーンです。「CIが高コスト」という状況を「これらのファイルがこれらのジョブを壊し続けている」という具体的な情報に変換します。

```plaintext
Using Orbit, help me understand what is driving our CI compute cost.

1. Find the job and pipeline failures across my organization over the last
   60 days, covering at least 20 projects. Rank the job names by how often
   they fail.
2. Flag any failing job name that recurs across three or more projects. Those
   usually point to a shared CI/CD template that is worth fixing once.
3. For the top recurring failures, find the merge requests that generate the
   most repeated failed pipelines.
4. Trace those failures back through the merge request diffs to the specific
   files, and the code definitions inside those files, that keep changing.
5. Show me the full chain from failing job to the exact code to review, and
   tell me where to focus a fix to cut the most CI spend.

Prioritize correctness and depth over speed.
```

返ってくる結果: 最もコストのかかる繰り返し失敗のランク付きリスト、クロスプロジェクトの失敗の背後にある共有テンプレート、そして各失敗に紐付いた修正すべきファイルと関数の短いリストです。

応用: 期間を変更したり、特定のグループやプロジェクトに絞り込んだり、上位3件を修正した場合のコンピューティング削減量の見積もりをエージェントに依頼したりすることができます。

<details>
<summary>このプロンプトで実行されるOrbitクエリを確認する</summary>

エージェントはこれらを順番に実行します。サンプルのタイムスタンプをウィンドウ開始日の日付に置き換え、マージリクエストIDとファイルパスは前のステップで返された値に置き換えてください。

組織全体で最も頻繁に発生するジョブの失敗をランク付けします:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"op": "gte", "value": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": [{"kind": "property", "node": "j", "property": "name", "alias": "job_name"}],
  "aggregations": [{"function": "count", "target": "j", "alias": "failures"}],
  "aggregation_sort": {"column": "failures", "direction": "DESC"},
  "limit": 40
}
```

複数のプロジェクトにまたがって繰り返し発生する失敗ジョブを検索します。Orbitにはdistinct-count関数がないため、ジョブ名とプロジェクトを組み合わせてグループ化します。3つ以上のプロジェクトに現れるジョブ名は、共有テンプレートのホットスポットです。

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "j",
      "entity": "Job",
      "filters": {
        "status": "failed",
        "created_at": {"op": "gte", "value": "2025-01-01T00:00:00Z"}
      }
    },
    {"id": "p", "entity": "Project"}
  ],
  "relationships": [{"type": "IN_PROJECT", "from": "j", "to": "p"}],
  "group_by": [
    {"kind": "property", "node": "j", "property": "name", "alias": "job_name"},
    {"kind": "property", "node": "p", "property": "full_path", "alias": "project"}
  ],
  "aggregations": [{"function": "count", "target": "j", "alias": "failures"}],
  "aggregation_sort": {"column": "failures", "direction": "DESC"},
  "limit": 200
}
```

最も繰り返し失敗を生成しているマージリクエストを検索します。`source`を`merge_request_event`にフィルタリングすることで、それらのパイプラインがトリガーしたダウンストリームの子パイプラインをカウントしないようにします。

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "pl",
      "entity": "Pipeline",
      "filters": {
        "status": "failed",
        "source": "merge_request_event",
        "created_at": {"op": "gte", "value": "2025-01-01T00:00:00Z"}
      }
    }
  ],
  "group_by": [{"kind": "property", "node": "pl", "property": "merge_request_id", "alias": "mr_id"}],
  "aggregations": [{"function": "count", "target": "pl", "alias": "failed_pipelines"}],
  "aggregation_sort": {"column": "failed_pipelines", "direction": "DESC"},
  "limit": 20
}
```

1つのマージリクエストから変更し続けているファイルを追跡します。これは単一のマージリクエストに限定してください。失敗したすべてのパイプラインに対して同じトラバーサルを一度に実行するとタイムアウトします。`HAS_FILE`エッジはまばらにしか存在しないため、結果が少ない場合は権威ある結果ではなくカバレッジが不完全であると判断してください。

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "mr", "entity": "MergeRequest", "filters": {"id": {"op": "eq", "value": 123456789}}},
    {"id": "d", "entity": "MergeRequestDiff"},
    {"id": "f", "entity": "MergeRequestDiffFile"}
  ],
  "relationships": [
    {"type": "HAS_DIFF", "from": "mr", "to": "d"},
    {"type": "HAS_FILE", "from": "d", "to": "f"}
  ],
  "group_by": [{"kind": "property", "node": "f", "property": "old_path", "alias": "file"}],
  "aggregations": [{"function": "count", "target": "d", "alias": "diff_snapshots"}],
  "aggregation_sort": {"column": "diff_snapshots", "direction": "DESC"},
  "limit": 20
}
```

ホットスポットファイル内のコード定義を詳しく調べます。`File`ノードと`Definition`ノードはインデックス作成済みのソースファイルにのみ存在するため、テストサポートヘルパーなど一部のパスはインデックス作成されていない場合があります。

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"op": "eq", "value": "app/models/project.rb"}}
    },
    {
      "id": "def",
      "entity": "Definition",
      "columns": ["name", "fqn", "definition_type", "start_line"]
    }
  ],
  "relationships": [{"type": "DEFINES", "from": "f", "to": "def"}],
  "limit": 30
}
```

</details>

## コードベースを素早く理解する {#understand-a-codebase-fast}

不慣れなプロジェクトに参加し、数日ではなく数分で全体像を把握します。

```plaintext
I'm new to the <my-org/my-project> project. Using Orbit, give me a tour:
- The most active contributors over the last few months.
- The core classes, modules, and how they relate.
- The main entry points and the files I should read first.

Then summarize how this codebase is structured and suggest the three files
to read first to understand it.
```

<details>
<summary>このプロンプトで実行されるOrbitクエリを確認する</summary>

プロジェクトの最もアクティブなコントリビューターを検索します:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "u", "entity": "User", "columns": ["username", "name"]},
    {
      "id": "mr",
      "entity": "MergeRequest",
      "filters": {"state": "merged"}
    },
    {
      "id": "p",
      "entity": "Project",
      "filters": {"full_path": "my-org/my-project"}
    }
  ],
  "relationships": [
    {"type": "AUTHORED", "from": "u", "to": "mr"},
    {"type": "IN_PROJECT", "from": "mr", "to": "p"}
  ],
  "group_by": [{"kind": "node", "node": "u"}],
  "aggregations": [
    {"function": "count", "target": "mr", "alias": "merged_mrs"}
  ],
  "aggregation_sort": {"column": "merged_mrs", "direction": "DESC"},
  "limit": 10
}
```

</details>

## 依存関係と影響範囲をマッピングする {#map-dependencies-and-blast-radius}

変更する前に「これを変更すると何が壊れるか」を把握します。

```plaintext
Using Orbit, map the blast radius of <shared-auth-lib>.
- Which projects and files import it?
- Which code definitions depend on it?
- What would break if I changed its public interface?

Rank the affected areas by how many places depend on them, and tell me the
riskiest change I could make.
```

<details>
<summary>このプロンプトで実行されるOrbitクエリを確認する</summary>

特定のモジュールをインポートしているすべてのファイルを検索します。`payments-service`をトレースしたいモジュールまたはライブラリに置き換えてください:

```json orbit-query
{
  "query_type": "traversal",
  "node": {
    "id": "sym",
    "entity": "ImportedSymbol",
    "columns": ["file_path", "import_path", "identifier_name"],
    "filters": {
      "import_path": {"op": "contains", "value": "payments-service"}
    }
  },
  "limit": 100
}
```

共有ライブラリに依存しているプロジェクトを検索します:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "f",
      "entity": "File",
      "filters": {"path": {"op": "contains", "value": "shared-auth-lib"}}
    },
    {"id": "b", "entity": "Branch", "columns": ["name", "is_default"]},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "ON_BRANCH", "from": "f", "to": "b"},
    {"type": "CONTAINS", "from": "p", "to": "b"}
  ],
  "limit": 100
}
```

最も多くのコードにインポートされている定義をランク付けします:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "sym",
      "entity": "ImportedSymbol",
      "columns": ["import_path"],
      "filters": {
        "import_path": {"op": "contains", "value": "payments"}
      }
    },
    {"id": "def", "entity": "Definition", "columns": ["name", "fqn", "file_path"]}
  ],
  "relationships": [
    {"type": "IMPORTS", "from": "sym", "to": "def"}
  ],
  "group_by": [{"kind": "node", "node": "def"}],
  "aggregations": [
    {"function": "count", "target": "sym", "alias": "import_count"}
  ],
  "aggregation_sort": {"column": "import_count", "direction": "DESC"},
  "limit": 20
}
```

</details>

## パイプラインの健全性を維持する {#keep-your-pipelines-healthy}

最も問題のあるCI/CDの原因とその失敗理由を特定します。

```plaintext
Using Orbit, show me where our CI/CD is unhealthy over the last 30 days:
- The projects with the most failed pipelines.
- The jobs that fail most often.
- The most common failure reasons.

Group the results so I can see which failures are worth fixing first.
```

<details>
<summary>このプロンプトで実行されるOrbitクエリを確認する</summary>

失敗したパイプラインが最も多いプロジェクトを検索します:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {"id": "pl", "entity": "Pipeline", "filters": {"status": "failed"}},
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "pl", "to": "p"}
  ],
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "pl", "alias": "failed_count"}
  ],
  "aggregation_sort": {"column": "failed_count", "direction": "DESC"},
  "limit": 10
}
```

失敗したジョブとその失敗理由を検索します:

```json orbit-query
{
  "query_type": "traversal",
  "node": {
    "id": "j",
    "entity": "Job",
    "columns": ["name", "status", "failure_reason"],
    "filters": {"status": "failed"}
  },
  "limit": 10
}
```

</details>

## セキュリティリスクをその発生源まで追跡する {#trace-security-risk-to-its-source}

リスクの所在とその経緯を把握します。

```plaintext
Using Orbit, find the critical and high severity vulnerabilities across
<my-org> that are still detected:
- Which projects are affected?
- How did each one get there? Trace it back to the scan and, where possible,
  the merge request that introduced the change.

Prioritize by severity and give me a short remediation shortlist.
```

<details>
<summary>このプロンプトで実行されるOrbitクエリを確認する</summary>

すべてのcriticalおよびhigh重大度の脆弱性を検索します:

```json orbit-query
{
  "query_type": "traversal",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "columns": ["title", "severity", "state", "report_type"],
      "filters": {
        "severity": {"op": "in", "value": ["critical", "high"]},
        "state": "detected"
      }
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "order_by": {"node": "v", "property": "severity", "direction": "DESC"},
  "limit": 50
}
```

プロジェクト別に脆弱性を集計します:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    },
    {"id": "p", "entity": "Project", "columns": ["name", "full_path"]}
  ],
  "relationships": [
    {"type": "IN_PROJECT", "from": "v", "to": "p"}
  ],
  "group_by": [{"kind": "node", "node": "p"}],
  "aggregations": [
    {"function": "count", "target": "v", "alias": "vuln_count"}
  ],
  "aggregation_sort": {"column": "vuln_count", "direction": "DESC"},
  "limit": 20
}
```

重大度別に脆弱性を集計します:

```json orbit-query
{
  "query_type": "aggregation",
  "nodes": [
    {
      "id": "v",
      "entity": "Vulnerability",
      "filters": {"state": "detected"}
    }
  ],
  "group_by": [
    {"kind": "property", "node": "v", "property": "severity", "alias": "severity"}
  ],
  "aggregations": [
    {"function": "count", "target": "v", "alias": "vuln_count"}
  ],
  "aggregation_sort": {"column": "vuln_count", "direction": "DESC"},
  "limit": 10
}
```

</details>

## 実際のソースコードを読む {#read-the-actual-source}

エージェントを離れることなく、実際のコードを会話に取り込みます。

```plaintext
Using Orbit, show me the source of <app/models/project.rb> and the definition
of <MyModule::my_function>, so I can review them here.
```

バーチャルカラム（`File`の`content`と`Definition`の`content`）はグラフクエリの後にGitalyのフェッチをトリガーするため、これらのレスポンスは他のクエリよりも遅くなります。

<details>
<summary>このプロンプトで実行されるOrbitクエリを確認する</summary>

ファイルのソーステキストをフェッチします。大きなレスポンスを避けるために`limit: 1`を使用してください:

```json orbit-query
{
  "query_type": "traversal",
  "node": {
    "id": "f",
    "entity": "File",
    "columns": ["path", "language", "content"],
    "filters": {
      "path": {"op": "ends_with", "value": "app/models/project.rb"}
    }
  },
  "limit": 1
}
```

特定の関数またはクラス定義のソーステキストをフェッチします。`content`フィールドはファイル全体ではなく、その定義のみの生のソーステキストを返します:

```json orbit-query
{
  "query_type": "traversal",
  "node": {
    "id": "d",
    "entity": "Definition",
    "columns": ["name", "fqn", "file_path", "start_line", "end_line", "content"],
    "filters": {
      "fqn": {"op": "eq", "value": "Gitlab::Auth::authenticate"}
    }
  },
  "limit": 5
}
```

</details>
