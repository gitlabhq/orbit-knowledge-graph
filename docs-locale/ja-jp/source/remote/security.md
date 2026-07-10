---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Orbit Remoteがデータを保護する方法（クエリに必要なロール、認可モデル、プログラムによるアクセスを含む）。
title: Orbit Remoteのセキュリティ
---

{{< details >}}

- プラン: Premium、Ultimate
- 提供形態: GitLab.com
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- `knowledge_graph`という名前の[機能フラグ](https://docs.gitlab.com/administration/feature_flags/)を使用して、GitLab 18.10で[導入](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。デフォルトでは無効です。この機能は[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)です。
- GitLab 19.1で[ベータ版](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)されました。

{{< /history >}}

> [!flag]
> この機能の利用可否は機能フラグによって制御されています。
> 詳細については、履歴を参照してください。
> この機能はテスト目的で利用可能ですが、本番環境での使用には対応していません。

Orbitへのクエリから返されるレスポンスには、あなたのロールで参照可能な情報のみが含まれます。あなたまたはエージェントが、より高いユーザーロールを必要とするGitLabの機能にアクセスしようとした場合、関連情報はグラフに表示されません。

Orbitのアクセスは階層的です。トップレベルグループで割り当てられたロールは、その配下のすべてのサブグループおよびプロジェクトに適用されます。Orbitを有効にしても、既存のアクセス権限は変更されません。

## Orbitのクエリに必要なロール {#roles-required-to-query-orbit}

グループをクエリするには、そのグループに対してレポーター以上のロールが必要です。

セキュリティデータへのアクセスには、セキュリティマネージャーロールが必要です。対象データは以下のとおりです。

- 脆弱性
- セキュリティの検出結果
- セキュリティスキャン
- スキャナー
- CVE/CWE識別子

セキュリティマネージャーロールが必要な理由は、クエリ実行後に集計結果をフィルタリングできないため、レポーターロールのユーザーにセキュリティの詳細が漏洩する可能性があるためです。レポーターロールのユーザーはグラフの他の部分をクエリできますが、集計カウントを含む結果からセキュリティエンティティは除外されます。

| データドメイン | 最低限必要なロール |
|---|---|
| コア、コードレビュー、CI/CD、プランニング | レポーター |
| セキュリティ | セキュリティマネージャー |

## セキュリティアーキテクチャ {#security-architecture}

Orbitは独自に権限を生成しません。GitLabが「誰が何を参照できるか」の信頼できる唯一の情報源であり、すべてのクエリはGitLabを通じて認可されます。

アクセスは以下のレイヤーで制御されます。

- 組織の分離。クエリは常に自分の組織内のデータのみを参照します。
- 階層的なロールベースのスコープ。結果は、必要なロールを持つグループ、サブグループ、およびプロジェクトに限定されます。兄弟グループはスコープ外となります。
- 各結果のチェック。結果が返される前に、GitLabが各アイテムに対するあなたの権限を再確認し、アクセスできないものを除外します。これにより、機密アイテムや、SAMLグループリンク、IP制限などのランタイム制御も対象となります。

グループの[IPアドレス制限](https://docs.gitlab.com/user/group/access_and_permissions/#restrict-group-access-by-ip-address)はクエリ結果にも適用されます。グループの許可された範囲外のIPからのリクエストは、そのグループからの結果を返しません。

Orbitは読み取り専用です。GitLabからの変更を読み取るのみで、書き戻しは行いません。また、独立した環境で動作し、独自の権限データは保存しません。

## プログラムによるアクセス {#programmatic-access}

プログラムによるアクセスは、既存のGitLab認証を使用し、トークンオーナーがGitLabで参照できる範囲にスコープが限定されます。

- REST API: `read_api`スコープを持つ標準（レガシー）パーソナルアクセストークンをBearerトークンとして送信します。きめ細かいパーソナルアクセストークンはサポートされていません。詳細については、[REST API](access/api.md)を参照してください。
- MCP: GitLab OAuth。ネイティブHTTPクライアントは`mcp_orbit`スコープをリクエストします。詳細については、[MCP](access/mcp.md)を参照してください。
- GitLab Duo Agent Platform: 設定するトークンはありません。詳細については、[GitLab Duo Agent Platform](access/duo.md)を参照してください。
