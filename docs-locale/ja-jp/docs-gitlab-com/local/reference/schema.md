---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Localスキーマ参照を使用して、ローカルコードグラフでサポートされているノードタイプとそれらがどのように接続されているかについて学習します。
title: GitLab Orbit Localスキーマ参照
---

{{< details >}}

- プラン: Free、Premium、Ultimate
- 提供形態: GitLab.com、GitLab Self-Managed、GitLab Dedicated
- ステータス: ベータ版

{{< /details >}}

{{< history >}}

- GitLab 19.0で[実験的機能](https://docs.gitlab.com/policy/development_stages_support/#experiment)として[導入](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)されました。
- GitLab 19.1で[ベータ版](https://docs.gitlab.com/policy/development_stages_support/#beta)に[変更](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)されました。

{{< /history >}}

GitLab Orbit Localスキーマ参照は、利用可能なノード、エッジ、およびローカルグラフ内のリレーションシップを定義します。

## ソースコードノード {#source-code-nodes}

以下のセクションでは、利用可能なノードとそのプロパティを定義します。

### `Directory` {#directory}

インデックス付けされたリポジトリ内のディレクトリ。

| プロパティ | 定義 |
|------------|------------|
| `id` | ディレクトリの固有識別子 |
| `project_id` | インデックス付けされたリポジトリの識別子で、絶対パスから導出されます。GitLabプロジェクトIDではありません。 |
| `branch` | ディレクトリがインデックス付けされたブランチ |
| `commit_sha` | ディレクトリがインデックス付けされたコミット |
| `path` | リポジトリのルートディレクトリに対するパス |
| `name` | ディレクトリ名 |

### `File` {#file}

インデックス付けされたリポジトリ内のソースコードファイル。

| プロパティ | 定義 |
|------------|------------|
| `id` | ファイルの固有識別子 |
| `project_id` | インデックス付けされたリポジトリの識別子で、絶対パスから導出されます。GitLabプロジェクトIDではありません |
| `branch` | ファイルがインデックス付けされたブランチ |
| `commit_sha` | ファイルがインデックス付けされたコミット |
| `path` | リポジトリのルートディレクトリに対するパス |
| `name` | 拡張子を含むファイル名 |
| `extension` | 先頭のドットを除いたファイル拡張子 |
| `language` | 検出されたプログラミング言語、一致する言語がない場合は`unknown` |
| `size_bytes` | ファイルがインデックス付けされたときに測定した、バイト単位のファイルサイズ |
| `reason` | `skip_excluded_extension`や`fault_invalid_utf8`など、ファイルがスキップされた、または解析に失敗した理由。ファイルが正常に解析された場合は空。 |

### `Definition` {#definition}

ファイルで宣言された関数、クラス、メソッド、モジュール、またはその他の名前付きシンボル。

| プロパティ | 定義 |
|------------|------------|
| `id` | 定義の固有識別子で、ファイルパスごとに生成されます。2つのインデックス付けされたリポジトリ内の同じ関数は、異なるIDを持ちます。 |
| `project_id` | インデックス付けされたリポジトリの識別子で、絶対パスから導出されます。GitLabプロジェクトIDではありません。 |
| `branch` | 定義がインデックス付けされたブランチ |
| `commit_sha` | 定義がインデックス付けされたコミット |
| `file_path` | 定義を宣言するファイルのパス |
| `fqn` | 関数、クラス、モジュール、メソッド、構造、enum、またはトレイトの完全修飾名 |
| `name` | ネームスペース修飾を含まない短縮名 |
| `definition_type` | `Class`、`Function`、`Method`、`Struct`など、定義の種類。値は先頭が大文字で、言語固有です。 |
| `start_line` | 定義の最初の行 |
| `end_line` | 定義の最後の行 |
| `start_byte` | 定義が開始するバイトオフセット |
| `end_byte` | 定義が終了するバイトオフセット |
| `start_char` | `start_line`上で定義が開始するカラム |
| `end_char` | `end_line`上で定義が終了するカラム |
| `search_text` | `fqn`と`file_path`から抽出された小文字の識別子トークン。ランク付けされたコード検索に使用されます |
| `token_count` | `search_text`内のトークン数 |

### `ImportedSymbol` {#importedsymbol}

ファイル内のインポートステートメント、またはファイル間のシンボル参照。

| プロパティ | 定義 |
|------------|------------|
| `id` | インポートされたシンボルの固有識別子 |
| `project_id` | インデックス付けされたリポジトリの識別子で、絶対パスから導出されます。GitLabプロジェクトIDではありません。 |
| `branch` | シンボルがインデックス付けされたブランチ |
| `commit_sha` | シンボルがインデックス付けされたコミット |
| `file_path` | インポートを含むファイルのパス |
| `import_type` | `Use`、`NamedImport`、`CjsRequire`など、インポートの種類。値は先頭が大文字で、言語固有です。 |
| `import_path` | シンボルのインポート元のモジュールまたはパス |
| `identifier_name` | インポートされたシンボルの名前。ワイルドカードインポートおよび副作用インポートの場合は空。 |
| `identifier_alias` | インポートがシンボルをバインドするローカル名。アナライザーが何も記録しない場合は空。一部の言語では、インポートがリネームされていなくても、それが入力されます。 |
| `start_line` | インポートの最初の行 |
| `end_line` | インポートの最後の行 |
| `start_byte` | インポートが開始するバイトオフセット |
| `end_byte` | インポートが終了するバイトオフセット |
| `start_char` | `start_line`上の、インポートが開始するカラム |
| `end_char` | `end_line`上の、インポートが終了するカラム |

## リレーションシップ {#relationships}

ローカルグラフ内のエッジには、以下のリレーションシップタイプがあります:

- `CONTAINS`
- `DEFINES`
- `IMPORTS`
- `CALLS`
- `EXTENDS`

同じリレーションシップタイプで異なるノードタイプを接続できます。

| リレーションシップタイプ | ソースノード | ターゲットノード | 説明 |
|--------------|--------|--------|-------------|
| `CONTAINS` | `Directory` | `Directory` | ディレクトリにサブディレクトリが含まれます |
| `CONTAINS` | `Directory` | `File` | ディレクトリにファイルが含まれます |
| `DEFINES` | `File` | `Definition` | ファイルで、クラスや関数などのコード定義を定義します |
| `DEFINES` | `Definition` | `Definition` | 外側の定義には、クラスとそのメソッドなど、ネストされた定義が字句的に含まれます |
| `IMPORTS` | `File` | `ImportedSymbol` | ファイルにインポートステートメントが含まれます |
| `IMPORTS` | `ImportedSymbol` | `Definition` | インポートされたシンボルが別のファイル内の具体的な定義に解決されます |
| `CALLS` | `Definition` | `Definition` | 定義から別の定義を呼び出します |
| `CALLS` | `Definition` | `ImportedSymbol` | 定義から、定義に解決されていないインポートされたシンボルを呼び出します |
| `CALLS` | `File` | `Definition`または`ImportedSymbol` | いずれの定義にも含まれないトップレベルの呼び出しサイト |
| `EXTENDS` <sup>1</sup> | `Definition` | `Definition` | 定義で、ファイル内に宣言された関数、クラス、メソッド、モジュール、またはその他の名前付きシンボルのスーパータイプを宣言します |

**補足説明**: 

1. `EXTENDS`は子から親を指します。ベースクラスを継承するクラスはソースノードであり、ベースクラスはターゲットノードです。
