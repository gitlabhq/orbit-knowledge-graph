---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local이 GitLab Orbit CLI와 DuckDB를 사용하여 사용자 머신에서 코드 그래프를 빌드하고 쿼리하는 방법입니다.
title: GitLab Orbit Local의 작동 방식
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 19.0에서 [도입](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되었으며 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)입니다.
- GitLab 19.1에서 [변경](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되어 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta) 단계가 되었습니다.

{{< /history >}}

> [!note]
GitLab Orbit Local은 실험 단계입니다. GA 출시 전에 기능과 명령 형식이 변경될 수 있습니다.

## 인덱싱 파이프라인 {#indexing-pipeline}

`orbit index`을 실행하면 GitLab Orbit Local은 다음을 수행합니다:

1. `.gitignore`을 존중하면서 리포지토리 디렉토리 트리를 탐색합니다.
1. 각 소스 파일을 언어별 파서(rust-analyzer, tree-sitter 또는 언어에 따른 사용자 지정 파서)에 전달합니다.
1. 정의(함수, 클래스, 모듈), 가져오기 선언 및 파일 간 기호 참조를 추출합니다.
1. 결과를 노드와 에지로 `~/.orbit/graph.duckdb`의 로컬 DuckDB 파일에 씁니다.

v2 파이프라인은 모든 언어 파서를 병렬로 실행합니다. 중간 크기의 리포지토리 인덱싱은 일반적으로 몇 초 안에 완료됩니다.

## 그래프 모델 {#the-graph-model}

GitLab Orbit Local은 코드 전용 그래프를 빌드합니다. GitLab 연결이 없기 때문에 SDLC 데이터(머지 리퀘스트, 파이프라인, 사용자)에 접근할 수 없습니다.

로컬 그래프의 노드:

- **파일** \- 리포지토리의 소스 파일
- **디렉토리** \- 리포지토리의 디렉토리
- **정의** \- 함수, 클래스, 모듈 또는 기타 명명된 기호
- **ImportedSymbol** \- 다른 파일 또는 패키지에서 가져온 기호

에지는 파일을 정의에 연결하고, 파일을 가져오기에 연결하며, 정의를 파일 간에 참조하는 기호에 연결합니다.

## 쿼리 실행 {#query-execution}

GitLab Orbit Local은 그래프를 DuckDB 데이터베이스로 노출합니다. `orbit sql`을(를) 사용하여 읽기 전용 SQL을 실행합니다:

1. `orbit sql`은 `~/.orbit/graph.duckdb`을 읽기 전용으로 엽니다.
1. SQL은 그래프 테이블에 대해 직접 실행됩니다 — DSL 컴파일 없음, 권한 부여 계층 없음.
1. 결과는 테이블, JSON, NDJSON 또는 CSV로 다시 스트리밍됩니다.

그래프의 모든 데이터는 CLI를 실행하는 모든 사용자가 접근할 수 있습니다.

## 스토리지 {#storage}

그래프는 `~/.orbit/graph.duckdb`의 단일 DuckDB 파일에 저장됩니다. 여러 리포지토리가 같은 데이터베이스를 공유합니다. 각 리포지토리는 매니페스트 테이블의 프로젝트 ID와 브랜치로 범위가 지정됩니다.

## 지원하는 언어 {#supported-languages}

GitLab Orbit Remote에서 지원하는 모든 13개 언어를 로컬에서도 지원합니다: Ruby, Java, Kotlin, Python, TypeScript, JavaScript, Rust, Go, C#, C, C++, PHP, Bash/Shell입니다.

전체 언어 지원 테이블은 [GitLab Orbit을 사용한 인덱스 데이터](../indexed-data.md#supported-languages)를 참조하세요.

## 청구 {#billing}

GitLab Orbit Local은 GitLab Credits를 사용하지 않습니다. 모든 처리는 로컬입니다.
