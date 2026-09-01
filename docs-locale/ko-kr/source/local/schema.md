---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local 코드 그래프의 4가지 노드 유형과 이들이 연결되는 방식에 대한 참고자료입니다.
title: 스키마 참고자료
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 19.0에서 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)으로 [도입](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되었습니다.
- GitLab 19.1에서 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta)로 [변경](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되었습니다.

{{< /history >}}

> [!note]
GitLab Orbit Local은 실험 단계입니다. GA 이전에 기능과 명령 형태가 변경될 수 있습니다.

GitLab Orbit Local은 4가지 노드 유형을 인덱싱합니다. 모두 소스 코드 도메인에 있습니다. GitLab Orbit Local이 GitLab에 연결되지 않기 때문에 SDLC 계층이 없습니다.

언제든 라이브 DuckDB 스키마를 검사하려면:

```shell
orbit schema
```

## 소스 코드 {#source-code}

| 노드 유형 | 설명 | 주요 속성 |
|-----------|-------------|----------------|
| `Directory` | 인덱싱된 리포지토리의 디렉토리 | `id`, `path`, `name` |
| `File` | 소스 코드 파일 | `id`, `path`, `name`, `extension`, `language`, `content` |
| `Definition` | 함수, 클래스, 메서드 또는 모듈 정의 | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `ImportedSymbol` | 가져오기 또는 파일 간 기호 참조 | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## 관계 {#relationships}

로컬 그래프의 엣지는 다음을 연결합니다:

- 디렉토리를 포함하는 파일 및 하위 디렉토리
- 선언하는 정의에 대한 파일
- 가져오는 기호에 대한 파일
- 다른 파일에서 확인하는 정의에 대한 가져온 기호

## GitLab Orbit Remote와의 차이점 {#differences-from-gitlab-orbit-remote}

[GitLab Orbit Remote](../remote/schema.md)는 6개 도메인에서 28가지 노드 유형을 인덱싱합니다. GitLab Orbit Local은 소스 코드 도메인만 포함합니다. GitLab 데이터가 필요한 모든 것(머지 리퀘스트, 파이프라인, 사용자, 취약점, 작업 항목)을 사용할 수 없습니다.

## 참고사항 {#notes}

- 정의 ID는 파일 경로별로 범위가 지정된 콘텐츠 해싱된 정수입니다. 두 인덱싱된 리포지토리에 있는 동일한 함수는 다른 ID를 갖습니다.
- `content` 필드는 `Definition` 및 `File` 노드에서 전체 소스 텍스트를 포함합니다. 이들은 에이전트 도구가 별도의 파일 읽기 없이 코드를 수화할 수 있도록 채워집니다.
- 인증 계층이 없습니다. GitLab Orbit Local은 사용자별 액세스 제어를 적용하지 않습니다. `~/.orbit/graph.duckdb`의 그래프 파일은 파일 시스템 권한으로만 보호됩니다.
