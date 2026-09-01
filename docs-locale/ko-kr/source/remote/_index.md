---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit이 GitLab 호스팅 인프라에서 실행됨
title: GitLab Orbit Remote
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab.com
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 18.10에서 [도입](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되었으며 [기능 플래그](https://docs.gitlab.com/administration/feature_flags/)의 이름은 `knowledge_graph`입니다. 기본적으로 비활성화되었습니다. 이 기능은 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment) 단계입니다.
- GitLab 19.1에서 [변경](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되어 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta) 단계가 되었습니다.

{{< /history >}}

> [!flag]
이 기능의 사용 가능 여부는 기능 플래그에 의해 제어됩니다. 자세한 내용은 이력을 참조하세요. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

GitLab Orbit Remote는 GitLab 호스팅 인프라에서 실행됩니다. 최상위 그룹에서 사용 설정하면 전체 SDLC 및 코드(그룹, 프로젝트, 사용자, 머지 리퀘스트, 파이프라인, 취약점 및 소스 코드)를 ClickHouse 속성 그래프로 자동 인덱싱합니다.

- 인덱스: 전체 SDLC + 코드 그래프
- 저장소: ClickHouse(관리형, 설정 불필요)

[GitLab Orbit Remote 시작하기](getting-started.md)

## 이 섹션 {#in-this-section}

| 페이지 | 설명 |
|---|---|
| [시작하기](getting-started.md) | GitLab Orbit을 사용 설정하고 첫 번째 쿼리를 실행합니다 |
| [작동 방식](how-it-works.md) | 인덱싱 파이프라인, 그래프 모델, 쿼리 실행 |
| [GitLab Orbit이 인덱싱하는 것](indexing.md) | SDLC 범위, 언어 지원, 인덱싱 범위 |
| [보안](security.md) | 쿼리에 필요한 역할, 인증 모델 및 프로그래밍 방식 액세스 |
| [스키마 참조](schema.md) | 6개 도메인에 걸친 모든 28개 노드 유형 |
| [요리책](cookbook.md) | 일반적인 사용 사례를 위한 복사-붙여넣기 쿼리 |
| [쿼리 언어](queries/) | 전체 쿼리 DSL 참조 |

## 액세스 방법 {#access-methods}

| 방법 | 설명 |
|---|---|
| [GitLab Duo Agent Platform](access/duo.md) | GitLab UI를 통한 자연어 질문 |
| [MCP](access/mcp.md) | Claude Code, Codex 및 기타 에이전트 연결 |
| [GitLab CLI (`glab`)](access/glab.md) | `glab orbit remote`을 스크립팅 및 검색에 사용할 수 있습니다(`glab` 1.94 이상에서 사용 가능) |
| [REST API](access/api.md) | 스크립트, CI 파이프라인 또는 사용자 지정 도구에서 쿼리 |

## 청구 {#billing}

MCP 및 REST API 쿼리는 GitLab Credits를 사용합니다. GitLab Duo Agent Platform 쿼리는 무료입니다.
