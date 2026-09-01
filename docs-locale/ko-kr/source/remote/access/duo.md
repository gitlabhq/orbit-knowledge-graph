---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Duo Agent Platform을 통해 GitLab Orbit을 사용합니다. 에이전트는 GitLab Duo Agent, Planner 에이전트, Security Analyst 에이전트, Data Analyst 에이전트, CI Expert 에이전트, Developer 플로우에서 GitLab Orbit의 그래프 도구를 호출하여 라이브 GitLab 데이터에 답변의 근거를 제시합니다."
title: GitLab Duo Agent Platform에서 GitLab Orbit 사용
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

GitLab Orbit은 GitLab Duo Agent Platform에 통합되어 있습니다. 에이전트는 GitLab Orbit의 명령 도구(`list_commands`, `invoke_command`)를 자동으로 호출하며, `get_graph_schema` 및 `query_graph`와 같은 명령을 실행합니다. 이는 교차 프로젝트 의존성, 영향 범위, 파이프라인 상속, 취약성 계보, 기여자 패턴을 포함한 SDLC 그래프를 순회하여 답변이 필요할 때 사용됩니다. GitLab Orbit이 답변을 찾지 못하면 에이전트는 기존 도구로 대체됩니다.

## 전제 조건 {#prerequisites}

- GitLab Orbit이 [그룹에서 활성화](../getting-started.md)되어 있습니다.
- [GitLab Duo Agent Platform](https://docs.gitlab.com/user/duo_agent_platform/)에 액세스할 수 있습니다.

## GitLab Orbit을 사용할 수 있는 위치 {#where-gitlab-orbit-is-available}

GitLab Orbit은 다음의 GitLab Duo Agent Platform 에이전트 및 플로우와 연결되어 있습니다:

| 에이전트 또는 플로우 | 사용 시점 |
|---|---|
| GitLab Duo Agent | 일반 개발 지원 도구입니다. 코드, 계획, 보안, 프로젝트 관리에 대한 도움을 받습니다. 답변이 그래프 컨텍스트의 이점을 얻을 때 GitLab Orbit을 호출합니다. |
| Planner 에이전트 | 이슈 및 마일스톤 계획입니다. 작업 항목 소유권, 차단 요소, 기여자 부하, 프로젝트 전반의 마일스톤 진행 상황에 대해 질문합니다. |
| Security Analyst 에이전트 | 취약성 분류입니다. 심각도별 공개된 취약성, 그룹 전반의 CVE 범위, 취약성 도입 타임라인에 대해 질문합니다. |
| Data Analyst 에이전트 | GLQL을 통한 SDLC 분석입니다. 파이프라인 상태, 머지 리퀘스트 주기 시간, 기여자 패턴, 배포 빈도에 대해 질문합니다. |
| CI Expert 에이전트 | 파이프라인 분류입니다. 작업 실패 원인, 파이프라인 상속, 가장 느린 작업, 자주 실패하는 프로젝트에 대해 질문합니다. |
| Developer 플로우 | 작업 항목을 UI의 임시 머지 리퀘스트로 변환합니다. GitLab Orbit은 에이전트의 구현을 라이브 SDLC 그래프(의존성, 소유권, 영향 범위)에 기반하여 제시합니다. |

에이전트가 GitLab Orbit을 사용하여 질문에 답변할 때, 답변은 에이전트의 일반 지식이 아닌 라이브 그래프에 기반합니다.

## 청구 {#billing}

GitLab Duo Agent Platform이 귀하를 대신하여 GitLab Orbit에 대해 수행한 쿼리는 무료입니다. GitLab Credits를 소비하지 않습니다.

## 예시 프롬프트 {#example-prompts}

위의 모든 서피스에서 이를 요청하면 에이전트가 올바른 도구를 선택합니다.

코드베이스 탐색:

- "내 그룹에서 가장 최근에 업데이트된 10개의 프로젝트는 무엇입니까?"
- "가장 많은 공개 머지 리퀘스트가 있는 프로젝트는 무엇입니까?"
- "머지된 머지 리퀘스트별로 이 프로젝트의 상위 기여자는 누구입니까?"

영향 범위 및 영향:

- "`payments-service` 라이브러리를 가져오는 프로젝트는 어떤 것입니까?"
- "이 프로젝트에서 `UserAuthService`에 의존하는 파일은 어떤 것입니까?"
- "이 함수를 사용 중단하면 참조하는 다른 파일은 어떤 것입니까?"

CI/CD 및 파이프라인 상태:

- "가장 높은 파이프라인 실패율을 가진 프로젝트는 어떤 것입니까?"
- "이 그룹에서 가장 일반적인 작업 실패 이유는 무엇입니까?"
- "실행하는 데 가장 오래 걸리는 파이프라인은 어떤 것입니까?"

보안:

- "이 그룹의 중요도가 높은 공개 취약성을 모두 표시합니다."
- "지난 30일 이내에 도입된 미해결 취약성이 있는 프로젝트는 어떤 것입니까?"
- "내 프로젝트 전반에 존재하는 CVE는 무엇입니까?"

계획 및 작업 항목:

- "이 그룹의 각 사용자에게 할당된 공개 이슈는 몇 개입니까?"
- "기한이 지난 마일스톤은 어떤 것입니까?"
- "이 에픽을 차단하는 작업 항목은 무엇입니까?"

## 제한 사항 {#limitations}

- GitLab Orbit은 활성화되어 있으며 액세스할 수 있는 그룹에 대해서만 답변합니다.
- 복잡한 다단계 질문은 범위를 좁히기 위해 추가 질문이 필요할 수 있습니다.
- 코드 콘텐츠(파일 텍스트, 함수 본문)를 사용할 수 있지만 큰 결과의 경우 기본적으로 반환되지 않을 수 있습니다. 명시적으로 요청합니다: "이 함수의 소스를 보여주세요."
