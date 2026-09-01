---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Orbit Remote이 데이터를 보호하는 방법(쿼리에 필요한 역할, 권한 부여 모델, 프로그래밍 방식 액세스 포함)입니다."
title: GitLab Orbit Remote 보안
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab.com
- 상태:  베타

{{< /details >}}

{{< history >}}

- [GitLab 18.10에서 도입](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되었으며 [기능 플래그](https://docs.gitlab.com/administration/feature_flags/)로 명명된 `knowledge_graph`입니다. 기본적으로 비활성화되었습니다. 이 기능은 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)입니다.
- GitLab 19.1에서 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta)로 [변경](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)되었습니다.

{{< /history >}}

> [!flag]
이 기능의 사용 가능 여부는 기능 플래그에 의해 제어됩니다. 자세한 내용은 이력을 참조하세요. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

GitLab Orbit에 대한 쿼리에서 반환되는 응답은 사용자의 역할에서 사용할 수 있는 정보만 포함합니다. 사용자 또는 에이전트가 더 높은 사용자 역할이 필요한 GitLab의 일부에 액세스하려고 하면 관련 정보가 그래프에 표시되지 않습니다.

GitLab Orbit의 액세스는 계층적입니다. 최상위 그룹에 할당된 역할은 그 아래의 모든 하위 그룹 및 프로젝트에 적용됩니다. GitLab Orbit을 설정해도 기존 액세스는 변경되지 않습니다.

## GitLab Orbit을 쿼리하는 데 필요한 역할 {#roles-required-to-query-gitlab-orbit}

그룹을 쿼리하려면 해당 그룹에 대해 Reporter 역할 이상이 필요합니다.

보안 데이터에 대한 액세스는 보안 관리자 역할이 필요합니다. 여기에는 다음 데이터가 포함됩니다:

- 취약점
- 보안 검사 결과
- 보안 스캔
- 스캐너
- CVE/CWE 식별자

보안 관리자 역할은 쿼리 실행 후 결과를 필터링할 수 없으며, 이로 인해 Reporter 역할을 가진 사용자에게 보안 세부 정보가 노출될 수 있기 때문에 필요합니다. Reporter 역할을 가진 사용자는 나머지 그래프를 쿼리할 수 있지만, 집계 개수를 포함한 결과에서 보안 항목이 제거됩니다.

| 데이터 영역 | 최소 역할 |
|---|---|
| 코드 검토, CI/CD, 계획 | Reporter |
| 보안 | Security Manager |

## 보안 아키텍처 {#security-architecture}

GitLab Orbit은 권한을 직접 생성하지 않습니다. GitLab은 누가 무엇을 볼 수 있는지에 대한 단일 출처이며, 모든 쿼리는 GitLab을 통해 권한을 부여받습니다.

액세스는 다음 계층에서 적용됩니다:

- 조직 격리. 쿼리는 자신의 조직의 데이터만 볼 수 있습니다.
- 계층적, 역할 기반 범위 지정. 결과는 필요한 역할을 보유한 그룹, 하위 그룹 및 프로젝트로 제한됩니다. 동위 그룹은 범위를 벗어납니다.
- 각 결과에 대한 확인. 결과를 반환하기 전에 GitLab은 각 항목에 대한 사용자의 권한을 다시 확인하고 액세스할 수 없는 항목을 제거합니다. 이렇게 하면 기밀 항목과 SAML 그룹 링크 및 IP 제한과 같은 런타임 제어를 포착합니다.

그룹 [IP 주소 제한](https://docs.gitlab.com/user/group/access_and_permissions/#restrict-group-access-by-ip-address)은 쿼리 결과에 적용됩니다. 그룹의 허용된 범위 밖의 IP에서 요청하면 해당 그룹에서 결과가 반환되지 않습니다.

GitLab Orbit은 읽기 전용입니다. GitLab에서 변경 사항을 읽고 다시 기록하지 않으며, 별도의 환경에서 실행되고, 자체 권한 데이터를 저장하지 않습니다.

## 프로그래밍 방식 액세스 {#programmatic-access}

프로그래밍 방식 액세스는 기존 GitLab 인증을 사용하며, 토큰 소유자가 GitLab에서 볼 수 있는 범위로 제한됩니다.

- REST API: `read_api` 범위를 가진 표준(레거시) 개인 액세스 토큰으로, Bearer 토큰으로 전송됩니다. 세밀한 개인 액세스 토큰은 지원되지 않습니다. 자세한 내용은 [REST API](access/api.md)를 참조하세요.
- MCP: GitLab OAuth. 네이티브 HTTP 클라이언트는 `mcp_orbit` 범위를 요청합니다. 자세한 내용은 [MCP](access/mcp.md)를 참조하세요.
- GitLab Duo Agent Platform: 구성할 토큰이 없습니다. 자세한 내용은 [GitLab Duo Agent Platform](access/duo.md)을 참조하세요.
