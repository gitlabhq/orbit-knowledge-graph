---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Remote가 GitLab 데이터와 소스 코드를 인덱싱하고 ClickHouse에서 그래프를 빌드하며 이를 쿼리 가능한 API로 노출하는 방식입니다.
title: GitLab Orbit Remote 작동 방식
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab.com
- 상태:  베타

{{< /details >}}

{{< history >}}

- [GitLab 18.10에서 도입됨](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676) [기능 플래그](https://docs.gitlab.com/administration/feature_flags/)로 이름이 `knowledge_graph`입니다. 기본적으로 비활성화되었습니다. 이 기능은 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)입니다.
- GitLab 19.1에서 [베타](https://gitlab.com/gitlab-org/gitlab/-/work_items/583676)로 [변경](https://docs.gitlab.com/policy/development_stages_support/#beta)되었습니다.

{{< /history >}}

> [!flag]
이 기능의 사용 가능 여부는 기능 플래그에 의해 제어됩니다. 자세한 내용은 이력을 참조하세요. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

## 인덱싱 파이프라인 {#indexing-pipeline}

GitLab Orbit는 두 가지 소스의 데이터를 인덱싱하고 이를 단일 그래프로 결합합니다.

### SDLC 데이터 {#sdlc-data}

GitLab는 변경 데이터 캡처(CDC) 파이프라인을 통해 변경 이벤트를 [GitLab Data Insights Platform](https://handbook.gitlab.com/handbook/engineering/architecture/design-documents/data_insights_platform/)으로 스트리밍합니다. 이 플랫폼은 레코드를 ClickHouse 테이블에 작성하며, GitLab Orbit는 이를 읽고 그 위에 그래프를 작성합니다.

이는 지속적으로 발생합니다. 사용자가 머지 리퀘스트를 열거나 작업 항목을 생성하거나 파이프라인을 시작하면, 변경 사항이 몇 분 내에 GitLab Orbit 그래프로 전파됩니다.

### 소스 코드 {#source-code}

GitLab Orbit는 GitLab Rails 내부 API를 호출하여 리포지토리에서 소스 파일을 가져옵니다. 각 파일을 언어별 파서로 분석하고, 정의(함수, 클래스, 모듈)와 가져오기 참조를 추출하여 노드와 에지로 그래프에 작성합니다.

코드는 기본 브랜치에서만 인덱싱됩니다. 기본 브랜치가 변경되면 자동으로 다시 인덱싱을 실행합니다.

### 그래프 구성 {#graph-construction}

SDLC 데이터와 코드를 읽은 후 GitLab Orbit는 통합 그래프를 ClickHouse에 작성합니다. 각 엔티티(프로젝트, 사용자, 함수 정의)는 노드가 됩니다. 각 관계(사용자가 머지 리퀘스트를 작성함, 파일이 모듈을 가져옴)는 지향 에지가 됩니다.

쿼리를 보내면 GitLab Orbit는 JSON 쿼리 DSL을 ClickHouse SQL로 컴파일하고, 실행한 후 형식화된 결과를 반환합니다.

## 그래프 모델 {#the-graph-model}

그래프는 두 개의 레이어로 구성됩니다:

- SDLC 레이어: GitLab 객체 및 관계입니다. 프로젝트는 그룹에 속합니다. 사용자가 머지 리퀘스트를 작성합니다. 파이프라인은 프로젝트에서 실행됩니다. 작업 항목이 사용자에게 할당됩니다.
- 코드 레이어: 소스 코드 구조와 파일 간 참조입니다. 함수는 파일에 정의됩니다. 파일은 다른 파일에서 기호를 가져옵니다. 정의는 프로젝트와 브랜치 내에 존재합니다.

두 레이어는 연결되어 있습니다. 머지 리퀘스트(SDLC 레이어)는 파일(코드 레이어)을 건드립니다. 사용자(SDLC 레이어)가 포함된 파일을 마지막으로 수정한 경우, 정의(코드 레이어)를 소유합니다.

## 성능 {#performance}

GitLab Orbit는 별도의 Kubernetes 클러스터에서 실행됩니다. GitLab 인스턴스와 컴퓨팅 또는 메모리를 공유하지 않습니다.

대규모 그룹의 초기 인덱싱(수천 개의 프로젝트, 수백만 줄의 코드)은 몇 분 내에 완료됩니다. 변경 후 증분 다시 인덱싱은 변경 크기에 따라 초 단위에서 분 단위로 완료됩니다.

## 쿼리 실행 {#query-execution}

모든 쿼리는 동일한 경로를 거칩니다:

1. GitLab Orbit는 JSON 쿼리 페이로드를 수신합니다(REST, MCP 또는 GitLab Duo Agent Platform을 통해).
1. 쿼리 엔진은 현재 스키마에 대해 쿼리를 검증합니다.
1. GitLab Orbit는 JSON DSL을 ClickHouse SQL로 컴파일합니다.
1. ClickHouse는 그래프 테이블에 대해 쿼리를 실행합니다.
1. GitLab Orbit는 권한 필터링을 적용하며, 결과는 요청한 사용자가 GitLab에서 액세스할 수 있는 엔티티로 제한됩니다. 자세한 내용은 [보안](security.md)을 참조하세요.
1. GitLab Orbit는 형식화된 JSON 결과를 반환합니다.

`options.include_debug_sql: true`을 설정하여 쿼리 응답의 컴파일된 SQL을 요청할 수 있습니다. 이 필드는 인스턴스 관리자 및 Reporter 이상의 액세스 권한이 있는 직접 GitLab 조직 구성원에게만 채워집니다.

## 데이터 보존 및 삭제 {#data-retention-and-deletion}

그룹에서 GitLab Orbit를 비활성화하면 인덱싱된 데이터는 즉시 삭제되지 않습니다. GitLab Orbit는 30일 동안 데이터를 보관하므로 그래프 기록을 잃지 않고 다시 활성화할 수 있습니다. 유예 기간이 지나면 해당 그룹의 모든 그래프 데이터(모든 노드, 에지 및 인덱싱 체크포인트 포함)가 영구적으로 삭제됩니다.

30일이 지나기 전에 GitLab Orbit를 다시 활성화하면 삭제가 취소되고 인덱싱이 중단된 부분부터 다시 시작됩니다.
