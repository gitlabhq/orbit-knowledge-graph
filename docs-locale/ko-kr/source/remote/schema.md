---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "6개 도메인에 걸친 27개 GitLab Orbit 노드 유형의 전체 참조이며, 속성 및 해당 유형을 포함합니다."
title: 스키마 참고자료
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

GitLab Orbit은 6개 도메인에 걸친 27개의 노드 유형을 인덱싱합니다. 이를 쿼리의 엔터티 이름으로 사용합니다.

언제든지 라이브 스키마를 가져오려면:

```shell
glab orbit remote schema
```

## 핵심 {#core}

| 노드 유형 | 설명 | 주요 속성 |
|-----------|-------------|----------------|
| `Group` | GitLab 그룹 또는 서브그룹 | `id`, `full_path`, `name`, `visibility`, `traversal_path` |
| `Project` | GitLab 프로젝트 및 리포지토리 | `id`, `full_path`, `name`, `visibility`, `archived`, `star_count` |
| `User` | GitLab 사용자 계정 | `id`, `username`, `email`, `name`, `state`, `is_admin` |
| `Note` | GitLab 객체에 대한 댓글 또는 주석 | `id`, `note`, `noteable_type`, `noteable_id`, `internal`, `confidential` |

## 소스 코드 {#source-code}

| 노드 유형 | 설명 | 주요 속성 |
|-----------|-------------|----------------|
| `Branch` | Git 브랜치 | `id`, `project_id`, `name`, `is_default` |
| `Definition` | 함수, 클래스, 메서드 또는 모듈 정의 | `id`, `file_path`, `fqn`, `name`, `definition_type`, `start_line`, `end_line`, `content` |
| `Directory` | 리포지토리의 디렉터리 | `id`, `project_id`, `path`, `name` |
| `File` | 소스 코드 파일 | `id`, `path`, `name`, `extension`, `language`, `content` |
| `ImportedSymbol` | 가져오기 또는 파일 간 기호 참조 | `id`, `file_path`, `import_type`, `import_path`, `identifier_name` |

## 코드 검토 {#code-review}

| 노드 유형 | 설명 | 주요 속성 |
|-----------|-------------|----------------|
| `MergeRequest` | 머지 리퀘스트 | `id`, `iid`, `title`, `description`, `source_branch`, `target_branch`, `state`, `draft`, `squash` |
| `MergeRequestDiff` | 머지 리퀘스트의 변경 사항 스냅샷 | `id`, `merge_request_id`, `commits_count`, `files_count` |
| `MergeRequestDiffFile` | 머지 리퀘스트 diff에서 변경된 파일 | `id`, `new_path`, `old_path`, `new_file`, `renamed_file`, `deleted_file` |

## CI/CD {#cicd}

| 노드 유형 | 설명 | 주요 속성 |
|-----------|-------------|----------------|
| `Pipeline` | CI/CD 파이프라인 실행 | `id`, `sha`, `ref`, `status`, `source`, `duration`, `failure_reason` |
| `Stage` | 파이프라인 스테이지 | `id`, `name`, `status`, `position` |
| `Job` | CI/CD 작업 | `id`, `name`, `status`, `ref`, `allow_failure`, `environment`, `failure_reason` |
| `Deployment` | 커밋의 CI/CD 배포 | `id`, `iid`, `status`, `ref`, `sha`, `environment_id` |
| `Environment` | CI/CD 배포 대상 | `id`, `name`, `state`, `tier`, `external_url` |
| `Runner` | CI/CD 러너 | `id`, `runner_type`, `name`, `active`, `locked` |

## 계획 {#planning}

| 노드 유형 | 설명 | 주요 속성 |
|-----------|-------------|----------------|
| `WorkItem` | 이슈, 에픽, 작업, 인시던트 또는 기타 작업 항목 | `id`, `iid`, `title`, `description`, `state`, `work_item_type`, `due_date`, `weight` |
| `Milestone` | 마일스톤 | `id`, `title`, `state`, `due_date`, `start_date` |
| `Label` | 작업 분류를 위한 레이블 | `id`, `title`, `color` |

## 보안 {#security}

| 노드 유형 | 설명 | 주요 속성 |
|-----------|-------------|----------------|
| `Finding` | `security_findings`에서의 보안 스캔 발견 | `id`, `uuid`, `name`, `description`, `severity`, `deduplicated` |
| `SecurityScan` | 파이프라인에서의 보안 스캔 실행 | `id`, `scan_type`, `status`, `latest` |
| `Vulnerability` | 확인되거나 잠재적인 보안 취약성 | `id`, `title`, `state`, `severity`, `report_type`, `resolved_on_default_branch` |
| `VulnerabilityIdentifier` | CVE, CWE 또는 기타 외부 참조 | `id`, `external_type`, `external_id`, `name`, `url` |
| `VulnerabilityOccurrence` | 취약성의 특정 발생(`Vulnerabilities::Finding` in Rails) | `id`, `uuid`, `severity`, `report_type`, `detection_method`, `cve`, `location` |
| `VulnerabilityScanner` | 보안 스캐너 | `id`, `external_id`, `name`, `vendor` |

## 참고사항 {#notes}

- 정의 ID는 프로젝트 및 브랜치 범위의 콘텐츠 해시 정수입니다. 다른 프로젝트의 동일한 기호에 대한 두 정의는 함수 이름과 파일 경로가 동일해도 다른 ID를 갖습니다.
- 모든 엔터티 ID는 기본 값이 정수인 경우에도 쿼리 응답에서 문자열로 반환됩니다. 이는 `Number.MAX_SAFE_INTEGER` 이상의 값에 대해 JavaScript 클라이언트에서 정밀도 손실을 방지합니다.
- `content` 필드는 `Definition` 및 `File` 노드에 정의 또는 파일의 전체 소스 텍스트를 포함합니다. 이러한 필드는 GitLab에 대한 별도의 API 호출을 하지 않고 파일 콘텐츠를 수화해야 하는 에이전트 도구에 사용할 수 있습니다.
- 모든 노드는 인증 필터링에 사용되는 `traversal_path` 속성을 포함합니다. 쿼리 결과는 요청하는 사용자가 액세스할 수 있는 엔터티로 자동 범위 지정됩니다.
