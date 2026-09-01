---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local 및 GitLab Orbit Remote를 사용하여 인덱싱할 수 있는 데이터를 알아봅니다.
title: GitLab Orbit를 사용하여 데이터 인덱싱
---

GitLab Orbit는 코드 리포지토리 및 최상위 그룹에 기반한 SDLC 데이터를 포함한 여러 소스에서 데이터를 인덱싱합니다. GitLab Orbit가 데이터를 인덱싱한 후 지식 그래프를 구축하여 코드베이스 전체의 관계, 종속성 및 구조적 컨텍스트를 검색하는 쿼리를 실행할 수 있습니다.

GitLab Orbit Remote를 사용하여 최상위 그룹 및 해당 하위 그룹과 프로젝트에서 데이터를 인덱싱합니다.

GitLab Orbit Local을 사용하여 모든 로컬 리포지토리의 작업 트리에서 데이터를 인덱싱합니다.

## GitLab Orbit가 인덱싱하는 데이터 {#what-data-gitlab-orbit-indexes}

GitLab Orbit Local 및 GitLab Orbit Remote는 다양한 유형의 데이터를 인덱싱합니다. 다음 섹션에서는 각 기능이 인덱싱하는 내용을 나열합니다.

GitLab Orbit Remote 및 GitLab Orbit Local은 다음을 인덱싱하지 않습니다:

- 바이너리 파일
- 확인된 브랜치(GitLab Orbit Local) 또는 기본 브랜치(GitLab Orbit Remote) 이외의 브랜치

### 소스 코드 {#source-code}

| 코드 구조 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------|--------|
| 파일 및 디렉터리 | {{< yes >}} | {{< yes >}} |
| 함수, 클래스, 메서드 및 모듈 정의 | {{< yes >}} | {{< yes >}} |
| import 선언 | {{< yes >}} | {{< yes >}} |
| 파일 간 기호 참조 | {{< yes >}} | {{< yes >}} |

### 그룹, 프로젝트 및 사용자 {#groups-projects-and-users}

| 그룹, 프로젝트 및 사용자 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| 그룹 | {{< no >}} | {{< yes >}} |
| 프로젝트 | {{< no >}} | {{< yes >}} |
| 사용자 | {{< no >}} | {{< yes >}} |
| 노트 및 댓글 | {{< no >}} | {{< yes >}} |

### 코드 검토 {#code-review}

| 코드 검토 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| 머지 리퀘스트 | {{< no >}} | {{< yes >}} |
| 머지 리퀘스트 diff | {{< no >}} | {{< yes >}} |
| 변경된 파일 | {{< no >}} | {{< yes >}} |

### CI/CD 파이프라인 {#cicd-pipelines}

| CI/CD | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| 파이프라인 | {{< no >}} | {{< yes >}} |
| 스테이지 | {{< no >}} | {{< yes >}} |
| 작업 | {{< no >}} | {{< yes >}} |

### 코드 계획 {#code-planning}

| 코드 계획 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| 이슈 | {{< no >}} | {{< yes >}} |
| 에픽 | {{< no >}} | {{< yes >}} |
| 태스크 | {{< no >}} | {{< yes >}} |
| 인시던트 | {{< no >}} | {{< yes >}} |
| 마일스톤 | {{< no >}} | {{< yes >}} |
| 레이블 | {{< no >}} | {{< yes >}} |

### 보안 {#security}

| 보안 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| 취약점 | {{< no >}} | {{< yes >}} |
| 보안 검결과 | {{< no >}} | {{< yes >}} |
| 보안 스캔 | {{< no >}} | {{< yes >}} |
| 스캐너 | {{< no >}} | {{< yes >}} |
| CVE 식별자 | {{< no >}} | {{< yes >}} |
| CWE 식별자 | {{< no >}} | {{< yes >}} |

### 작업 범위 {#work-scope}

| 범위 | GitLab Orbit Local | GitLab Orbit Remote |
|-----------|-------------------|-------------------|
| 작업 트리(확인된 코드) | {{< yes >}} | {{< no >}} |
| 기본 브랜치만 | {{< no >}} | {{< yes >}} |
| 여러 리포지토리 | {{< yes >}} | {{< no >}} |
| 브랜치 선택 | {{< no >}} | {{< no >}} |

## 지원되는 언어 {#supported-languages}

GitLab Orbit Remote 및 Local은 다음 언어에 대한 데이터를 인덱싱합니다:

| 언어 | 정의 | 파일 간 참조 |
|----------|-------------|----------------------|
| Ruby | {{< yes >}} | {{< yes >}} |
| Java | {{< yes >}} | {{< yes >}} |
| Kotlin | {{< yes >}} | {{< yes >}} |
| Python | {{< yes >}} | {{< yes >}} |
| TypeScript | {{< yes >}} | {{< yes >}} |
| JavaScript | {{< yes >}} | {{< yes >}} |
| Rust | {{< yes >}} | {{< yes >}} |
| Go | {{< yes >}} | {{< yes >}} |
| C# | {{< yes >}} | {{< yes >}} |
| C | {{< yes >}} | {{< yes >}} |
| C++ | {{< yes >}} | {{< yes >}} |
| PHP | {{< yes >}} | {{< yes >}} |
| Bash/Shell | {{< yes >}} | {{< no >}} |
