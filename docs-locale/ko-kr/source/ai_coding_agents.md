---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Orbit 스킬을 설치하여 AI 코딩 에이전트에 기본 제공되는 쿼리 레시피, DSL 지침 및 GitLab Orbit Remote와 GitLab Orbit Local 모두에 대한 문제 해결을 제공합니다."
title: GitLab Orbit 스킬로 AI 코딩 에이전트 설정
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  베타

{{< /details >}}

GitLab Orbit 스킬은 AI 코딩 에이전트에 GitLab Orbit 그래프를 쿼리하기 위한 구조화된 지침을 제공합니다. 포함 내용은 다음과 같습니다:

- **Query recipes** \- 일반적인 질문(영향 범위, 파이프라인 기록, 기여자 패턴)에 대해 바로 붙여넣을 수 있는 JSON 본문입니다.
- **DSL reference** \- 에이전트가 첫 시도에서 유효한 쿼리를 작성할 수 있도록 전체 쿼리 언어를 제공합니다.
- **Troubleshooting** \- 종료 코드, 빈 결과 진단 및 일반적인 함정입니다.
- **Repository map helpers** \- 로컬 체크아웃 또는 GitLab Orbit Remote에서 코드베이스 구조를 요약하는 스크립트입니다.

이 스킬은 [GitLab Orbit Remote](remote/_index.md)와 [GitLab Orbit Local](local/_index.md) 모두에서 작동합니다.

## 전제 조건 {#prerequisites}

- [GitLab CLI(`glab`)](https://docs.gitlab.com/cli/) v1.95.0 이상(다음 포함: `glab skills install`). 하위 명령이 인식되지 않으면 먼저 `glab`를 업데이트하세요.

## 스킬 설치 {#install-the-skill}

전역으로 설치(모든 프로젝트에서 사용 가능):

```shell
glab skills install --global orbit
```

이 명령은 스킬을 `~/.agents/skills/orbit`에 설치합니다.

현재 프로젝트에만 설치:

```shell
glab skills install orbit
```

이 명령은 스킬을 프로젝트 루트의 `.agents/skills/orbit`에 설치합니다.

스킬이 이미 설치된 경우 `glab`은(는) `SKILL.md`이(가) 존재한다고 보고하고 `--force`을(를) 제안하여 덮어씁니다.

## GitLab Orbit 스킬 업데이트 {#update-the-gitlab-orbit-skill}

최신 버전으로 업데이트하려면 `--force`과(와) 함께 install 명령을 다시 실행하세요:

```shell
glab skills install --global --force orbit
```
