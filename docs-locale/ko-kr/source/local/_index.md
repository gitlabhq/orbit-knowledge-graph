---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local - 자신의 머신에서 코드 그래프를 구축하고 쿼리합니다. GitLab 인스턴스가 필요하지 않습니다.
title: GitLab Orbit Local
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 19.0에서 [소개](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되었으며 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)으로 제공됩니다.
- GitLab 19.1에서 [베타](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)로 [변경](https://docs.gitlab.com/policy/development_stages_support/#beta)되었습니다.

{{< /history >}}

GitLab Orbit Local은 전적으로 사용자의 머신에서 실행됩니다. 모든 로컬 리포지토리에 대한 코드 그래프를 구축하고 GitLab Orbit Remote와 동일한 쿼리 언어를 사용하여 쿼리합니다. GitLab 계정이나 네트워크 연결이 필요하지 않습니다.

- 인덱싱: 코드만 포함하며 파일, 정의, 교차 파일 참조를 포함합니다.
- 스토리지: DuckDB (로컬 파일 위치: `~/.orbit/graph.duckdb`)

[GitLab Orbit Local 시작하기](getting-started.md)

## 이 섹션에서 {#in-this-section}

| 페이지 | 설명 |
|---|---|
| [시작하기](getting-started.md) | 액세스 방법을 선택하고 첫 번째 쿼리를 실행합니다 |
| [작동 방식](how-it-works.md) | 인덱싱 파이프라인, 그래프 모델, 쿼리 실행 |
| [GitLab Orbit Local 인덱싱 항목](indexing.md) | 코드 커버리지, 언어 지원, 범위 |
| [스키마 참조](schema.md) | 로컬 코드 그래프의 네 가지 노드 유형 |

## 액세스 방법 {#access-methods}

| 방법 | 설명 |
|---|---|
| [GitLab Orbit CLI (`orbit`)](access/cli.md) | `orbit` 바이너리를 직접 실행하여 인덱싱하고 쿼리합니다 |
| [GitLab CLI (`glab`)](access/glab.md) | `glab orbit local`을 통해 GitLab Orbit Local을 실행합니다 |
| [MCP](access/mcp.md) | Claude Code, Codex 및 기타 에이전트에 로컬 그래프를 노출합니다 |

## 청구 {#billing}

GitLab Orbit Local은 GitLab Credits를 사용하지 않습니다. 모든 처리는 로컬에서 수행됩니다.
