---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit CLI이나 GitLab Orbit Local MCP 서버와 같은 사용 가능한 도구를 사용하여 로컬 그래프와 상호 작용합니다.
title: 도구 연결
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

GitLab Orbit Local은 로컬 그래프에 액세스하고 코딩 에이전트와 상호 작용하는 데 사용할 수 있는 도구를 제공합니다.

## GitLab Orbit CLI을 사용하는 GitLab Orbit Local {#gitlab-orbit-local-with-the-gitlab-orbit-cli}

GitLab Orbit CLI을 사용하면 GitLab 인스턴스에 연결하지 않고도 로컬 그래프를 빌드하고 쿼리할 수 있습니다.

GitLab Orbit Local MCP 서버를 사용하여 AI 에이전트에 그래프를 노출하고 AI 코딩 어시스턴트를 구성하여 그래프를 참조하도록 할 수 있습니다.

## GitLab CLI을 사용하는 GitLab Orbit Local {#gitlab-orbit-local-with-the-gitlab-cli}

GitLab CLI를 확장하여 GitLab Orbit CLI 명령을 설치하고 실행합니다. GitLab CLI는 또한 업데이트를 관리하고 쿼리 레시피, DSL 지침 및 문제 해결 도움말로 에이전트를 업데이트하는 GitLab Orbit 스킬을 설치합니다.

## GitLab Orbit Local MCP 서버 {#gitlab-orbit-local-mcp-server}

GitLab Orbit Local MCP 서버를 AI 클라이언트에 연결합니다. 클라이언트에 연결한 후 에이전트를 구성하여 로컬 그래프와 상호 작용할 수 있습니다.
