---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: GitLab Orbit Local 및 GitLab Orbit Remote에서 발생할 수 있는 일반적인 오류를 해결합니다.
title: GitLab Orbit 문제 해결
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 19.1에서 [도입](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/661)되었습니다.

{{< /history >}}

이 페이지를 사용하여 [GitLab Orbit Local](local/_index.md) 또는 [GitLab Orbit Remote](remote/_index.md)에서 발생할 수 있는 오류를 해결합니다.

## GitLab Orbit Local {#gitlab-orbit-local}

GitLab Orbit Local 오류는 `orbit` 바이너리를 직접 실행하거나 `glab orbit local`를 통해 실행할 때 발생합니다.

### `no local graph found` {#no-local-graph-found}

**Symptoms:**

```plaintext
Error: no local graph found at ~/.orbit/graph.duckdb. Run `orbit index` first.
```

**Cause:** 리포지토리가 아직 인덱싱되지 않았거나 지정한 `--db` 경로가 존재하지 않습니다. GitLab Orbit Local의 이전 버전에서는 이 오류가 `Table 'Definition' does not exist`로 보고되었습니다.

**Resolution:** 먼저 리포지토리를 인덱싱합니다:

```shell
glab orbit local index /path/to/your/repo
```

### `IO Error: Could not set lock on file` {#io-error-could-not-set-lock-on-file}

**Symptoms:** 명령이 잠시 일시 중지된 후 `Could not set lock on file`을 포함하는 오류로 실패합니다.

**Cause:** 다른 `orbit` 프로세스가 이미 실행 중이며 DuckDB 쓰기 잠금을 유지하고 있습니다. GitLab Orbit는 지수 백오프로 자동으로 다시 시도하지만, 재시도 윈도우 내에 잠금이 해제되지 않으면 실패합니다.

**Resolution:** 다른 프로세스가 완료될 때까지 기다리거나 중지합니다:

```shell
pkill orbit
```

그 다음 명령을 다시 시도합니다.

### `list_contains source_tags` {#list_contains-source_tags}

**Symptoms:** 쿼리가 `list_contains source_tags`을 포함하는 오류로 실패합니다.

**Cause:** `source_tags` 속성을 포함하는 특정 필터 조합으로 인해 발생하는 알려진 버그입니다.

**Resolution:** 쿼리에서 `source_tags` 필터를 제거하고 다시 시도합니다.

### `error: unrecognized subcommand 'mcp'` {#error-unrecognized-subcommand-mcp}

**Symptoms:**

```plaintext
error: unrecognized subcommand 'mcp'
```

**Cause:** `orbit mcp serve` 하위 명령은 아직 구현되지 않았습니다. GitLab Orbit Local의 MCP 지원은 로드맵에 있지만 현재 릴리스에서는 사용할 수 없습니다.

**Resolution:** [지원되는 액세스 방법](local/_index.md) 중 하나를 사용합니다.

## GitLab Orbit Remote {#gitlab-orbit-remote}

GitLab Orbit Remote 오류는 `glab orbit remote` 명령을 실행할 때 발생합니다. GitLab Orbit Remote에는 GitLab Premium 또는 Ultimate와 인스턴스에서 활성화된 `knowledge_graph` 기능 플래그가 필요합니다.

### 종료 코드 2 {#exit-code-2}

**Symptoms:** `glab orbit remote` 명령이 코드 2로 종료됩니다.

**Cause:** `knowledge_graph` 기능 플래그가 네임스페이스 또는 인스턴스에 대해 활성화되지 않았습니다.

**Resolution:** GitLab 관리자에게 문의하여 네임스페이스에 대해 `knowledge_graph` 기능 플래그를 활성화합니다.

### 종료 코드 3 {#exit-code-3}

**Symptoms:** `glab orbit remote` 명령이 코드 3으로 종료됩니다.

**Cause:** GitLab CLI로 인증되지 않았습니다.

**Resolution:** 로그인합니다:

```shell
glab auth login
```

### `insufficient_scope` MCP 끝점에서 {#insufficient_scope-on-the-mcp-endpoint}

**Symptoms:** GitLab Orbit MCP 끝점으로 연결하지 못하고 `insufficient_scope`이 표시됩니다.

**Cause:** 개인 액세스 토큰 또는 OAuth 토큰에 `mcp_orbit` 범위가 포함되지 않았습니다. `read_api` 범위 만으로는 MCP 전송에 충분하지 않습니다.

**Resolution:** `mcp_orbit` 범위로 새 토큰을 생성하거나 다시 인증하여 추가 범위를 부여합니다.
