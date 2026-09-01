---
stage: Orbit
group: Context Systems
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: 접근 방법을 선택하고 첫 번째 로컬 GitLab Orbit 그래프를 구축합니다.
title: GitLab Orbit Local 시작하기
---

{{< details >}}

- 티어: Free, Premium, Ultimate
- 제공 서비스: GitLab.com, GitLab Self-Managed, GitLab Dedicated
- 상태:  베타

{{< /details >}}

{{< history >}}

- GitLab 19.0에서 [도입](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되었으며 [실험](https://docs.gitlab.com/policy/development_stages_support/#experiment)입니다.
- GitLab 19.1에서 [변경](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/324)되어 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta) 단계가 되었습니다.

{{< /history >}}

GitLab Orbit Local은 당신의 머신에서 실행됩니다. `orbit` 바이너리를 설치하고 당신의 작업 방식에 맞는 접근 방법을 선택한 후 첫 번째 쿼리를 실행합니다.

## 설치 {#install}

`orbit` 바이너리를 원라인 설치관리자로 직접 설치하거나 npm에서 설치하거나 이미 사용 중인 경우 GitLab CLI(`glab`)를 통해 설치합니다.

Linux에서는 설치관리자가 기본적으로 glibc 아카이브를 사용하고 Alpine과 같은 musl 기반 배포판에서 완전히 정적인 musl 아카이브를 자동으로 선택합니다. 정적 Linux 아카이브를 강제하려면 `--libc musl`을 전달합니다.

{{< tabs >}}

{{< tab title="macOS 및 Linux" >}}

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash
```

정적 musl 바이너리를 명시적으로 설치하려면 (예: glibc 시스템에서):

```shell
curl -fsSL "https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.sh" | bash -s -- --libc musl
```

새 터미널을 열고 확인합니다:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="Windows" >}}

```powershell
irm https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/raw/main/install.ps1 | iex
```

새 터미널을 열고 확인합니다:

```shell
orbit help
```

엔드포인트 정책이 원격 스크립트 실행을 제한하는 경우 스크립트를 실행하지 않고 설치할 수 있습니다. Windows 릴리스 아카이브에는 GitLab Inc.에서 서명한 단일 자체 포함 `orbit.exe`, 애플리케이션 허용 목록 정책이 게시자별로 이를 승인할 수 있습니다:

1. [최신 릴리스](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/releases)에서 `orbit-local-windows-x86_64.zip`와 해당 `.sha256` 파일을 다운로드합니다.
1. 체크섬을 확인합니다:

   ```powershell
   (Get-FileHash .\orbit-local-windows-x86_64.zip -Algorithm SHA256).Hash
   ```

   출력이 `.sha256` 파일의 해시와 일치해야 합니다. 비교는 대소문자를 구분하지 않습니다: `Get-FileHash`는 대문자를 반환하고 `.sha256` 파일은 소문자를 저장합니다.
1. 웹의 표시를 지웁니다. 브라우저가 다운로드에 연결하고 추출이 `orbit.exe`로 이동한 후 첫 실행 시 SmartScreen 프롬프트가 표시되거나 정책에 의해 차단될 수 있습니다:

   ```powershell
   Unblock-File .\orbit-local-windows-x86_64.zip
   ```

1. 아카이브를 추출하고 대상 디렉토리를 만든 후 `orbit.exe`을 이동합니다. 이 예제는 `$env:LOCALAPPDATA\Programs\orbit`를 사용하며 설치관리자가 사용하는 기본값과 동일합니다:

   ```powershell
   Expand-Archive -Path .\orbit-local-windows-x86_64.zip -DestinationPath .
   New-Item -ItemType Directory -Force -Path "$env:LOCALAPPDATA\Programs\orbit"
   Move-Item .\orbit.exe "$env:LOCALAPPDATA\Programs\orbit\orbit.exe"
   ```

1. 해당 디렉토리가 아직 `PATH`에 없으면 사용자를 위해 추가합니다:

   ```powershell
   [Environment]::SetEnvironmentVariable("PATH", "$env:LOCALAPPDATA\Programs\orbit;$([Environment]::GetEnvironmentVariable('PATH', 'User'))", "User")
   ```

관리자 권한이 필요하지 않습니다.

새 터미널을 열고 확인합니다:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="npm" >}}

모든 플랫폼에서 npm으로도 설치할 수 있습니다:

```shell
npm install -g @gitlab/orbit
```

[`@gitlab/orbit`](https://www.npmjs.com/package/@gitlab/orbit) 패키지는 당신의 플랫폼용 미리 빌드된 바이너리를 설치합니다. Linux에서는 항상 glibc 및 musl 배포판 모두에서 실행되는 완전히 정적인 musl 바이너리를 사용합니다.

확인:

```shell
orbit help
```

{{< /tab >}}

{{< tab title="GitLab CLI (glab)" >}}

이미 [`glab`](https://gitlab.com/gitlab-org/cli)가 설치되어 있으면:

```shell
glab orbit local --install
```

확인:

```shell
glab orbit local help
```

자세한 내용은 [`glab orbit local` 참조](https://docs.gitlab.com/cli/orbit/local/)를 참조하세요.

{{< /tab >}}

{{< /tabs >}}

## 접근 방법 선택 {#pick-an-access-method}

| 방법 | 최적용 | 설정 |
|---|---|---|
| [GitLab Orbit CLI (`orbit`)](access/cli.md) | 직접 CLI 사용, 스크립팅, 인덱싱 작업 | 원라인 설치관리자 또는 `glab orbit local --install` |
| [GitLab CLI (`glab`)](access/glab.md) | `glab`을 이미 사용 중인 모든 사람 | `glab orbit local --install` |
| [MCP](access/mcp.md) | Claude Code, Codex 및 기타 AI 에이전트 | `claude mcp add orbit-local -- orbit mcp serve` |

세 가지 모두 동일한 로컬 그래프를 읽습니다. GitLab Orbit Local은 DuckDB SQL로 쿼리되며 구조화된 JSON 쿼리 DSL은 [GitLab Orbit Remote](../remote/_index.md)에만 해당됩니다.

## 60초 빠른 시작 {#60-second-quickstart}

> [!note]
`glab orbit local`는 관리되는 `orbit` 바이너리를 래핑합니다. 바이너리는 다운로드되고 체크섬이 확인되며 처음 사용 시 최신 상태로 유지됩니다. `glab` 1.94 이상이 필요합니다. 대신 바이너리를 직접 실행하려면 [`orbit` CLI 직접 사용](access/cli.md)을 참조하세요.

리포지토리를 인덱싱하고 GitLab Orbit이 찾은 내용을 검사합니다:

```shell
glab orbit local index /path/to/your/repo
glab orbit local schema
```

이것은 `~/.orbit/graph.duckdb`에서 로컬 DuckDB 그래프를 구축하고 모든 테이블과 열을 인쇄합니다: `gl_definition`, `gl_file`, `gl_directory`, `gl_imported_symbol`, `gl_edge` 및 `_orbit_manifest` 기록 관리 테이블.

다음:

- 실제 쿼리 실행: [glab과 함께 GitLab Orbit Local 사용](access/glab.md).
- AI 에이전트에 연결: `glab orbit setup`을 실행하여 GitLab Orbit 스킬을 설치하거나 [MCP를 통해 연결](access/mcp.md)합니다.
- 테이블 레이아웃 찾아보기: [스키마 참조](schema.md).

## 청구 {#billing}

GitLab Orbit Local은 GitLab Credits를 사용하지 않습니다. 모든 처리는 로컬입니다.

## 다음으로 시도할 내용 {#what-to-try-next}

- [GitLab Orbit Local이 인덱싱하는 것](indexing.md) \- 언어 및 범위 커버리지.
- [스키마 참조](schema.md) \- 로컬 그래프의 4가지 노드 유형.
- [요리책](../remote/cookbook.md) \- 복사-붙여넣기 쿼리 (코드 전용 쿼리는 Local에 적용됨).
- [GitLab Orbit Remote 시작하기](../remote/getting-started.md) \- 전체 GitLab 인스턴스를 쿼리합니다.
