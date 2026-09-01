---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: "GitLab Orbit on GitLab Self-Managed를 위한 필수 조건, 설치 순서, 공유 구성 값."
title: GitLab Self-Managed에서 GitLab Orbit 시작하기
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab Self-Managed
- 상태:  베타

{{< /details >}}

{{< history >}}

- [GitLab](https://gitlab.com/groups/gitlab-org/-/epics/22739) 19.2.2에서 도입되었습니다.

{{< /history >}}

> [!note]
GitLab Orbit on GitLab Self-Managed는 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta) 버전입니다. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

GitLab Orbit는 설치하지 않는 세 가지 시스템에 따라 달라집니다: ClickHouse, Kubernetes, NATS. 나머지 설정 단계는 세 시스템이 모두 존재하고 도달 가능해야 합니다.

## 전제 조건 {#prerequisites}

- GitLab 19.2.2 이상.
- GitLab 인스턴스 및 PostgreSQL 서버에 대한 관리자 액세스.
- PostgreSQL 재시작 한 번을 위한 유지 보수 시간.
- ClickHouse 26.2 이상, GitLab용으로 설정됨.
- Kubernetes 1.33 이상.
- JetStream이 켜진 NATS.

## 설치 순서 {#installation-order}

각 단계는 이전 단계에 따라 달라집니다. 다음 순서로 구성 요소를 설치합니다:

1. GitLab용 ClickHouse를 설정하고 GitLab ClickHouse 마이그레이션을 실행합니다.
1. JetStream이 켜진 상태에서 클러스터에 NATS를 설치합니다.
1. [데이터 복제 설정](data-replication.md): PostgreSQL을 준비한 다음 Siphon을 설치합니다. 이 단계 후 GitLab 행이 ClickHouse 데이터 레이크에 도착합니다.
1. [GitLab Orbit 설정](orbit-setup.md): ClickHouse 아이덴티티를 생성하고, 차트를 설치하고, GitLab을 GitLab Orbit으로 지정하고, 최상위 그룹에 대해 인덱싱을 켭니다.

1단계부터 3단계까지는 GitLab Orbit에 따라 달라지지 않습니다. 각각을 독립적으로 확인할 수 있습니다.

## ClickHouse {#clickhouse}

GitLab Orbit는 ClickHouse 26.2 이상이 필요합니다. 그래프가 전문 인덱스와 해당 릴리스에서 도입된 구체화된 공통 테이블 표현식을 사용하기 때문입니다. GitLab는 모든 25.x 또는 26.x 릴리스를 지원하므로 25.x 릴리스는 GitLab의 나머지 부분에는 사용되지만 GitLab Orbit에는 사용되지 않습니다. ClickHouse에 대한 다른 모든 GitLab 요구 사항은 변경되지 않습니다.

먼저 GitLab용 ClickHouse를 설정합니다. 자세한 내용은 [ClickHouse](https://docs.gitlab.com/integration/clickhouse/)를 참조하세요. [ClickHouse 마이그레이션 실행](https://docs.gitlab.com/integration/clickhouse/#run-clickhouse-migrations) 및 [분석을 위해 ClickHouse 활성화](https://docs.gitlab.com/integration/clickhouse/#enable-clickhouse-for-analytics)를 포함한 해당 페이지의 모든 단계를 완료합니다. 이러한 마이그레이션은 복제가 쓰기 위한 데이터 레이크 테이블을 생성합니다. 이러한 테이블이 없으면 Siphon이 쓸 대상이 없으며 복제가 실패합니다.

GitLab Orbit는 두 개의 데이터베이스를 사용합니다:

| 데이터베이스 | 기록 대상 | 읽기 대상 |
|----------|------------|---------|
| `gitlab_clickhouse_main_production` | GitLab, Siphon | GitLab, GitLab Orbit 인덱서 및 디스패처 |
| `orbit` | GitLab Orbit 디스패처(스키마) 및 인덱서(데이터) | 3개의 GitLab Orbit 구성 요소 모두 |

두 데이터베이스는 별도의 ClickHouse 인스턴스에 있을 수 있습니다. 그렇다면 GitLab, Siphon, GitLab Orbit에 각각 사용하는 데이터베이스를 보유한 인스턴스에 대한 자격 증명을 제공합니다.

`gitlab_clickhouse_main_production` 데이터베이스는 ClickHouse 설정이 완료된 후에 존재합니다. GitLab Orbit를 설정할 때 `orbit` 데이터베이스를 생성합니다. GitLab Orbit는 시작 시 생성하지 않습니다.

### 크기 조정 및 설정 {#sizing-and-settings}

ClickHouse를 위해 최소 8개의 CPU와 32GiB의 메모리를 할당합니다. ClickHouse는 그래프가 구축되는 동안 8개의 코어를 포화시킵니다.

GitLab PostgreSQL 데이터베이스의 크기만큼 최소한 ClickHouse 스토리지를 할당합니다.

직접 실행하는 ClickHouse 인스턴스는 `max_bytes_before_external_sort` 및 `max_bytes_before_external_group_by`을(를) `0`로 설정하여 디스크로의 유출을 끕니다. ClickHouse Cloud는 둘 다 사용 가능한 메모리의 절반으로 설정합니다. 유출 없이 큰 정렬은 전체 결과를 메모리에 보유하고 서버의 메모리가 부족합니다. `default` 프로필에서 둘 다 설정합니다. 다음 값은 32GiB 인스턴스에 적합하며, 각 임계값에 8GiB, 메모리 상한선에 20GiB입니다:

```xml
<profiles>
  <default>
    <max_bytes_before_external_sort>8589934592</max_bytes_before_external_sort>
    <max_bytes_before_external_group_by>8589934592</max_bytes_before_external_group_by>
    <max_memory_usage>21474836480</max_memory_usage>
  </default>
</profiles>
```

## Kubernetes {#kubernetes}

GitLab Orbit는 Kubernetes 1.33 이상이 필요하며, `ImageVolume` 기능 게이트가 켜져 있고 컨테이너 런타임에서 지원됩니다.

GitLab Orbit는 GitLab과 별도의 클러스터에서 실행될 수 있습니다. 해당 클러스터는 GitLab, PostgreSQL, ClickHouse, NATS에 도달할 수 있어야 하며, GitLab은 해당 클러스터에 도달할 수 있어야 합니다.

## NATS {#nats}

Siphon은 변경된 모든 행을 NATS JetStream 스트림에 발행하고, Siphon과 GitLab Orbit 모두 해당 스트림에서 읽습니다. NATS는 JetStream이 켜져 있고 영구 볼륨이 필요합니다.

NATS 서버 `max_payload`을(를) 64MB로 설정합니다. 기본값인 1MB는 일부 GitLab 행보다 작습니다. Siphon은 서버 값을 읽어 행이 전송하기에 너무 큰지 결정합니다.

## 객체 스토리지(선택 사항) {#object-storage-optional}

Siphon은 `max_payload`을(를) 올린 후에도 여전히 너무 큰 행과 함께 스냅샷 이벤트를 객체 스토어에 저장합니다. 기본적으로 Siphon은 NATS JetStream 객체 저장소 버킷을 사용하므로 추가 서비스가 필요하지 않습니다. 해당 트래픽을 JetStream 볼륨에서 제외하거나 별도의 보존 정책을 적용하려면 대신 S3 호환 또는 Google Cloud Storage 버킷을 구성합니다.

GitLab Orbit는 객체 스토리지를 사용하지 않습니다. 그래프는 ClickHouse에 저장되며 다시 인덱싱하여 재구축할 수 있습니다. 인덱서는 리포지토리 체크아웃을 위해 노드 로컬 디스크가 필요합니다. 차트는 이 디스크를 임시 스토리지 요청으로 크기 조정합니다.

## 공유 구성 값 {#shared-configuration-values}

이러한 각 값을 한 번 선택하고 이를 읽는 모든 구성 요소에서 동일한 값을 사용합니다. 구성 요소는 이러한 값을 다른 값과 검증하지 않습니다. 일치하지 않으면 영향을 받는 구성 요소가 시작되지만 데이터를 처리하지 않습니다.

| 값 | 사용 대상 | 예제 |
|-------|---------|---------|
| NATS 스트림 이름 | Siphon 및 GitLab Orbit | `siphon_stream_main_db` |
| 데이터 레이크 데이터베이스 | GitLab, Siphon, GitLab Orbit | `gitlab_clickhouse_main_production` |
| 그래프 데이터베이스 | GitLab Orbit | `orbit` |
| PostgreSQL 호스트 및 포트 | Siphon | `postgres.example.com:5432` |
| 클러스터에서 도달 가능한 GitLab URL | GitLab Orbit | `https://gitlab.example.com` |
| GitLab에서 도달 가능한 GitLab Orbit gRPC 엔드포인트 | GitLab | `tls://orbit.example.com:50054` |

GitLab Orbit는 기본적으로 스트림 이름 `siphon_stream_main_db`을(를) 예상합니다. 다른 이름을 사용하려면 `stream_name`를 `siphon-values.yaml`에서 설정하고 `schedule.tasks.siphon.events_stream_name`를 `orbit-values.yaml`에서 설정합니다.

## 다음 단계 {#next-step}

- [데이터 복제 설정](data-replication.md)
