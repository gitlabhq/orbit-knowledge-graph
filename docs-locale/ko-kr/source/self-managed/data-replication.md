---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: PostgreSQL 논리적 복제를 구성하고 Siphon을 설치하여 GitLab 데이터가 ClickHouse에 도달하도록 합니다.
title: 데이터 복제 설정
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

GitLab Orbit은 GitLab 데이터베이스 자체가 아닌 ClickHouse의 GitLab 데이터베이스 복사본에서 GitLab 데이터를 읽습니다. [Siphon](https://gitlab.com/gitlab-org/analytics-section/siphon)은 해당 복사본을 최신 상태로 유지합니다.

Siphon은 Kubernetes에서 3개의 배포로 실행됩니다:

| 배포 | 역할 |
|------------|------|
| Producer | PostgreSQL 미리 쓰기 로그를 읽고 변경된 각 행을 NATS에 게시합니다. |
| Consumer | NATS를 읽고 행을 ClickHouse에 씁니다. |
| Reconciler | 네임스페이스 순회 경로와 같은 파생 열을 일정에 따라 다시 계산하고 영향을 받는 행을 NATS를 통해 다시 게시합니다. |

전제 조건:

- GitLab Self-Managed의 GitLab Orbit을 위한 [전제 조건](getting-started.md#prerequisites).
- GitLab용으로 설정된 ClickHouse 인스턴스, GitLab ClickHouse 마이그레이션이 적용됨. 자세한 내용은 [ClickHouse](getting-started.md#clickhouse)를 참조하세요.
- GitLab PostgreSQL 서버에 대한 슈퍼유저 액세스.
- PostgreSQL 재시작 한 번을 위한 유지 보수 시간.
- Helm 3 및 `kubectl` 클러스터 액세스.

다음 순서로 복제를 설정합니다:

1. PostgreSQL에서 논리적 복제를 켭니다.
1. Siphon PostgreSQL 사용자를 생성합니다.
1. 게시 및 권한을 생성합니다.
1. Siphon ClickHouse 사용자를 생성합니다.
1. Siphon에 비밀번호를 제공합니다.
1. Siphon을 설치합니다.

## PostgreSQL에서 논리적 복제 켜기 {#turn-on-logical-replication-in-postgresql}

Siphon 생산자는 `wal_level`이 `logical`이 아니면 시작 시 중지됩니다. `wal_level`, `max_replication_slots`, `max_wal_senders` 변경 사항은 모두 PostgreSQL 재로드가 아닌 전체 재시작이 필요하며, `gitlab-ctl reconfigure`은 PostgreSQL을 재시작하지 않습니다.

{{< tabs >}}

{{< tab title="Linux 패키지(Omnibus)" >}}

1. `/etc/gitlab/gitlab.rb`을 편집합니다.

   ```ruby
   postgresql['wal_level'] = 'logical'
   postgresql['max_replication_slots'] = 10
   postgresql['max_wal_senders'] = 10

   # Accept connections from the cluster. Replace with the address and CIDR for your network.
   postgresql['listen_address'] = '0.0.0.0'
   postgresql['md5_auth_cidr_addresses'] = ['10.0.0.0/8']

   # Keep GitLab itself on the local socket. Without this, setting listen_address
   # also repoints GitLab at that address, and GitLab loses its database connection.
   gitlab_rails['db_host'] = '/var/opt/gitlab/postgresql'
   ```

1. 파일을 저장하고 GitLab을 재구성한 후 PostgreSQL을 다시 시작합니다:

   ```shell
   sudo gitlab-ctl reconfigure
   sudo gitlab-ctl restart postgresql
   ```

1. 설정을 확인합니다:

   ```shell
   sudo gitlab-psql -c 'SHOW wal_level'
   sudo gitlab-psql -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level`은 `logical`을 반환하고 나머지 두 개는 `gitlab.rb`와 일치하며, 행 중 `pending_restart = t`을 보고하지 않습니다. 부분적으로 적용된 조합은 PostgreSQL이 다음 재시작 시 시작되지 않게 합니다. 복구하려면 `/etc/gitlab/gitlab.rb`을 수정하고 재구성한 후 서비스가 실행 중인지 확인합니다.

{{< /tab >}}

{{< tab title="Helm 차트(Kubernetes)" >}}

GitLab Helm 차트는 프로덕션 PostgreSQL을 관리하지 않으므로 자신의 서버 또는 관리형 데이터베이스에서 이러한 설정을 적용합니다.

1. 다음 매개변수를 설정합니다:

   | 매개변수 | 값 |
   |-----------|-------|
   | `wal_level` | `logical` |
   | `max_replication_slots` | `10` 이상 |
   | `max_wal_senders` | `10` 이상 |

   관리형 데이터베이스에서 `postgresql.conf` 대신 제공자의 매개변수 그룹을 통해 설정합니다. 일부 공급자는 `wal_level`을 논리적 복제 플래그와 같은 다른 이름으로 노출합니다.

1. 서버를 다시 시작합니다.

1. 클러스터에서 포트 5432에 도달하는 연결을 허용합니다.

1. 설정을 확인합니다:

   ```shell
   psql -h <postgresql_host> -U <admin_user> -d gitlabhq_production \
     -c "SELECT name, setting, pending_restart FROM pg_settings WHERE name IN ('wal_level','max_wal_senders','max_replication_slots')"
   ```

   `wal_level`은 `logical`를 반환하고, 나머지 두 개는 설정한 값과 일치하며, 행 중 `pending_restart = t`을 보고하지 않습니다.

{{< /tab >}}

{{< /tabs >}}

## Siphon PostgreSQL 사용자 생성 {#create-the-siphon-postgresql-users}

Siphon은 3개의 역할을 사용합니다. 슈퍼유저로 생성합니다. 역할은 이미 `REPLICATION` 속성을 가지고 있고 GitLab 애플리케이션 역할이 이를 갖지 않은 경우에만 `REPLICATION`을 부여할 수 있습니다.

슈퍼유저로 `gitlabhq_production`에 연결하고 실행합니다:

```sql
CREATE USER siphon WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_replicator WITH REPLICATION LOGIN PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE USER siphon_snapshot WITH PASSWORD '<your_password>'
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
```

3개의 역할은 동일한 비밀번호를 사용할 수 있습니다.

이 절차의 값 파일을 사용하면 Siphon은 모든 연결에 대해 `siphon_replicator`으로 연결합니다. 다른 두 역할은 다음 단계의 Rake 작업이 이들에게 읽기 액세스를 부여하기 때문에 존재합니다. 분할 사용자 설정은 나중에 이들을 사용할 수 있습니다.

## 게시 및 권한 생성 {#create-the-publication-and-grants}

GitLab은 PostgreSQL을 Siphon용으로 준비하는 Rake 작업을 제공합니다. 작업은 멱등성을 가지며 다음을 생성합니다:

- 게시.
- Siphon이 호출하여 게시에 테이블을 추가하는 도우미 함수.
- 모든 GitLab 스키마에 대한 읽기 액세스.

GitLab 19.2.2 이상에서는 이 작업을 포함합니다.

{{< tabs >}}

{{< tab title="Linux 패키지(Omnibus)" >}}

1. 작업이 있는지 확인합니다:

   ```shell
   sudo gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. 작업을 실행합니다:

   ```shell
   sudo gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< tab title="Helm 차트(Kubernetes)" >}}

1. 도구 상자 배포를 찾습니다:

   ```shell
   kubectl -n <gitlab_namespace> get deploy -l app=toolbox
   ```

1. 작업이 있는지 확인합니다:

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake -T | grep gitlab:siphon:setup
   ```

1. 작업을 실행합니다:

   ```shell
   kubectl -n <gitlab_namespace> exec -it deploy/<release>-toolbox -- gitlab-rake gitlab:siphon:setup
   ```

{{< /tab >}}

{{< /tabs >}}

작업은 `siphon_publication_main_1` 이름의 게시를 생성하고 `siphon` 역할에 `public.siphon_alter_publication`의 `EXECUTE`을 부여합니다. 생산자는 `siphon_replicator` 연결에서 함수를 호출하므로 `siphon_replicator`에 동일한 `EXECUTE` 권한을 부여합니다. 슈퍼유저로 실행합니다:

```sql
GRANT EXECUTE ON FUNCTION public.siphon_alter_publication(text, text, integer)
  TO siphon_replicator;
```

생산자가 시작될 때까지 게시는 비어 있습니다. 그러면 Siphon은 동일한 함수를 통해 게시에 테이블을 추가합니다.

## Siphon ClickHouse 사용자 생성 {#create-the-siphon-clickhouse-user}

Siphon은 GitLab이 이미 사용하는 동일한 데이터베이스에 씁니다. Siphon은 GitLab ClickHouse 마이그레이션이 대상 테이블을 생성하기 때문에 스키마 권한이 필요하지 않습니다.

관리자로 ClickHouse에 연결하고 실행합니다:

```sql
CREATE USER siphon IDENTIFIED WITH sha256_password BY '<your_password>';
CREATE ROLE siphon_app;
GRANT SELECT, INSERT, dictGet ON gitlab_clickhouse_main_production.* TO siphon_app;
GRANT siphon_app TO siphon;
```

`dictGet` 권한은 필수입니다. 이 없으면 포드는 여전히 상태 확인을 통과합니다. 프로브는 메트릭 엔드포인트만 스크래핑하기 때문입니다. 실패는 소비자 로그의 권한 오류로 나타납니다. 사전으로 지원하는 모든 테이블은 비어 있습니다.

`system` 데이터베이스를 제한하는 관리형 ClickHouse에서도 실행합니다:

```sql
GRANT SELECT ON system.tables, system.columns TO siphon_app;
```

## Siphon이 비밀번호에 접근할 수 있도록 설정 {#make-the-passwords-available-to-siphon}

Siphon은 Kubernetes Secret으로 지원되는 환경 변수에서 두 비밀번호를 읽습니다. Secret을 생성하기 전에 네임스페이스가 있어야 합니다. 다음 섹션의 값 파일은 Siphon 네임스페이스에 `siphon-secrets` 이름의 하나의 Secret을 예상하며, 다음 키가 있습니다:

| 키 | 보유 |
|-----|-------|
| `pg-password` | `siphon`, `siphon_replicator`, `siphon_snapshot` PostgreSQL 역할의 비밀번호 |
| `ch-siphon-password` | ClickHouse `siphon` 사용자의 비밀번호 |

값을 이미 실행 중인 비밀 관리자에 유지해야 합니다. External Secrets Operator와 같은 도구로 클러스터에 동기화합니다. 평문을 다른 곳에 저장하지 마세요.

## Siphon 설치 {#install-siphon}

`configMode: split`을 사용하면 Siphon은 GitLab과 함께 제공되는 이미지에서 포드 시작 시 테이블 목록을 작성합니다. 목록은 GitLab 버전과 일치하며 수동 업데이트가 필요하지 않습니다.

`global.gitlabVersion`은 `gitlab-siphon-tables` 이미지의 태그이며, 복제된 테이블 집합을 GitLab 버전에 고정합니다. 태그는 `v19.2.0-ee`부터 시작합니다. 설치 전에 [컨테이너 레지스트리](https://gitlab.com/gitlab-org/gitlab/container_registry)에 태그가 있는지 확인하세요. 태그가 없으면 포드가 시작되지 않기 때문입니다.

1. 다음을 `siphon-values.yaml`로 저장하고 자리 표시자를 바꿉니다:

   ```yaml
   configMode: split

   global:
     # Tag of the gitlab-siphon-tables image.
     # Must match your GitLab version exactly, patch level included.
     gitlabVersion: v19.2.2-ee

   image:
     repository: registry.gitlab.com/gitlab-org/analytics-section/siphon
     tag: 0.0.124-beta

   siphonConnectionConfigMap:
     create: true
     data:
       prometheus:
         port: 8080
       connection:
         queueing:
           driver: nats
           url: nats://nats.nats.svc.cluster.local:4222
           stream_name: siphon_stream_main_db
           nats_config:
             replicas: 1
             max_age_seconds: 1296000
         clickhouse:
           host: <clickhouse_host>
           # Native protocol, not the HTTP port GitLab uses.
           port: 9000
           ssl: false
           username: siphon
           password: "${CLICKHOUSE_SIPHON_PASSWORD}"
           database: gitlab_clickhouse_main_production
         databases:
           main:
             host: <postgresql_host>
             port: 5432
             database: gitlabhq_production
             user: siphon_replicator
             password: "${SIPHON_DB_PASSWORD}"
             ssl_mode: require
             advisory_lock_id: 1
             application_name: siphon_main_1
             advisory_lock_timeout_ms: 100
             advisory_lock_timeout_fuzziness_ms: 50
             lock_timeout_ms: 500
             lock_timeout_fuzziness_ms: 300
       overrides:
         siphon_main_1:
           database_ref: main

   siphonLayoutConfigMap:
     create: true
     data:
       stream_name: siphon_stream_main_db
       refresh_mode: inline
       partitions_monitoring_interval_in_seconds: 3600
       max_column_size_in_bytes: 10485760
       producers:
         main:
           - siphon_main_1
       consumers:
         - siphon_consumer_1
       reconcilers:
         - siphon_reconciler_1
       # Point the producer at the publication the Rake task created. Without this
       # override, the producer derives the name from the application identifier
       # and tries to create its own.
       replication_overrides:
         siphon_main_1:
           publication_name: siphon_publication_main_1
       # GitLab splits its schema three ways even on one database. Map ci and sec onto main.
       database_mapping:
         ci: main
         sec: main

   deployments:
     postgres-producer:
       configMode: split
       split:
         role: producer
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
     clickhouse-consumer:
       configMode: split
       split:
         role: consumer
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
     reconciler:
       configMode: split
       split:
         role: reconciler
       envFromSecrets:
         - {name: SIPHON_DB_PASSWORD, secretName: siphon-secrets, secretKey: pg-password}
         - {name: CLICKHOUSE_SIPHON_PASSWORD, secretName: siphon-secrets, secretKey: ch-siphon-password}
   ```

1. 차트를 설치합니다:

   ```shell
   helm repo add siphon https://gitlab.com/api/v4/projects/76780115/packages/helm/stable
   helm repo update

   helm upgrade --install siphon siphon/siphon \
     --version 1.18.0 \
     --namespace siphon \
     --create-namespace \
     --values siphon-values.yaml
   ```

   이 명령은 직접 Helm 설치에 대한 참조입니다. 네임스페이스 이름과 배포 방법을 자신의 도구에 맞게 조정합니다.

1. 3개의 배포가 모두 실행 중인지 확인합니다:

   ```shell
   kubectl -n siphon get pods
   ```

출력은 `postgres-producer`, `clickhouse-consumer`, `reconciler` 포드가 `Running` 상태에 있는 것을 나열합니다.

Siphon은 값 파일을 변경할 때 포드를 다시 시작하지 않습니다. 변경을 적용하려면 배포를 다시 시작합니다:

```shell
kubectl -n siphon rollout restart deployment
```

### 값 파일 설정 {#values-file-settings}

| 설정 | 요구 사항 |
|---------|-------------|
| `database_mapping` | 필수입니다. GitLab 스키마는 인스턴스가 별도로 저장하든 그렇지 않든 3개의 논리적 데이터베이스(`main`, `ci`, `sec`)로 분할됩니다. 매핑이 없으면 생성기가 실패합니다. 대신 `ci` 및 `sec`를 참조하는 테이블 정의를 제거하지 마세요. 모든 CI, 취약성, 종속성 테이블을 자동으로 삭제하기 때문입니다. |
| `stream_name` | GitLab Orbit이 읽는 스트림 이름과 일치해야 합니다. 두 측면이 공유하는 값의 전체 목록은 [공유 구성 값](getting-started.md#shared-configuration-values)을 참조하세요. |
| `advisory_lock_id` 및 잠금 시간 초과 | 필수입니다. 생산자는 이들 없이 시작 시 중지됩니다. |
| `nats_config.replicas` | NATS 클러스터 크기와 일치해야 합니다. 단일 NATS 서버는 1개의 복제본만 지원합니다. |
| `ssl_mode` | PostgreSQL 서버가 제공하는 것과 일치해야 합니다. 예제에서는 `require`을 사용하며, Linux 패키지 PostgreSQL이 기본적으로 TLS를 제공하기 때문에 작동합니다. TLS를 제공하지 않는 서버는 연결을 거부하고 생산자는 `server refused TLS connection`로 시작 시 중지됩니다. |
| `connection.replication.use_alter_publication_function` | `true`로 유지되어야 하며, 이는 차트 기본값입니다. `public.siphon_alter_publication`의 `EXECUTE` 권한은 이 설정에 대해 존재합니다: 게시는 GitLab 데이터베이스 사용자에게 속하므로 Siphon 역할의 직접 `ALTER PUBLICATION`이 실패합니다. |
| `max_age_seconds` | 소비자가 얼마나 멀리 뒤로 재생할 수 있는지 제어합니다. 60개 이상의 테이블에서 15일 동안 변경된 모든 행을 보존하면 큰 JetStream 파일 저장소가 생성됩니다. 전체 보존 기간에 대해 NATS 볼륨을 크기 조정하거나 값을 낮춥니다. |

### 큰 행을 위한 객체 저장소 {#object-storage-for-large-rows}

스트림에 너무 큰 행은 객체 저장소로 이동합니다. 이전 값 파일은 구성하지 않으므로 Siphon은 NATS JetStream 객체 저장소를 사용하며 자격 증명이 필요하지 않습니다.

외부 버킷을 사용하려면 `queueing` 아래에 `object_storage_config`을 추가하되 `identifier`, `type`, `bucket_name`를 사용하고 모든 Siphon 포드에 버킷의 자격 증명을 제공합니다. `type: s3`을 사용하면 Siphon은 표준 AWS SDK 체인을 따르므로 `AWS_REGION`을 설정하고 인스턴스 역할을 연결하거나 `AWS_ACCESS_KEY_ID` 및 `AWS_SECRET_ACCESS_KEY`을 `env` 및 `envFromSecrets`을 통해 전달합니다. S3 호환 서비스의 경우 `AWS_ENDPOINT_URL_S3`도 설정합니다. Google Cloud Storage의 경우 기본 애플리케이션 자격 증명과 함께 `type: gcs`을 사용합니다.

## 복제 확인 {#verify-replication}

첫 번째 복사는 한 번에 하나의 테이블을 처리하고 각 테이블이 병합되는 동안 복제를 일시 중지합니다. 따라서 기간은 데이터 양이 아닌 테이블의 수에 따라 달라집니다. GitLab 19.2.2 인스턴스는 60개 이상의 테이블을 복제합니다. 이 확인을 통해 복제가 실행 중인지 확인합니다. 첫 번째 복사가 완료되었는지는 확인하지 않습니다.

1. Siphon이 PostgreSQL을 읽고 있는지 확인합니다. 슬롯은 활성 상태여야 하고 `confirmed_flush_lsn`은 GitLab에 쓴 후 진행되어야 합니다:

   ```sql
   SELECT slot_name, active, wal_status, confirmed_flush_lsn
   FROM pg_replication_slots
   WHERE slot_name = 'siphon_main_1_slot';
   ```

1. 행이 ClickHouse에 도착하고 있는지 확인합니다:

   ```sql
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_namespaces FINAL;
   SELECT count() FROM gitlab_clickhouse_main_production.siphon_projects FINAL;
   ```

   PostgreSQL의 동일한 테이블과 개수를 비교합니다. `FINAL`은 필수입니다. 테이블이 `ReplacingMergeTree`이고 업데이트된 행은 배경 병합이 완료될 때까지 1회 이상 표시되기 때문입니다. ClickHouse 개수는 일반적으로 약간 더 높습니다. 테이블이 첫 번째 쓰기에서 자리 표시자 행을 받을 수 있기 때문입니다.

> [!warning]
비활성 복제 슬롯은 GitLab PostgreSQL 디스크에 미리 쓰기 로그를 보관하고 이를 채울 수 있습니다. 디스크 여유 공간보다 오래 Siphon을 중지하면 `SELECT pg_drop_replication_slot('siphon_main_1_slot');`으로 슬롯을 삭제하고 나중에 새로운 스냅샷을 생성합니다.

## 다음 단계 {#next-step}

- [GitLab Orbit 설정](orbit-setup.md)
