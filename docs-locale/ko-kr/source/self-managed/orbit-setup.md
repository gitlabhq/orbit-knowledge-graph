---
stage: Analytics
group: Knowledge Graph
info: To determine the technical writer assigned to the Stage/Group associated with this page, see https://handbook.gitlab.com/handbook/product/ux/technical-writing/#assignments
description: Kubernetes에 GitLab Orbit을 설치하고 GitLab에 연결한 후 첫 번째 그룹을 인덱싱합니다.
title: GitLab Orbit 설정
---

{{< details >}}

- 티어: Premium, Ultimate
- 제공 서비스: GitLab Self-Managed
- 상태:  베타

{{< /details >}}

{{< history >}}

- [GitLab 19.2.2에서 도입](https://gitlab.com/groups/gitlab-org/-/epics/22739)되었습니다.

{{< /history >}}

> [!note]
GitLab Self-Managed의 GitLab Orbit은 [베타](https://docs.gitlab.com/policy/development_stages_support/#beta) 단계입니다. 이 기능은 테스트 가능하지만 프로덕션 사용은 준비되지 않았습니다.

GitLab Orbit은 인스턴스 옆에 Helm 릴리스로 실행됩니다. 인덱서는 ClickHouse 데이터 레이크를 읽고 GitLab 내부 API를 통해 소스 코드를 가져옵니다. 웹 서버는 그래프에서의 쿼리에 응답합니다.

GitLab Orbit은 HTTP를 통해 GitLab을 호출하고, GitLab은 포트 50054의 gRPC를 통해 GitLab Orbit을 호출합니다. 양방향 모두 열려 있어야 합니다. GitLab Orbit은 Gitaly에 직접 연결되지 않습니다.

전제 조건:

- [데이터 복제](data-replication.md)가 실행 중이고 데이터 레이크에 행이 도착하고 있습니다.
- 인덱싱하려는 그룹에 대한 소유자 역할.
- GitLab에 대한 관리자 액세스.

이 순서대로 GitLab Orbit을 설정합니다:

1. ClickHouse 데이터베이스 및 ID를 만듭니다.
1. GitLab에서 GitLab Orbit을 켭니다.
1. GitLab Orbit에서 자격 증명을 사용할 수 있도록 합니다.
1. GitLab Orbit을 설치합니다.
1. 그룹에 대해 인덱싱을 켭니다.

## ClickHouse 데이터베이스 및 ID 만들기 {#create-the-clickhouse-database-and-identities}

GitLab Orbit은 자신의 그래프 데이터베이스와 세 명의 사용자가 필요합니다:

| 사용자 | 역할 | 액세스 |
|------|------|--------|
| `gkg_writer` | `gkg_app` | 그래프 데이터베이스를 읽고 쓰기 |
| `gkg_reader` | `gkg_reader_app` | 그래프 데이터베이스 읽기 |
| `gkg_siphon_reader` | `gkg_siphon_reader_app` | 데이터 레이크 읽기 |

데이터베이스와 사용자를 만드는 명령문은 GitLab Orbit 이미지에 포함되어 있으므로 명령문은 실행하는 버전과 항상 일치합니다. 명령문은 멱등원입니다. GitLab Orbit은 시작 시 그래프 데이터베이스를 만들지 않으며, 디스패처는 이미 존재하는 데이터베이스에 대해서만 스키마 마이그레이션을 실행합니다.

데이터베이스 및 ID를 만들려면:

1. 명령문을 읽습니다:

   ```shell
   docker run --rm --entrypoint cat \
     registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg:0.96.0 \
     /usr/share/gkg/clickhouse-setup.sql
   ```

1. 그래프 데이터베이스 이름, 데이터 레이크 데이터베이스 이름 및 각 사용자의 암호를 대체합니다. `orbit`을 그래프 데이터베이스 이름으로 사용합니다.

1. 결과를 관리자로서 ClickHouse에 대해 실행합니다. 명령문이 그래프 데이터베이스를 만들고 `gkg_app` 역할에 해당 데이터베이스에 필요한 권한을 부여합니다.

1. 배송 파일에 포함되지 않은 세 가지 권한을 추가합니다:

   ```sql
   GRANT SELECT ON system.parts TO gkg_app;
   GRANT SELECT ON system.tables TO gkg_app;
   GRANT SELECT ON system.dictionaries TO gkg_reader_app;
   ```

직접 실행하는 ClickHouse 인스턴스에서 모든 사용자는 `system` 데이터베이스를 읽을 수 있으므로 이러한 권한은 아무것도 변경하지 않습니다. 관리형 ClickHouse는 일반적으로 `system` 데이터베이스를 제한합니다. 권한이 없으면 새 버전이 배포될 때 스키마 마이그레이션이 중지되고 쿼리 경로 해결이 실패합니다. 두 경우 모두 권한을 추가합니다. 관리형 서비스로 이동해도 구성이 변경되지 않습니다.

GitLab Orbit은 포트 8123의 HTTP 인터페이스 또는 TLS를 사용한 포트 8443을 통해 ClickHouse에 도달합니다. Siphon은 포트 9000의 네이티브 프로토콜을 사용하므로 HTTP 포트와 포트 9000 모두 클러스터에서 도달할 수 있어야 합니다.

## GitLab에서 GitLab Orbit 켜기 {#turn-on-gitlab-orbit-in-gitlab}

GitLab과 GitLab Orbit은 하나의 대칭 키로 서로를 인증하며, GitLab이 이를 소유합니다. GitLab에서 먼저 GitLab Orbit을 켜서 나중 단계에서 클러스터로 복사하기 전에 키가 존재하도록 합니다.

시작하기 전에 gRPC 엔드포인트를 결정합니다. GitLab은 포트 50054의 TLS를 통해 엔드포인트에 연결합니다. 설정의 스키마가 연결이 암호화되는지 여부를 제어하므로 값은 `tls://`로 시작해야 합니다.

{{< tabs >}}

{{< tab title="Linux 패키지(Omnibus)" >}}

1. `/etc/gitlab/gitlab.rb`를 편집합니다.

   ```ruby
   gitlab_rails['orbit_enabled'] = true
   gitlab_rails['orbit_grpc_endpoint'] = 'tls://orbit.example.com:50054'
   ```

1. 파일을 저장하고 GitLab을 다시 구성합니다.

   ```shell
   sudo gitlab-ctl reconfigure
   ```

   GitLab은 공유 키를 생성하고 `/var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret`에 씁니다.

1. 키를 읽습니다. 다시 인코딩하거나 자르지 말고 정확히 값을 복사합니다:

   ```shell
   sudo cat /var/opt/gitlab/gitlab-rails/etc/gitlab_knowledge_graph_secret
   ```

다중 노드 설정에서 모든 Rails 노드는 동일한 키를 유지해야 합니다. 한 노드에서 키를 생성한 다음 `gitlab_rails['orbit_secret']`을 다른 노드의 해당 값으로 설정하거나 `/etc/gitlab/gitlab-secrets.json`을 첫 번째 재구성 전에 동기화합니다.

{{< /tab >}}

{{< tab title="Helm 차트(Kubernetes)" >}}

GitLab 차트는 키를 생성하지 않으므로 직접 생성합니다. GitLab을 업그레이드하기 전에 Secret이 존재해야 합니다. 차트가 `optional` 플래그 없이 Secret을 탑재하므로 누락된 Secret은 모든 새 포드를 `ContainerCreating`에 남겨둡니다.

1. 32개의 무작위 바이트로 구성된 키를 base64로 인코딩합니다:

   ```shell
   openssl rand -base64 32
   ```

1. GitLab 릴리스의 네임스페이스에 있는 Secret에 해당 값을 넣습니다.

1. Secret을 참조합니다:

   ```yaml
   global:
     appConfig:
       knowledgeGraph:
         enabled: true
         grpcEndpoint: 'tls://orbit.example.com:50054'
         jwtSecret:
           secret: gitlab-orbit-jwt
           key: secret
   ```

{{< /tab >}}

{{< /tabs >}}

GitLab Orbit 차트를 설치할 때까지 GitLab은 GitLab Orbit에 도달할 수 없습니다. 그 전의 연결 오류는 예상됩니다.

## GitLab Orbit에 자격 증명 제공 {#make-the-credentials-available-to-gitlab-orbit}

GitLab Orbit은 Kubernetes Secret의 자신의 키에서 각 자격 증명을 읽습니다. Secret을 만들기 전에 네임스페이스가 존재해야 합니다. 다음 섹션의 값 파일은 GitLab Orbit 네임스페이스에 `gkg-secrets`이라고 하는 하나의 Secret을 예상하며 다음 키가 있습니다:

| 키 | 보유 항목 |
|-----|-------|
| `gitlab-jwt-verifying-key` | GitLab에서 생성한 공유 키는 이전 단계에서 GitLab의 토큰을 확인하는 데 사용됨 |
| `gitlab-jwt-signing-key` | GitLab에 다시 전송된 토큰에 서명하는 데 사용되는 동일한 공유 키 |
| `datalake-password` | ClickHouse `gkg_siphon_reader` 사용자의 암호 |
| `graph-password` | ClickHouse `gkg_writer` 사용자의 암호 |
| `graph-read-password` | ClickHouse `gkg_reader` 사용자의 암호 |

두 키 항목이 동일한 값을 보유합니다. 각 키를 다른 Secret에서 가져오려면 `secrets.perKey`을 사용합니다.

값을 이미 실행하고 있는 비밀 관리자에 보관해야 합니다. External Secrets Operator와 같은 도구를 사용하여 클러스터로 동기화합니다. 평문을 다른 곳에 저장하지 마세요.

gRPC 엔드포인트에 대한 TLS 인증서도 제공해야 합니다. 자세한 내용은 [TLS 및 네트워크 요구 사항](#tls-and-network-requirements)을 참조하세요.

## GitLab Orbit 설치 {#install-gitlab-orbit}

1. 다음을 `orbit-values.yaml`로 저장하고 자리 표시자를 바꿉니다:

   ```yaml
   image:
     tag: "0.96.0"

   secrets:
     perKey:
       gitlabJwtVerifyingKey: {secretName: gkg-secrets}
       gitlabJwtSigningKey: {secretName: gkg-secrets}
       datalakePassword: {secretName: gkg-secrets}
       graphPassword: {secretName: gkg-secrets}
       graphReadPassword: {secretName: gkg-secrets}

   # Off by default. Creates the graph schema on first run and schedules indexing.
   dispatcher:
     enabled: true

   # HTTP interface. For a TLS endpoint such as ClickHouse Cloud,
   # add httpPort: 8443 and ssl: true to both blocks.
   clickhouse:
     datalake:
       host: <clickhouse_host>
       database: gitlab_clickhouse_main_production
       user: gkg_siphon_reader
     graph:
       host: <clickhouse_host>
       database: orbit
       user: gkg_writer
       readUser: gkg_reader

   # The same NATS cluster Siphon publishes to.
   nats:
     url: "nats://nats.nats.svc.cluster.local:4222"

   # The GitLab URL the cluster can reach. If GitLab is exposed under a host name
   # that does not match its certificate, keep baseUrl on the certificate name and
   # add resolveHost with the host to route to.
   gitlab:
     baseUrl: "https://gitlab.example.com"

   webserver:
     service:
       type: ClusterIP

   # Only when the webserver terminates TLS itself. Omit both keys if a load
   # balancer terminates in front of it.
   tls:
     enabled: true
     existingSecret: gkg-webserver-tls
   ```

1. 차트를 설치합니다:

   ```shell
   helm upgrade --install gkg \
     oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg \
     --version 1.5.0 \
     --namespace gitlab-orbit \
     --create-namespace \
     --values orbit-values.yaml
   ```

   이 명령은 직접 Helm 설치에 대한 참조입니다. 네임스페이스 이름과 배포 방법을 자신의 도구와 일치하도록 조정합니다.

1. 세 가지 구성 요소가 모두 실행 중인지 확인합니다:

   ```shell
   kubectl -n gitlab-orbit get pods
   ```

출력은 웹 서버, 인덱서 및 디스패처 포드를 `Running` 상태로 나열합니다. 차트는 메트릭, 자동 크기 조정, 분석 및 청구를 포함하여 기본적으로 다른 모든 것을 끕니다. GitLab Self-Managed에서는 이들을 꺼진 상태로 둡니다.

### TLS 및 네트워크 요구 사항 {#tls-and-network-requirements}

GitLab은 포트 50054의 TLS를 통해 gRPC 엔드포인트에 연결합니다. Rails와 Workhorse 모두 자신의 연결을 열므로 둘 다 엔드포인트에 대한 경로가 필요합니다.

TLS는 두 위치 중 하나에서 종료될 수 있습니다: `gkg-webserver` 서비스 앞의 로드 밸런서 또는 `tls.enabled` 및 `tls.existingSecret`을 사용하는 웹 서버 자체입니다. 웹 서버 경우에만 인증서가 클러스터 내부에 필요합니다.

공개적으로 신뢰할 수 있는 인증 기관(CA)에서 발급한 인증서는 GitLab에서 추가 구성이 필요하지 않습니다. 자신의 CA에서 인증서를 받으려면 CA 인증서를 GitLab 트러스트 저장소에 추가합니다. 자세한 내용은 [사용자 지정 공개 인증서 설치](https://docs.gitlab.com/omnibus/settings/ssl/#install-custom-public-certificates)를 참조하세요.

GitLab이 동일한 클러스터에서 실행되면 `ClusterIP`으로 충분하며 엔드포인트는 `tls://gkg-webserver.gitlab-orbit.svc.cluster.local:50054`입니다. 인증서는 GitLab이 연결하는 호스트 이름에 유효해야 합니다. 그렇지 않으면 클러스터가 지원하는 방법으로 서비스를 노출하고 주소를 개인 네트워크에 보관합니다.

### 리소스 요구 사항 {#resource-requirements}

차트 기본값은 대부분의 설치에 적합합니다. 3개의 웹 서버 복제본 각각은 500m CPU와 4GiB를 요청합니다. 3개의 인덱서 복제본 각각은 2 CPU, 4GiB 및 5GiB의 임시 스토리지를 요청합니다. 이러한 요청은 Siphon 및 NATS 이전에 대략 8 CPU 및 24GiB의 합계입니다.

인덱서에 최소 8 CPU와 16GiB의 메모리에 도달할 수 있는 공간을 제공합니다. 많은 동시 인덱싱 작업이 모두를 사용할 수 있습니다. 차트 제한을 통해 기본적으로 해당 헤드룸을 허용합니다.

인덱서는 또한 각 노드에서 코드 아카이브를 다운로드할 충분한 임시 스토리지가 필요합니다. 차트는 `indexer.tmpSizeLimit`으로 해당 스크래치 공간의 크기를 조정하며, 기본값은 10GiB이고 인덱서 임시 스토리지 요청 및 제한이 있습니다.

## 그룹에 대해 인덱싱 켜기 {#turn-on-indexing-for-a-group}

GitLab Orbit은 최상위 그룹을 인덱싱합니다. 하위 그룹 및 프로젝트는 자동으로 인덱싱을 상속합니다.

1. 오른쪽 위 모서리에서 **운영자**를 선택합니다.
1. `/admin/orbit`에서 GitLab Orbit 구성 페이지로 이동합니다.
1. **사용가능한 그룹** 아래에서 최상위 그룹을 찾습니다.
1. **인덱싱 켜기**를 선택한 후 확인합니다.

그룹이 **그룹 인덱싱됨**으로 이동합니다.

대신 API로 인덱싱을 켜려면 관리자 액세스 권한이 있는 토큰을 사용합니다:

```shell
curl --request PUT \
  --header "PRIVATE-TOKEN: <your_access_token>" \
  --url "https://gitlab.example.com/api/v4/admin/knowledge_graph/namespaces/<group_id>"
```

인덱싱을 켜도 기존 데이터가 즉시 인덱싱되지는 않습니다. 새로운 변경 사항이 몇 분 내에 복제를 통해 흐릅니다. 스윕은 기존 데이터를 1시간 이내에 선택합니다.

## 설치 확인 {#verify-the-installation}

설치를 확인하려면 인덱싱을 켠 그룹의 프로젝트에 대해 그래프를 쿼리합니다.

요청 본문을 `request.json`에 넣습니다:

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [{
      "id": "p",
      "entity": "Project",
      "columns": ["name", "full_path"],
      "filters": {
        "full_path": {"starts_with": "your-group/"}
      }
    }],
    "limit": 10
  },
  "response_format": "raw"
}
```

```shell
curl --request POST \
  --header "Authorization: Bearer <your_access_token>" \
  --header "Content-Type: application/json" \
  --data @request.json \
  --url "https://gitlab.example.com/api/v4/orbit/query"
```

쿼리는 그룹이 인덱싱된 후 행을 반환합니다. 포드 상태 및 상태 엔드포인트는 첫 번째 인덱스가 완료되기 훨씬 전에 둘 다 통과하기 때문에 인덱싱을 확인하지 않습니다.

## 관련 항목 {#related-topics}

- [GitLab Orbit이 인덱싱하는 항목](../remote/indexing.md)
- [스키마 참고자료](../remote/schema.md)
- [요리책](../remote/cookbook.md)
- [쿼리 언어](../remote/queries/_index.md)
