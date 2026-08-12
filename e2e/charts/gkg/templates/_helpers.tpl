{{/*
Expand the name of the chart.
*/}}
{{- define "gkg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated to 63 chars for DNS naming spec compliance.
*/}}
{{- define "gkg.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if or (eq $name .Release.Name) (hasSuffix $name .Release.Name) }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "gkg.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "gkg.labels" -}}
helm.sh/chart: {{ include "gkg.chart" . }}
{{ include "gkg.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "gkg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gkg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Component labels - include chart-level metadata plus component identity.
When serviceTag is passed, the version label reflects the resolved per-service tag.
Usage: {{ include "gkg.componentLabels" (dict "root" . "component" "webserver" "serviceTag" .Values.webserver.image.tag) }}
*/}}
{{- define "gkg.componentLabels" -}}
helm.sh/chart: {{ include "gkg.chart" .root }}
{{ include "gkg.componentSelectorLabels" . }}
app.kubernetes.io/version: {{ include "gkg.serviceImageTag" (dict "root" .root "serviceTag" .serviceTag) | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end }}

{{/*
Component selector labels.
Usage: {{ include "gkg.componentSelectorLabels" (dict "root" . "component" "webserver") }}
*/}}
{{- define "gkg.componentSelectorLabels" -}}
app.kubernetes.io/name: {{ include "gkg.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "gkg.serviceAccountName" -}}
{{- if .Values.serviceAccount.name }}
{{- .Values.serviceAccount.name }}
{{- else }}
{{- include "gkg.fullname" . }}
{{- end }}
{{- end }}

{{/*
Resolve the effective image tag for a given service.
Prefers the per-service tag (e.g. webserver.image.tag) when non-empty;
falls back to the global image.tag otherwise.

Usage: {{ include "gkg.serviceImageTag" (dict "root" . "serviceTag" .Values.webserver.image.tag) }}
*/}}
{{- define "gkg.serviceImageTag" -}}
{{- .serviceTag | default .root.Values.image.tag -}}
{{- end }}

{{/*
Resolve the effective image repository for a given service.
Prefers the per-service repository when non-empty;
falls back to the global image.repository otherwise.

Usage: {{ include "gkg.serviceImageRepository" (dict "root" . "serviceRepository" .Values.webserver.image.repository) }}
*/}}
{{- define "gkg.serviceImageRepository" -}}
{{- .serviceRepository | default .root.Values.image.repository -}}
{{- end }}

{{/*
Schedule config block for config files.
Renders explicit task keys with serde(flatten)-compatible structure.
Usage: {{ include "gkg.scheduleConfig" . }}
*/}}
{{- define "gkg.scheduleConfig" -}}
{{- toYaml .Values.schedule.tasks -}}
{{- end }}

{{/*
NATS connection URL. Parent charts depending on this chart can override
this template in their own _helpers.tpl to derive the URL from their own
release (e.g. a bundled NATS subchart).
Usage: {{ include "gkg.natsUrl" . }}
*/}}
{{- define "gkg.natsUrl" -}}
{{- .Values.nats.url -}}
{{- end }}

{{/*
GitLab base URL. Parent charts depending on this chart can override this
template in their own _helpers.tpl to derive the URL from their own release
(e.g. a bundled GitLab subchart).
Usage: {{ include "gkg.gitlabBaseUrl" . }}
*/}}
{{- define "gkg.gitlabBaseUrl" -}}
{{- .Values.gitlab.baseUrl -}}
{{- end }}

{{/*
ClickHouse datalake URL. Parent charts depending on this chart can override
this template in their own _helpers.tpl to derive the URL from their own
release (e.g. a bundled ClickHouse subchart).
Usage: {{ include "gkg.clickhouseDatalakeUrl" . }}
*/}}
{{- define "gkg.clickhouseDatalakeUrl" -}}
{{- $c := .Values.clickhouse.datalake -}}
{{- printf "%s://%s:%v" (ternary "https" "http" $c.ssl) $c.host $c.httpPort -}}
{{- end }}

{{/*
ClickHouse graph URL. Parent charts depending on this chart can override
this template in their own _helpers.tpl to derive the URL from their own
release (e.g. a bundled ClickHouse subchart).
Usage: {{ include "gkg.clickhouseGraphUrl" . }}
*/}}
{{- define "gkg.clickhouseGraphUrl" -}}
{{- $c := .Values.clickhouse.graph -}}
{{- printf "%s://%s:%v" (ternary "https" "http" $c.ssl) $c.host $c.httpPort -}}
{{- end }}

{{/*
ClickHouse config block for config files (HTTP connection, no password).
The `url` is provided by the caller so the URL template can be overridden
independently of the rest of the config.
Usage: {{ include "gkg.clickhouseConfig" (dict "url" (include "gkg.clickhouseDatalakeUrl" .) "config" .Values.clickhouse.datalake) }}
*/}}
{{- define "gkg.clickhouseConfig" -}}
url: {{ .url | quote }}
database: {{ .config.database | quote }}
username: {{ .config.user | quote }}
{{- if .config.sessionSettings }}
session_settings:
  {{- toYaml .config.sessionSettings | nindent 2 }}
{{- end }}
{{- if .config.insertSettings }}
insert_settings:
  {{- toYaml .config.insertSettings | nindent 2 }}
{{- end }}
{{- end }}

{{/*
Analytics (Snowplow) config block for config files.
`collector_url` is omitted when analytics are disabled.
Usage: {{ include "gkg.analyticsConfig" . }}
*/}}
{{- define "gkg.analyticsConfig" -}}
enabled: {{ .Values.analytics.enabled }}
{{- if .Values.analytics.enabled }}
collector_url: {{ required "analytics.collector_url is required when analytics.enabled is true" .Values.analytics.collector_url | quote }}
{{- end }}
deployment:
  type: {{ .Values.analytics.deployment.type | quote }}
  environment: {{ .Values.analytics.deployment.environment | quote }}
{{- end }}

{{/*
Billing (Snowplow events) config block for config files.
`collector_url` is omitted when billing is disabled.
Usage: {{ include "gkg.billingConfig" . }}
*/}}
{{- define "gkg.billingConfig" -}}
enabled: {{ .Values.billing.enabled }}
{{- if .Values.billing.enabled }}
collector_url: {{ required "billing.collector_url is required when billing.enabled is true" .Values.billing.collector_url | quote }}
{{- end }}
{{- with .Values.billing.quota }}
quota:
  enabled: {{ .enabled }}
  {{- if .enabled }}
  customers_dot_url: {{ required "billing.quota.customers_dot_url is required when billing.quota.enabled is true" .customers_dot_url | quote }}
  {{- end }}
  api_user: {{ .api_user | quote }}
{{- end }}
{{- end }}

{{/*
Feature flags config block. Passes every key under `.Values.features` straight
through to the Orbit `features:` config section, so new flags need no chart change.
Empty by default, so the default render omits the block and stays valid against
image tags whose config schema predates a flag; callers guard the `features:`
parent on a non-empty result.
Usage: {{ include "gkg.featuresConfig" . }}
*/}}
{{- define "gkg.featuresConfig" -}}
{{- with .Values.features }}
{{- toYaml . }}
{{- end }}
{{- end }}

{{/*
Container security context - hardened defaults.
*/}}
{{- define "gkg.securityContext" -}}
runAsNonRoot: true
runAsUser: 65532
readOnlyRootFilesystem: true
allowPrivilegeEscalation: false
capabilities:
  drop:
    - ALL
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/*
Pod security context.
*/}}
{{- define "gkg.podSecurityContext" -}}
fsGroup: 65532
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/*
Mount path (under /etc/secrets) for a logical secret key.
Usage: {{ include "gkg.secretKeyPath" "gitlabJwtVerifyingKey" }}
*/}}
{{- define "gkg.secretKeyPath" -}}
{{- $paths := dict
    "gitlabJwtVerifyingKey" "gitlab/jwt/verifying_key"
    "gitlabJwtSigningKey"   "gitlab/jwt/signing_key"
    "datalakePassword"      "datalake/password"
    "graphPassword"         "graph/password"
    "graphReadPassword"     "graph/password"
    "quotaApiToken"         "billing/quota/api_token"
-}}
{{- index $paths . -}}
{{- end }}

{{/*
Volume name for a per-key Secret mount.
Usage: {{ include "gkg.perKeyVolumeName" "gitlabJwtVerifyingKey" }}
*/}}
{{- define "gkg.perKeyVolumeName" -}}
{{- printf "secret-%s" (kebabcase .) -}}
{{- end }}

{{/*
Secret volume definitions. Accepts a list of value keys referencing secrets.keys.<name>.
One volume per requested key, each sourced from `secrets.perKey.<name>.secretName`
(required); `key` selects the field and defaults to `secrets.keys.<name>`.
Usage: {{ include "gkg.secretVolume" (dict "root" . "keys" (list "gitlabJwtVerifyingKey" "datalakePassword" "graphPassword")) }}
*/}}
{{- define "gkg.secretVolume" -}}
{{- $root := .root -}}
{{- range $key := .keys }}
{{- $entry := index $root.Values.secrets.perKey $key }}
- name: {{ include "gkg.perKeyVolumeName" $key }}
  secret:
    secretName: {{ required (printf "secrets.perKey.%s.secretName is required" $key) $entry.secretName }}
    items:
      - key: {{ $entry.key | default (index $root.Values.secrets.keys $key) }}
        path: {{ base (include "gkg.secretKeyPath" $key) }}
{{- end }}
{{- end }}

{{/*
Secret volume mounts matching `gkg.secretVolume`.
One subPath mount per key at its canonical /etc/secrets/<path>.
Usage: {{ include "gkg.secretVolumeMount" (dict "root" . "keys" (list "datalakePassword" "graphPassword")) }}
*/}}
{{- define "gkg.secretVolumeMount" -}}
{{- range $key := .keys }}
- name: {{ include "gkg.perKeyVolumeName" $key }}
  mountPath: /etc/secrets/{{ include "gkg.secretKeyPath" $key }}
  subPath: {{ base (include "gkg.secretKeyPath" $key) }}
  readOnly: true
{{- end }}
{{- end }}

{{/*
NATS TLS volume definition. Only rendered when nats.tls.enabled is true.
Mounts CA, client cert, and client key from a Kubernetes Secret.
Usage: {{ include "gkg.natsTlsVolume" . }}
*/}}
{{- define "gkg.natsTlsVolume" -}}
{{- if .Values.nats.tls.enabled -}}
- name: nats-tls
  secret:
    secretName: {{ required "nats.tls.existingSecret is required when nats.tls.enabled is true" .Values.nats.tls.existingSecret }}
    items:
      - key: {{ .Values.nats.tls.keys.caCert }}
        path: ca.pem
      - key: {{ .Values.nats.tls.keys.clientCert }}
        path: client.pem
      - key: {{ .Values.nats.tls.keys.clientKey }}
        path: client-key.pem
{{- end }}
{{- end }}

{{/*
NATS TLS volume mount. Only rendered when nats.tls.enabled is true.
Usage: {{ include "gkg.natsTlsVolumeMount" . }}
*/}}
{{- define "gkg.natsTlsVolumeMount" -}}
{{- if .Values.nats.tls.enabled -}}
- name: nats-tls
  mountPath: /etc/nats-tls
  readOnly: true
{{- end }}
{{- end }}

{{/*
Indexer pool resource name: <fullname>-indexer-<pool>.
Usage: {{ include "gkg.indexerPoolName" (dict "root" $ "pool" "default") }}
*/}}
{{- define "gkg.indexerPoolName" -}}
{{- printf "%s-indexer-%s" (include "gkg.fullname" .root) .pool | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Classify an indexer pool by its modules list. Used for labels and warnings.
- universal: empty/omitted, OR contains all of {code, sdlc, namespace_deletion}
- code:      contains "code" and not "sdlc"
- sdlc:      contains "sdlc" and not "code"
- custom:    anything else
Usage: {{ include "gkg.indexerPoolKind" (list "code") }}
*/}}
{{- define "gkg.indexerPoolKind" -}}
{{- $m := . | default (list) -}}
{{- if not $m -}}universal
{{- else -}}
{{- $hasCode := has "code" $m -}}
{{- $hasSdlc := has "sdlc" $m -}}
{{- $hasNs := has "namespace_deletion" $m -}}
{{- if and $hasCode $hasSdlc $hasNs -}}universal
{{- else if and $hasCode (not $hasSdlc) -}}code
{{- else if and $hasSdlc (not $hasCode) -}}sdlc
{{- else -}}custom
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Whether a pool is enabled. Pool is on iff indexer.enabled AND pool.enabled is not explicitly false.
Sprig's `default` treats `false` as "empty" and returns the default, so we test explicit presence.
Returns "true" or "" (Helm-falsy) — use with `if`.
Usage: {{ if eq (include "gkg.indexerPoolEnabled" (dict "root" $ "pool" $pool)) "true" }}
*/}}
{{- define "gkg.indexerPoolEnabled" -}}
{{- if .root.Values.indexer.enabled -}}
{{- if hasKey .pool "enabled" -}}
{{- if .pool.enabled -}}true{{- end -}}
{{- else -}}true{{- end -}}
{{- end -}}
{{- end }}

{{/*
Pool config with indexer top-level defaults merged in. Pool fields override.
Returns YAML; consume with `fromYaml`.
Usage: {{ $merged := include "gkg.indexerPoolMerged" (dict "root" $ "pool" $pool) | fromYaml }}
*/}}
{{- define "gkg.indexerPoolMerged" -}}
{{- $i := .root.Values.indexer -}}
{{- $defaults := dict
    "replicas" $i.replicas
    "logLevel" $i.logLevel
    "image" (deepCopy $i.image)
    "probes" (deepCopy $i.probes)
    "tmpSizeLimit" $i.tmpSizeLimit
    "resources" (deepCopy $i.resources)
    "nodeSelector" (deepCopy $i.nodeSelector)
    "tolerations" (deepCopy $i.tolerations)
    "affinity" (deepCopy $i.affinity)
    "modules" (list)
    "engine" (dict)
-}}
{{- $merged := mustMergeOverwrite $defaults (deepCopy .pool) -}}
{{- toYaml $merged -}}
{{- end }}

{{/*
Effective NATS config for a pool: top-level .nats deep-merged with pool.nats.
Returns YAML; consume with `fromYaml`. Mirrors gkg.indexerPoolEngine.
Usage: {{ $nats := include "gkg.indexerPoolNats" (dict "root" $ "pool" $pool) | fromYaml }}
*/}}
{{- define "gkg.indexerPoolNats" -}}
{{- $base := deepCopy (.root.Values.nats | default dict) -}}
{{- $override := deepCopy (.pool.nats | default dict) -}}
{{- toYaml (mustMergeOverwrite $base $override) -}}
{{- end }}

{{/*
Effective ClickHouse graph config for a pool: top-level .clickhouse.graph deep-merged
with pool.clickhouse.graph. Lets pools tune insert/query settings while inheriting the
shared connection details (host, database, user). Mirrors gkg.indexerPoolNats.
Returns YAML; consume with `fromYaml`.
Usage: {{ $graph := include "gkg.indexerPoolClickhouseGraph" (dict "root" $ "pool" $pool) | fromYaml }}
*/}}
{{- define "gkg.indexerPoolClickhouseGraph" -}}
{{- $base := deepCopy (.root.Values.clickhouse.graph | default dict) -}}
{{- $override := deepCopy ((.pool.clickhouse | default dict).graph | default dict) -}}
{{- toYaml (mustMergeOverwrite $base $override) -}}
{{- end }}

{{/*
Effective engine config for a pool: top-level .engine deep-merged with pool.engine,
then `modules` set from pool.modules (if non-empty).
Returns YAML for direct embedding inside a configmap.
Usage: {{ include "gkg.indexerPoolEngine" (dict "root" $ "pool" $pool) }}
*/}}
{{- define "gkg.indexerPoolEngine" -}}
{{- $base := deepCopy (.root.Values.engine | default dict) -}}
{{- $override := deepCopy (.pool.engine | default dict) -}}
{{- $merged := mustMergeOverwrite $base $override -}}
{{- $modules := .pool.modules | default (list) -}}
{{- if $modules -}}
{{- $_ := set $merged "modules" $modules -}}
{{- end -}}
{{- toYaml $merged -}}
{{- end }}

{{/*
Pool labels: component=indexer, plus pool name and pool kind for log/metric slicing.
Usage: {{ include "gkg.indexerPoolLabels" (dict "root" $ "pool" $name "kind" $kind "serviceTag" $tag) }}
*/}}
{{- define "gkg.indexerPoolLabels" -}}
{{- include "gkg.componentLabels" (dict "root" .root "component" "indexer" "serviceTag" .serviceTag) }}
gkg.gitlab.com/indexer-pool: {{ .pool | quote }}
gkg.gitlab.com/indexer-modules: {{ .kind | quote }}
{{- end }}

{{/*
Pool selector labels: must include the pool name so each Deployment selects only its own pods.
Usage: {{ include "gkg.indexerPoolSelectorLabels" (dict "root" $ "pool" $name) }}
*/}}
{{- define "gkg.indexerPoolSelectorLabels" -}}
{{- include "gkg.componentSelectorLabels" (dict "root" .root "component" "indexer") }}
gkg.gitlab.com/indexer-pool: {{ .pool | quote }}
{{- end }}

{{/*
Indexer module coverage across all enabled pools. Used by NOTES.txt for warnings.
Returns JSON with three keys:
  covered: dict of bool per module (code, sdlc, namespace_deletion)
  kinds:   dict of bool per pool kind (universal, code, sdlc, custom)
  count:   number of enabled pools
Usage: {{ $cov := include "gkg.indexerCoverage" . | fromJson }}
*/}}
{{- define "gkg.indexerCoverage" -}}
{{- $covered := dict "code" false "sdlc" false "namespace_deletion" false -}}
{{- $kinds := dict "universal" false "code" false "sdlc" false "custom" false -}}
{{- $state := dict "count" 0 -}}
{{- $allModules := list "code" "sdlc" "namespace_deletion" -}}
{{- if .Values.indexer.enabled -}}
{{- range $name, $pool := .Values.indexer.pools -}}
  {{- if eq (include "gkg.indexerPoolEnabled" (dict "root" $ "pool" $pool)) "true" -}}
    {{- $_ := set $state "count" (add (get $state "count") 1) -}}
    {{- $modules := $pool.modules | default (list) -}}
    {{- $effective := ternary $allModules $modules (empty $modules) -}}
    {{- range $effective -}}
      {{- if hasKey $covered . -}}{{- $_ := set $covered . true -}}{{- end -}}
    {{- end -}}
    {{- $kind := include "gkg.indexerPoolKind" $modules -}}
    {{- $_ := set $kinds $kind true -}}
  {{- end -}}
{{- end -}}
{{- end -}}
{{- dict "covered" $covered "kinds" $kinds "count" (get $state "count") | toJson -}}
{{- end }}

{{/*
PDB spec body. Emits exactly one of minAvailable/maxUnavailable.
minAvailable wins when both are set on the same dict.
Usage: {{ include "gkg.pdbSpec" $pdb | nindent 2 }}
*/}}
{{- define "gkg.pdbSpec" -}}
{{- if .minAvailable -}}
minAvailable: {{ .minAvailable }}
{{- else if .maxUnavailable -}}
maxUnavailable: {{ .maxUnavailable }}
{{- end -}}
{{- end }}

{{/*
Resolve effective PDB config for an indexer pool by overlaying pool.pdb on
indexer.pdb. When the pool flips the threshold field (e.g. defaults set
maxUnavailable but the pool sets minAvailable), the opposing field is
dropped so the rendered PDB has exactly one threshold.
Returns YAML; consume with `fromYaml`.
Usage: {{ $pdb := include "gkg.resolvePoolPdb" (dict "root" $ "pool" $pool) | fromYaml }}
*/}}
{{- define "gkg.resolvePoolPdb" -}}
{{- $base := deepCopy (.root.Values.indexer.pdb | default dict) -}}
{{- $override := deepCopy (.pool.pdb | default dict) -}}
{{- $merged := mustMergeOverwrite $base $override -}}
{{- if and $override.minAvailable (hasKey $merged "maxUnavailable") -}}
{{- $_ := unset $merged "maxUnavailable" -}}
{{- end -}}
{{- if and $override.maxUnavailable (hasKey $merged "minAvailable") -}}
{{- $_ := unset $merged "minAvailable" -}}
{{- end -}}
{{- toYaml $merged -}}
{{- end }}

{{/*
Default health-check targets when healthCheck.targets is not user-overridden.
Emits one entry per enabled indexer pool, plus the dispatcher when enabled.
Used by health-check templates so adding a pool auto-enrolls it for liveness probing.
Usage: {{ include "gkg.healthCheckDefaultDeployments" . }}
*/}}
{{- define "gkg.healthCheckDefaultDeployments" -}}
{{- $names := list -}}
{{- if .Values.indexer.enabled -}}
{{- range $name, $pool := .Values.indexer.pools -}}
{{- if eq (include "gkg.indexerPoolEnabled" (dict "root" $ "pool" $pool)) "true" -}}
{{- $names = append $names (include "gkg.indexerPoolName" (dict "root" $ "pool" $name)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if .Values.dispatcher.enabled -}}
{{- $names = append $names (printf "%s-dispatcher" (include "gkg.fullname" .)) -}}
{{- end -}}
{{- toYaml $names -}}
{{- end }}
