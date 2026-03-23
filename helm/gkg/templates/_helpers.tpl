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
Usage: {{ include "gkg.componentLabels" (dict "root" . "component" "webserver") }}
*/}}
{{- define "gkg.componentLabels" -}}
helm.sh/chart: {{ include "gkg.chart" .root }}
{{ include "gkg.componentSelectorLabels" . }}
app.kubernetes.io/version: {{ .root.Values.image.tag | quote }}
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
Schedule config block for config files.
Renders explicit task keys with serde(flatten)-compatible structure.
Usage: {{ include "gkg.scheduleConfig" . }}
*/}}
{{- define "gkg.scheduleConfig" -}}
{{- toYaml .Values.schedule.tasks -}}
{{- end }}

{{/*
ClickHouse config block for config files (HTTP connection, no password).
Usage: {{ include "gkg.clickhouseConfig" (dict "config" .Values.clickhouse.datalake) }}
*/}}
{{- define "gkg.clickhouseConfig" -}}
url: "{{ if .config.ssl }}https{{ else }}http{{ end }}://{{ .config.host }}:{{ .config.httpPort }}"
database: {{ .config.database | quote }}
username: {{ .config.user | quote }}
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
Secret volume definition. Only rendered when secrets.existingSecret is set.
Accepts a list of value keys referencing secrets.keys.<name>.
Usage: {{ include "gkg.secretVolume" (dict "root" . "keys" (list "gitlabJwtVerifyingKey" "datalakePassword" "graphPassword")) }}
*/}}
{{- define "gkg.secretVolume" -}}
{{- if .root.Values.secrets.existingSecret -}}
- name: secrets
  secret:
    secretName: {{ .root.Values.secrets.existingSecret }}
    items:
      {{- range .keys }}
      {{- if eq . "gitlabJwtVerifyingKey" }}
      - key: {{ $.root.Values.secrets.keys.gitlabJwtVerifyingKey }}
        path: gitlab/jwt/verifying_key
      {{- else if eq . "gitlabJwtSigningKey" }}
      - key: {{ $.root.Values.secrets.keys.gitlabJwtSigningKey }}
        path: gitlab/jwt/signing_key
      {{- else if eq . "datalakePassword" }}
      - key: {{ $.root.Values.secrets.keys.datalakePassword }}
        path: datalake/password
      {{- else if eq . "graphPassword" }}
      - key: {{ $.root.Values.secrets.keys.graphPassword }}
        path: graph/password
      {{- else if eq . "graphReadPassword" }}
      - key: {{ $.root.Values.secrets.keys.graphReadPassword }}
        path: graph/password
      {{- end }}
      {{- end }}
{{- end }}
{{- end }}

{{/*
Secret volume mount. Only rendered when secrets.existingSecret is set.
Usage: {{ include "gkg.secretVolumeMount" . }}
*/}}
{{- define "gkg.secretVolumeMount" -}}
{{- if .Values.secrets.existingSecret -}}
- name: secrets
  mountPath: /etc/secrets
  readOnly: true
{{- end }}
{{- end }}
