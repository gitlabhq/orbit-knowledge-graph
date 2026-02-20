{{/*
Expand the name of the chart.
*/}}
{{- define "gkg.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "gkg.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
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
Common labels
*/}}
{{- define "gkg.labels" -}}
helm.sh/chart: {{ include "gkg.chart" . }}
{{ include "gkg.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "gkg.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gkg.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Component labels - pass component name as .component
*/}}
{{- define "gkg.componentLabels" -}}
helm.sh/chart: {{ include "gkg.chart" .root }}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end }}

{{/*
Component selector labels
*/}}
{{- define "gkg.componentSelectorLabels" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}

{{/*
NATS URL - host:port only, code adds nats:// prefix
When nats.enabled is false, uses nats.url config value
*/}}
{{- define "gkg.natsUrl" -}}
{{- if .Values.nats.enabled -}}
{{ .Release.Name }}-nats:4222
{{- else -}}
{{ .Values.nats.url }}
{{- end -}}
{{- end }}

{{/*
ClickHouse YAML config block for config file. Indentation controlled by caller.
*/}}
{{- define "gkg.clickhouseConfig" -}}
url: "http://{{ .config.host }}:8123"
database: {{ .config.database | quote }}
username: {{ .config.user | quote }}
{{- end }}

{{/*
ClickHouse password secret env var. Passwords stay as env vars to avoid
storing secrets in ConfigMaps — env vars override config file values.
*/}}
{{- define "gkg.clickhouseSecretEnv" -}}
- name: GKG_{{ .prefix }}__PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .secretName }}
      key: password
      optional: true
{{- end }}

{{/*
Security context for containers
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
Pod security context
*/}}
{{- define "gkg.podSecurityContext" -}}
fsGroup: 65532
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end }}
