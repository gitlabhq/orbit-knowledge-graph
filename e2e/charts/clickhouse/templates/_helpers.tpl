{{- define "clickhouse.replicated" -}}
{{- if gt (int .Values.replicas) 1 }}true{{ end -}}
{{- end }}

{{- define "clickhouse.headless" -}}
{{ .Release.Name }}-headless
{{- end }}

{{- define "clickhouse.createDatabase" -}}
{{- $root := index . 0 }}{{ $name := index . 1 -}}
CREATE DATABASE IF NOT EXISTS `{{ $name }}`
{{- if include "clickhouse.replicated" $root }} ON CLUSTER default ENGINE = Replicated('/clickhouse/databases/{{ $name }}', '{shard}', '{replica}'){{ end }};
{{- end }}
