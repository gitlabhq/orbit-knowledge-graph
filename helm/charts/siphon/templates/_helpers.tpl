{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "siphon.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels for a deployment instance.
Usage: {{ include "siphon.labels" (dict "name" $name "context" $) }}
*/}}
{{- define "siphon.labels" -}}
{{ include "siphon.selectorLabels" . }}
helm.sh/chart: {{ include "siphon.chart" .context }}
app.kubernetes.io/managed-by: {{ .context.Release.Service }}
app.kubernetes.io/instance: {{ .context.Release.Name }}
{{- if .context.Chart.AppVersion }}
app.kubernetes.io/version: {{ .context.Chart.AppVersion | quote }}
{{- end }}
{{- end }}

{{/*
Selector labels for a deployment instance. These must be stable and
are used in Deployment.spec.selector.matchLabels and PodMonitor selectors.
Usage: {{ include "siphon.selectorLabels" (dict "name" $name) }}
*/}}
{{- define "siphon.selectorLabels" -}}
app: {{ .name }}
{{- end }}

{{/*
Preprocess deployments by merging deploymentProfile defaults into each
deployment that declares a `profile:` key, then normalizing shorthand
syntax (table_mapping, streams).

Merge semantics (same as Helm mergeOverwrite):
  - Profile provides base values for the deployment
  - Per-deployment values override profile values
  - Maps (labels, appConfig, resources, podSpec) are deep-merged (deployment wins)
  - Lists and scalars (command, env, envFromSecrets) are replaced by deployment

table_mapping shorthand:
  - String item:  "users"  →  {table: users, schema: public, subject: users}
  - Map item:     schema defaults to "public", subject defaults to the table value

streams shorthand:
  - String item:  "users"  →  {identifier: users, subject: users, target: siphon_users}
  - Map item:     subject defaults to identifier, target defaults to "siphon_" + identifier

envFromSecrets template expansion:
  - If envFromSecretsTemplate and envFromSecretsName are set (and no explicit envFromSecrets),
    generates envFromSecrets by injecting secretName into each template entry.
  - Template entries that already have secretName keep their value.

Mutates .Values.deployments in place. Idempotent (runs only once per render).
Usage: {{ include "siphon.prepareDeployments" $ }}
*/}}
{{- define "siphon.prepareDeployments" -}}
{{- if not $.Values._deploymentsPrepared -}}
{{- $profiles := $.Values.deploymentProfiles | default dict -}}
{{- range $name, $deployment := $.Values.deployments -}}
{{-   $profileName := $deployment.profile | default "" -}}
{{-   if ne $profileName "" -}}
{{-     if not (hasKey $profiles $profileName) -}}
{{-       fail (printf "deployment %q references profile %q, but it is not defined in deploymentProfiles" $name $profileName) -}}
{{-     end -}}
{{-     $profile := deepCopy (index $profiles $profileName) -}}
{{-     $merged := mergeOverwrite $profile $deployment -}}
{{-     $_ := set $.Values.deployments $name $merged -}}
{{-   end -}}
{{-   $current := index $.Values.deployments $name -}}
{{-   if $current.enabled -}}
{{-     if and $current.appConfig (not (hasKey $current "mode")) -}}
{{-       fail (printf "deployment %q has appConfig but is missing required key \"mode\" (must be one of: producer, consumer, combined)" $name) -}}
{{-     end -}}
{{-     if and (hasKey $current "mode") (not $current.appConfig) -}}
{{-       fail (printf "deployment %q has mode %q but is missing required key \"appConfig\"" $name $current.mode) -}}
{{-     end -}}
{{-   end -}}
{{-   if and (hasKey $current "envFromSecretsTemplate") (hasKey $current "envFromSecretsName") $current.envFromSecretsName (not (hasKey $current "envFromSecrets")) -}}
{{-     $efs := dict "items" (list) -}}
{{-     range $current.envFromSecretsTemplate -}}
{{-       $entry := deepCopy . -}}
{{-       if not (hasKey $entry "secretName") -}}{{- $_ := set $entry "secretName" $current.envFromSecretsName -}}{{- end -}}
{{-       $_ := set $efs "items" (append (index $efs "items") $entry) -}}
{{-     end -}}
{{-     $_ := set $current "envFromSecrets" (index $efs "items") -}}
{{-   end -}}
{{-   if and $current.appConfig (hasKey $current.appConfig "table_mapping") -}}
{{-     $tm := dict "items" (list) -}}
{{-     range $current.appConfig.table_mapping -}}
{{-       if kindIs "string" . -}}
{{-         $_ := set $tm "items" (append (index $tm "items") (dict "table" . "schema" "public" "subject" .)) -}}
{{-       else -}}
{{-         $entry := deepCopy . -}}
{{-         if not (hasKey $entry "table") -}}{{- fail (printf "deployment %q: table_mapping entry is missing required key \"table\": %s" $name (toJson $entry)) -}}{{- end -}}
{{-         if not (hasKey $entry "schema") -}}{{- $_ := set $entry "schema" "public" -}}{{- end -}}
{{-         if not (hasKey $entry "subject") -}}{{- $_ := set $entry "subject" (index $entry "table") -}}{{- end -}}
{{-         $_ := set $tm "items" (append (index $tm "items") $entry) -}}
{{-       end -}}
{{-     end -}}
{{-     $_ := set $current.appConfig "table_mapping" (index $tm "items") -}}
{{-   end -}}
{{-   if and $current.appConfig (hasKey $current.appConfig "streams") -}}
{{-     $st := dict "items" (list) -}}
{{-     range $current.appConfig.streams -}}
{{-       if kindIs "string" . -}}
{{-         $_ := set $st "items" (append (index $st "items") (dict "identifier" . "subject" . "target" (printf "siphon_%s" .))) -}}
{{-       else -}}
{{-         $entry := deepCopy . -}}
{{-         if not (hasKey $entry "identifier") -}}{{- fail (printf "deployment %q: streams entry is missing required key \"identifier\": %s" $name (toJson $entry)) -}}{{- end -}}
{{-         if not (hasKey $entry "subject") -}}{{- $_ := set $entry "subject" (index $entry "identifier") -}}{{- end -}}
{{-         if not (hasKey $entry "target") -}}{{- $_ := set $entry "target" (printf "siphon_%s" (index $entry "identifier")) -}}{{- end -}}
{{-         $_ := set $st "items" (append (index $st "items") $entry) -}}
{{-       end -}}
{{-     end -}}
{{-     $_ := set $current.appConfig "streams" (index $st "items") -}}
{{-   end -}}
{{- end -}}
{{- $_ := set $.Values "_deploymentsPrepared" true -}}
{{- end -}}
{{- end }}

{{/*
Merge appConfigDefaults with a per-deployment appConfig.

Only default sections whose key also appears in the deployment's appConfig
are merged. This prevents unrelated defaults (e.g. database settings) from
leaking into deployments that don't need them (e.g. a consumer).

Usage:
  {{ include "siphon.mergedAppConfig" (dict "defaults" $.Values.appConfigDefaults "override" $deployment.appConfig) }}
*/}}
{{- define "siphon.mergedAppConfig" -}}
{{- $defaults := .defaults | default dict -}}
{{- $override := .override | default dict -}}
{{- $result := dict -}}
{{- range $key, $val := $override -}}
{{-   if and (kindIs "map" $val) (hasKey $defaults $key) (kindIs "map" (index $defaults $key)) -}}
{{-     $_ := set $result $key (mergeOverwrite (deepCopy (index $defaults $key)) $val) -}}
{{-   else -}}
{{-     $_ := set $result $key $val -}}
{{-   end -}}
{{- end -}}
{{- toYaml $result -}}
{{- end }}

{{/*
Wrap a merged appConfig dict according to the deployment's mode.

- mode: producer  → extracts prometheus, wraps remainder in producers:[...]
- mode: consumer  → extracts prometheus, sets type, wraps in consumers:[...]
- mode: combined  → passes appConfig through as-is (no wrapping)

For producer/consumer, prometheus (if present) is lifted to the top level
of the output dict so the combined config matches siphon's expected format.

Usage:
  {{ include "siphon.wrapConfig" (dict "merged" $merged "deployment" $deployment) }}
*/}}
{{- define "siphon.wrapConfig" -}}
{{- $merged := .merged -}}
{{- $deployment := .deployment -}}
{{- $mode := $deployment.mode -}}
{{- if eq $mode "combined" -}}
{{-   toYaml $merged -}}
{{- else -}}
{{-   $prometheus := $merged.prometheus -}}
{{-   $_ := unset $merged "prometheus" -}}
{{-   if eq $mode "consumer" -}}
{{-     $_ := set $merged "type" ($deployment.consumerType | default "clickhouse") -}}
{{-   end -}}
{{-   $wrapKey := ternary "consumers" "producers" (eq $mode "consumer") -}}
{{-   $outDict := dict $wrapKey (list $merged) -}}
{{-   if $prometheus -}}{{- $_ := set $outDict "prometheus" $prometheus -}}{{- end -}}
{{-   toYaml $outDict -}}
{{- end -}}
{{- end }}
