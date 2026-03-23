{{- define "siphon.pod" -}}
{{- $name := index . "name" -}}
{{- $deployment := index . "deployment" -}}
{{- $context := index . "context" -}}
{{- $command := index . "command" -}}
{{- $args := index . "args" -}}
{{- $metricsPort := index . "metricsPort" -}}
{{- $podSpec := index . "podSpec" -}}
{{- $labels := index . "labels" -}}
{{- $env := index . "env" -}}
{{- $envFromSecrets := index . "envFromSecrets" -}}
{{- $volumes := index . "volumes" -}}
{{- $volumeMounts := index . "volumeMounts" -}}
{{- $resources := index . "resources" -}}
{{- $globalImage := $context.Values.image | default dict -}}
{{- $deployImage := $deployment.image | default dict -}}
{{- $imageRepo := $deployImage.repository | default $globalImage.repository -}}
{{- $imageTag := $deployImage.tag | default $globalImage.tag -}}
{{- $imagePullPolicy := $deployImage.pullPolicy | default $globalImage.pullPolicy -}}
metadata:
  labels:
    {{- include "siphon.selectorLabels" (dict "name" $name) | nindent 4 }}
    {{- with $labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: {{ $name }}
      image: "{{ $imageRepo }}:{{ $imageTag }}"
      imagePullPolicy: "{{ $imagePullPolicy }}"
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop:
            - ALL
        runAsNonRoot: true
        runAsUser: 65532
      {{- with $command }}
      command:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with $args }}
      args:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- if or $env $envFromSecrets }}
      env:
        {{- range $env }}
        - name: {{ .name }}
          value: {{ .value | quote }}
        {{- end }}
        {{- range $envFromSecrets }}
        - name: {{ .name }}
          valueFrom:
            secretKeyRef:
              name: {{ .secretName }}
              key: {{ .secretKey }}
        {{- end }}
      {{- end }}
      ports:
        - name: http-metrics
          containerPort: {{ $metricsPort }}
      livenessProbe:
        httpGet:
          path: /metrics
          port: http-metrics
        initialDelaySeconds: 10
        periodSeconds: 10
      readinessProbe:
        httpGet:
          path: /metrics
          port: http-metrics
        initialDelaySeconds: 5
        periodSeconds: 10
      resources:
        {{- toYaml $resources | nindent 8 }}
      volumeMounts:
        - name: config-volume
          mountPath: /etc/config
        {{- if $deployment.natsClientCertsSecretName }}
        - name: cert-volume
          mountPath: /etc/ssl/certs/custom
          readOnly: true
        {{- end }}
        {{- range $volumeMounts }}
        - {{ toYaml . | nindent 10 | trim }}
        {{- end }}
  volumes:
    - name: config-volume
      configMap:
        name: {{ $name }}-config
    {{- if $deployment.natsClientCertsSecretName }}
    - name: cert-volume
      secret:
        secretName: {{ $deployment.natsClientCertsSecretName }}
    {{- end }}
    {{- range $volumes }}
    - {{ toYaml . | nindent 6 | trim }}
    {{- end }}
  {{- range $k, $v := $podSpec }}
  {{- with $v }}
  {{ $k }}:
    {{- toYaml $v | nindent 4 }}
  {{- end }}
  {{- end }}
{{- end }}
