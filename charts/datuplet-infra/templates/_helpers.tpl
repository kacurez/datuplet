{{/* charts/datuplet-infra/templates/_helpers.tpl */}}

{{/* Release-name prefix used in resource names. */}}
{{- define "datuplet-infra.prefix" -}}
{{- /* empty prefix — namespace disambiguates */ -}}
{{- end -}}

{{/* Standard labels — applied to every resource. */}}
{{- define "datuplet-infra.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: datuplet
{{- end -}}

{{/* Selector labels for a specific component. Call with `(dict "ctx" . "name" "openfga")` */}}
{{- define "datuplet-infra.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/part-of: datuplet
{{- end -}}

{{/* OpenFGA HTTP URL — used by bootstrap + lakekeeper.
     Port 8080 is the openfga subchart's HTTP service port.
     Port 8081 is gRPC (used by lakekeeper authz client). */}}
{{- define "datuplet-infra.openfgaHttpUrl" -}}
http://openfga.{{ .Release.Namespace }}.svc.cluster.local:8080
{{- end -}}

{{- define "datuplet-infra.openfgaGrpcUrl" -}}
openfga.{{ .Release.Namespace }}.svc.cluster.local:8081
{{- end -}}

{{/* Lakekeeper REST URL. */}}
{{- define "datuplet-infra.lakekeeperUrl" -}}
http://lakekeeper.{{ .Release.Namespace }}.svc.cluster.local:8181
{{- end -}}

{{/* Default signing-key Secret name. */}}
{{- define "datuplet-infra.signingKeySecretName" -}}
{{- if .Values.pipelineApi.signingKey.existingSecret -}}
{{- .Values.pipelineApi.signingKey.existingSecret -}}
{{- else if .Values.pipelineApi.signingKey.secretName -}}
{{- .Values.pipelineApi.signingKey.secretName -}}
{{- else -}}
{{- include "datuplet-infra.prefix" . -}}signing-key
{{- end -}}
{{- end -}}
