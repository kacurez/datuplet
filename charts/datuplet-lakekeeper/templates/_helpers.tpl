{{/* charts/datuplet-lakekeeper/templates/_helpers.tpl */}}

{{- define "datuplet-lakekeeper.prefix" -}}
{{- /* empty prefix — namespace disambiguates */ -}}
{{- end -}}

{{- define "datuplet-lakekeeper.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: datuplet
{{- end -}}

{{- define "datuplet-lakekeeper.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/part-of: datuplet
{{- end -}}

{{/* pipeline-api's own public URL used for OIDC discovery doc + JWKS issuer.
     Defaults to in-cluster Service URL when publicUrl is not set. */}}
{{- define "datuplet-lakekeeper.pipelineApiPublicUrl" -}}
{{- if .Values.pipelineApi.publicUrl -}}
{{- .Values.pipelineApi.publicUrl -}}
{{- else -}}
http://{{ include "datuplet-lakekeeper.prefix" . }}pipeline-api.{{ .Release.Namespace }}.svc.cluster.local:8081
{{- end -}}
{{- end -}}
