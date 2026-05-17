{{/* charts/datuplet-operators/templates/_helpers.tpl */}}

{{- define "datuplet-operators.prefix" -}}
{{- /* empty prefix — namespace disambiguates */ -}}
{{- end -}}

{{- define "datuplet-operators.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: datuplet
{{- end -}}
