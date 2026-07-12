{{/* charts/datuplet-app/templates/_helpers.tpl */}}

{{/*
datuplet-app.image — render "<repository>:<tag>", defaulting the tag to the
chart appVersion (RFC 024 W2: charts are released as committed; the release
pipeline no longer rewrites values). Usage:
  {{ include "datuplet-app.image" (dict "img" .Values.image.pipelineApi "root" $) }}
*/}}
{{- define "datuplet-app.image" -}}
{{- printf "%s:%s" .img.repository (.img.tag | default .root.Chart.AppVersion) -}}
{{- end -}}

{{- define "datuplet-app.prefix" -}}
{{- /* Empty prefix — namespace disambiguates cross-install collisions. */ -}}
{{- end -}}

{{- define "datuplet-app.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: datuplet
{{- end -}}

{{- define "datuplet-app.selectorLabels" -}}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/part-of: datuplet
{{- end -}}

{{/* Platform Secret references (datuplet-infra → datuplet-app convention).
     datuplet-infra uses no release prefix so these return bare names. */}}
{{- define "datuplet-app.platformSigningKeySecret" -}}
signing-key
{{- end -}}

{{- define "datuplet-app.platformOpenfgaApiKeySecret" -}}
openfga-api-key
{{- end -}}

{{- define "datuplet-app.platformPgPipelineApiSecret" -}}
pg-pipeline-api
{{- end -}}

{{- define "datuplet-app.platformPgPipelineApiPwSecret" -}}
pg-pipeline-api-pw
{{- end -}}

{{- define "datuplet-app.platformClusterName" -}}
pg
{{- end -}}

{{- define "datuplet-app.openfgaHttpUrl" -}}
http://openfga.{{ .Release.Namespace }}.svc.cluster.local:8080
{{- end -}}

{{/* In-cluster lakekeeper catalog URL (with /catalog suffix pipeline-api expects). */}}
{{- define "datuplet-app.lakekeeperUrl" -}}
{{- if .Values.warehouse.lakekeeperUrl -}}
{{- .Values.warehouse.lakekeeperUrl -}}
{{- else -}}
http://lakekeeper.{{ .Release.Namespace }}.svc.cluster.local:8181/catalog
{{- end -}}
{{- end -}}

{{/* pipeline-api's own public URL used for OIDC discovery doc + JWKS issuer.
     Defaults to in-cluster Service URL when publicUrl is not set. */}}
{{- define "datuplet-app.pipelineApiPublicUrl" -}}
{{- if .Values.pipelineApi.publicUrl -}}
{{- .Values.pipelineApi.publicUrl -}}
{{- else -}}
http://{{ include "datuplet-app.prefix" . }}pipeline-api.{{ .Release.Namespace }}.svc.cluster.local:8081
{{- end -}}
{{- end -}}

{{/* pipeline-api's in-cluster Service URL — used by sibling components
     (operator, DG sidecars, commit Jobs) that need to reach pipeline-api
     from inside the cluster regardless of any external publicUrl override.
     ALWAYS the in-cluster Service DNS — never honours publicUrl, because
     publicUrl is for external (browser / CLI) consumers and may not
     resolve from in-cluster pods. */}}
{{- define "datuplet-app.pipelineApiInClusterUrl" -}}
http://{{ include "datuplet-app.prefix" . }}pipeline-api.{{ .Release.Namespace }}.svc.cluster.local:8081
{{- end -}}
