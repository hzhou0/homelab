{{- define "opnsense-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "opnsense-operator.fullname" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "opnsense-operator.serviceAccountName" -}}
{{- default (include "opnsense-operator.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "opnsense-operator.secretName" -}}
{{- if .Values.opnsense.existingSecret -}}
{{- .Values.opnsense.existingSecret -}}
{{- else -}}
{{- printf "%s-creds" (include "opnsense-operator.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "opnsense-operator.labels" -}}
app.kubernetes.io/name: {{ include "opnsense-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: homelab
{{- end -}}

{{- define "opnsense-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "opnsense-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "opnsense-operator.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
