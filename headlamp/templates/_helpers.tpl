{{ define "headlamp.fullname" -}}
{{ .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "headlamp.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "headlamp.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: homelab-headlamp
{{- end }}

{{ define "headlamp.selectorLabels" -}}
app.kubernetes.io/name: headlamp
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{ define "headlamp.subjects" -}}
{{- range .groups }}
- kind: Group
  name: {{ printf "%s%s" $.prefix . | quote }}
  apiGroup: rbac.authorization.k8s.io
{{- end }}
{{- end }}
