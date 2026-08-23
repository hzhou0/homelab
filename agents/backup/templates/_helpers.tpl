{{ define "agentBackup.fullname" -}}
{{ printf "%s-agent-state" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "agentBackup.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: homelab-agent-backup
app.kubernetes.io/name: agent-state-backup
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
