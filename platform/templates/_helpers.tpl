{{/*
Common labels stamped on platform-owned objects.
*/}}
{{- define "platform.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: homelab-platform
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
Fully-qualified username of the operator ServiceAccount, used by RBAC bindings
and by the constraint self-protection policy (matched against request.userInfo).
*/}}
{{- define "platform.operatorUsername" -}}
system:serviceaccount:{{ .Values.operator.namespace }}:{{ .Values.operator.serviceAccountName }}
{{- end -}}

{{/*
Glob match values for each tier's namespaces, e.g. "app-*".
*/}}
{{- define "platform.appGlob" -}}{{ .Values.prefixes.app }}*{{- end -}}
{{- define "platform.toolGlob" -}}{{ .Values.prefixes.tool }}*{{- end -}}
