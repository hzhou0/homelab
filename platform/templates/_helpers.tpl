{{/*
Common labels stamped on platform-owned objects.
*/}}
{{- define "platform.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: homelab-platform
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
The identity Argo syncs the operator's repository as. No kubeconfig holds it: Kubernetes cannot
tell an impersonated request from one made with the identity's own token.
*/}}
{{- define "platform.argoServiceAccountName" -}}
{{ .Values.operator.serviceAccountName }}-argo
{{- end -}}

{{/*
Fully-qualified username every operator deployment arrives as, matched against request.userInfo
by the deployment guardrails.
*/}}
{{- define "platform.operatorUsername" -}}
system:serviceaccount:{{ .Values.operator.namespace }}:{{ include "platform.argoServiceAccountName" . }}
{{- end -}}

{{/*
Glob match values for each tier's namespaces, e.g. "app-*".
*/}}
{{- define "platform.appGlob" -}}{{ .Values.prefixes.app }}*{{- end -}}
{{- define "platform.toolGlob" -}}{{ .Values.prefixes.tool }}*{{- end -}}

{{/*
CEL predicate matching a resource in either governed tier.
*/}}
{{- define "platform.inTierNamespace" -}}
object.metadata.namespace.startsWith('{{ .Values.prefixes.app }}') || object.metadata.namespace.startsWith('{{ .Values.prefixes.tool }}')
{{- end -}}

{{/*
CEL list literal of the StorageClasses a governed claim may name.
*/}}
{{- define "platform.storageClassList" -}}{{ .Values.storageClasses | toJson }}{{- end -}}
