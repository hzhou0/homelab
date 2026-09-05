{{/*
Every helper takes `(dict "root" $ "component" <key>)`: one release holds three workloads, so
nothing can be derived from the release alone.
*/}}
{{ define "identity.fullname" -}}
{{ printf "%s-%s" .root.Release.Name .component | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "identity.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "identity.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/part-of: homelab-identity
{{- end }}

{{ define "identity.selectorLabels" -}}
app.kubernetes.io/name: identity
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Each port is its component's own default rather than a decision made here, so none is configurable.
*/}}
{{ define "identity.ldapPort" }}3890{{ end }}
{{ define "identity.directoryPort" }}17170{{ end }}
{{ define "identity.redisPort" }}6379{{ end }}
{{ define "identity.portalPort" }}9091{{ end }}
{{ define "identity.metricsPort" }}9959{{ end }}

{{ define "identity.ldapURL" -}}
ldap://{{ include "identity.fullname" (dict "root" . "component" "lldap") }}:{{ include "identity.ldapPort" . }}
{{- end }}

{{/*
A portal outside the cookie domain cannot set the session cookie, and outside the gateway's zone is
not served at all — so neither host is a free choice.
*/}}
{{ define "identity.portalHost" -}}
auth.{{ .Values.publishedDomain }}
{{- end }}

{{ define "identity.directoryHost" -}}
users.{{ .Values.publishedDomain }}
{{- end }}
