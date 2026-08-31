{{/*
Component helpers take `(dict "root" $ "component" <name>)`: one release holds every Neon
component, so a name or a label set means nothing without saying which.

Trimming convention: each define opens with `-}}` and closes with `{{-`, so an `nindent`ed body
never contributes a whitespace-only line.
*/}}
{{ define "neon.fullname" -}}
{{ printf "%s-%s" .root.Release.Name .component | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "neon.chartLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: homelab-neon
{{- end }}

{{ define "neon.labels" -}}
{{ include "neon.chartLabels" .root }}
{{ include "neon.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Values.image.tag | default .root.Chart.AppVersion | quote }}
{{- end }}

{{ define "neon.selectorLabels" -}}
app.kubernetes.io/name: neon
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{ define "neon.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{ define "neon.brokerEndpoint" -}}
http://{{ include "neon.fullname" (dict "root" . "component" "storage-broker") }}:{{ .Values.storageBroker.port }}
{{- end }}

{{ define "neon.controllerURL" -}}
http://{{ include "neon.fullname" (dict "root" . "component" "storage-controller") }}:{{ .Values.storageController.port }}
{{- end }}

{{ define "neon.ctlURL" -}}
http://{{ include "neon.fullname" (dict "root" . "component" "ctl") }}:{{ .Values.ctl.port }}
{{- end }}

{{/*
Pageserver and safekeeper names carry the node id rather than an ordinal, because the id is what
the controller keys generations and placement on and it must not move when a list is reordered.
*/}}
{{ define "neon.pageserverName" -}}
{{ include "neon.fullname" (dict "root" .root "component" (printf "pageserver-%v" .id)) }}
{{- end }}

{{ define "neon.safekeeperName" -}}
{{ include "neon.fullname" (dict "root" .root "component" (printf "safekeeper-%v" .id)) }}
{{- end }}

{{/*
The S3 credentials every storage component reads. Neon's remote storage reads the standard AWS
variables rather than anything of its own.
*/}}
{{ define "neon.bucketEnv" -}}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secretName }}
      key: bucketAccessKey
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secretName }}
      key: bucketSecretKey
{{- end }}

{{/*
A TOML inline table, which is what both the pageserver config and the safekeeper's
--remote-storage flag expect.
*/}}
{{ define "neon.remoteStorage" -}}
{bucket_name = "{{ .root.Values.bucket.name }}", bucket_region = "{{ .root.Values.bucket.region }}", prefix_in_bucket = "{{ .prefix }}", endpoint = "{{ .root.Values.bucket.endpoint }}"}
{{- end }}
