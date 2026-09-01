{{/*
Component helpers take `(dict "root" $ "component" <name>)`: one release holds every Neon
component, so a name or a label set means nothing without saying which.
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
Neon's remote storage reads the standard AWS variables rather than anything of its own.
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

{{ define "neon.remoteStorage" -}}
{bucket_name = "{{ .root.Values.bucket.name }}", bucket_region = "{{ .root.Values.bucket.region }}", prefix_in_bucket = "{{ .prefix }}", endpoint = "{{ .root.Values.bucket.endpoint }}"}
{{- end }}

{{/*
Everything a component authenticates with is a function of the private key, so it is derived where
it is used rather than minted once and distributed. Deterministic, so no two pods can disagree.
*/}}
{{ define "neon.authInit" -}}
- name: auth
  image: "{{ .root.Values.ctl.image.repository }}:{{ .root.Values.ctl.image.tag }}"
  imagePullPolicy: {{ .root.Values.ctl.image.pullPolicy }}
  args:
    - derive
    - --dir={{ include "neon.authDir" . }}
    - --scopes={{ .scopes }}
  env:
    {{- include "neon.secretEnv" (dict "root" .root "name" "NEON_CTL_AUTH_KEY" "key" "authPrivateKey") | nindent 4 }}
  volumeMounts:
    {{- include "neon.authVolumeMount" (dict "write" true) | nindent 4 }}
{{- end }}

{{ define "neon.authVolume" -}}
- name: auth
  emptyDir: {}
{{- end }}

{{ define "neon.authDir" -}}
/etc/neon/auth
{{- end }}

{{ define "neon.authVolumeMount" -}}
- name: auth
  mountPath: /etc/neon/auth
  readOnly: {{ not .write }}
{{- end }}

{{ define "neon.authPublicKeyPath" -}}
{{ include "neon.authDir" . }}/public.pem
{{- end }}

{{ define "neon.secretEnv" -}}
- name: {{ .name }}
  valueFrom:
    secretKeyRef:
      name: {{ .root.Values.secretName }}
      key: {{ .key }}
{{- end }}
