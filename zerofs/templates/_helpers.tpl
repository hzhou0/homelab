{{/*
Gateway helpers take `(dict "root" $ "node" <key>)`: the pair is two peers of one filesystem, so a
name or a label set means nothing without saying which half it belongs to.

Trimming convention: each define opens with `-}}` and closes with `{{-`, so an `nindent`ed body
never contributes a whitespace-only line.
*/}}

{{/*
The pair is structural, not a value: a third node would be a second writer, and a single node has
nothing to fail over to. `role` is only the bootstrap hint — each node asks its peer who is leading
before it acts on it.
*/}}
{{ define "zerofs.gatewayNodes" -}}
- name: a
  role: leader
- name: b
  role: standby
{{- end }}

{{ define "zerofs.gateway.fullname" -}}
{{ printf "%s-gateway-%s" .root.Release.Name .node | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "zerofs.csi.controller.fullname" -}}
{{ printf "%s-csi-controller" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "zerofs.csi.node.fullname" -}}
{{ printf "%s-csi-node" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "zerofs.chartLabels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: homelab-zerofs
{{- end }}

{{ define "zerofs.gateway.labels" -}}
{{ include "zerofs.chartLabels" .root }}
{{ include "zerofs.gateway.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Values.gateway.image.tag | default .root.Chart.AppVersion | quote }}
{{- end }}

{{/*
`component` separates leader from standby. `name` is shared by both, and is what the replication
grant and the anti-affinity rule select on.
*/}}
{{ define "zerofs.gateway.selectorLabels" -}}
app.kubernetes.io/name: zerofs-gateway
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .node }}
{{- end }}

{{ define "zerofs.gateway.podSelectorLabels" -}}
app.kubernetes.io/name: zerofs-gateway
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{ define "zerofs.csi.driverName" -}}
csi.zerofs.net
{{- end }}

{{ define "zerofs.csi.socketDir" -}}
/csi
{{- end }}

{{ define "zerofs.csi.socketName" -}}
csi.sock
{{- end }}

{{ define "zerofs.csi.socketPath" -}}
{{ include "zerofs.csi.socketDir" . }}/{{ include "zerofs.csi.socketName" . }}
{{- end }}

{{/*
The kubelet finds a plugin by driver name, so this path is a contract with it rather than a layout
choice: the node plugin's socket and the registrar's advertised path must resolve to the same file.
*/}}
{{ define "zerofs.csi.pluginDir" -}}
{{ .Values.csi.kubeletDir }}/plugins/{{ include "zerofs.csi.driverName" . }}
{{- end }}

{{ define "zerofs.csi.labels" -}}
{{ include "zerofs.chartLabels" .root }}
{{ include "zerofs.csi.selectorLabels" . }}
{{- end }}

{{ define "zerofs.csi.selectorLabels" -}}
app.kubernetes.io/name: zerofs-csi
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
TOML types by syntax: a fraction that happens to render without a point (1, 0) parses as an integer,
which the config's f64 fields reject at boot.
*/}}
{{ define "zerofs.float" -}}
{{ $s := printf "%v" (float64 .) -}}
{{ if or (contains "." $s) (contains "e" $s) }}{{ $s }}{{ else }}{{ printf "%s.0" $s }}{{ end }}
{{- end }}

{{ define "zerofs.gateway.address" -}}
{{ printf "%s.%s.svc" (include "zerofs.gateway.fullname" .) .root.Release.Namespace }}
{{- end }}

{{ define "zerofs.adminEndpoints" -}}
{{- $root := . -}}
{{- $urls := list -}}
{{- range (include "zerofs.gatewayNodes" . | fromYamlArray) -}}
{{- $urls = append $urls (printf "http://%s:%v" (include "zerofs.gateway.address" (dict "root" $root "node" .name)) $root.Values.gateway.ports.admin) -}}
{{- end -}}
{{ join "," $urls }}
{{- end }}

{{ define "zerofs.ninepEndpoints" -}}
{{- $root := . -}}
{{- $addrs := list -}}
{{- range (include "zerofs.gatewayNodes" . | fromYamlArray) -}}
{{- $addrs = append $addrs (printf "%s:%v" (include "zerofs.gateway.address" (dict "root" $root "node" .name)) $root.Values.gateway.ports.ninep) -}}
{{- end -}}
{{ join "," $addrs }}
{{- end }}

{{/*
Credentials are referenced, never rendered: the file itself is a ConfigMap, and every `${VAR}` in it
is expanded from the pre-created Secret at process start.
*/}}
{{ define "zerofs.gateway.config" -}}
{{- $root := .root -}}
{{- $g := $root.Values.gateway -}}
{{- $peers := list -}}
{{- range (include "zerofs.gatewayNodes" $root | fromYamlArray) -}}
{{- if ne .name $.node -}}
{{- $peers = append $peers (printf "\"%s:%v\"" (include "zerofs.gateway.address" (dict "root" $root "node" .name)) $g.ports.replication) -}}
{{- end -}}
{{- end -}}
[cache]
dir = "{{ $g.cache.dir }}"
disk_size_gb = {{ include "zerofs.float" $g.cache.diskSizeGb }}
{{- with $g.cache.memorySizeGb }}
memory_size_gb = {{ include "zerofs.float" . }}
{{- end }}

[storage]
url = "{{ required "gateway.storage.url is required: s3://bucket/prefix, identical on both nodes" $g.storage.url }}"
encryption_password = "${ZEROFS_PASSWORD}"
{{- with $g.storage.storageClass }}
storage_class = "{{ . }}"
{{- end }}

[aws]
access_key_id = "${AWS_ACCESS_KEY_ID}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY}"
{{- range $key, $value := $g.aws }}
{{- if $value }}
{{ $key }} = {{ $value | quote }}
{{- end }}
{{- end }}

[filesystem]
compression = "{{ $g.filesystem.compression }}"
{{- if $g.filesystem.maxSizeGb }}
max_size_gb = {{ include "zerofs.float" $g.filesystem.maxSizeGb }}
{{- end }}

[servers]

[servers.ninep]
addresses = ["0.0.0.0:{{ $g.ports.ninep }}"]

[servers.rpc]
addresses = ["0.0.0.0:{{ $g.ports.admin }}"]

[prometheus]
addresses = ["0.0.0.0:{{ $g.ports.metrics }}"]

[replication]
node_id = "{{ include "zerofs.gateway.fullname" . }}"
role = "{{ .role }}"
replication_listen = "0.0.0.0:{{ $g.ports.replication }}"
peers = [{{ join ", " $peers }}]
{{- with $g.extraConfig }}

{{ . }}
{{- end }}
{{- end }}

{{ define "zerofs.bindingBackup.fullname" -}}
{{ printf "%s-binding-backup" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "zerofs.bindingBackup.labels" -}}
{{ include "zerofs.chartLabels" . }}
app.kubernetes.io/name: zerofs-binding-backup
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Takes `(dict "secret" <name>)`. Both readers of the object store authenticate the same way, from
key names the pre-created Secret is required to use.
*/}}
{{ define "zerofs.awsCredentialEnv" -}}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .secret }}
      key: accessKeyId
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .secret }}
      key: secretAccessKey
{{- end }}
