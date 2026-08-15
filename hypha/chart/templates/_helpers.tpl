{{/*
Every helper here takes `(dict "root" $ "deployment" <key>)`: one release holds several deployments,
so nothing can be derived from the release alone.

Trimming convention: each define opens with `-}}` and each end closes with `{{-`, so every body
starts and ends flush. A body that keeps its trailing newline puts a whitespace-only line into
whatever `nindent` block includes it.
*/}}
{{ define "hypha.fullname" -}}
{{ printf "%s-%s" .root.Release.Name .deployment | trunc 63 | trimSuffix "-" }}
{{- end }}

{{ define "hypha.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "hypha.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Values.image.tag | default .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/part-of: homelab-hypha
{{- end }}

{{/*
`component` is what separates one deployment's pods, Service endpoints and alerts from another's.
*/}}
{{ define "hypha.selectorLabels" -}}
app.kubernetes.io/name: hypha
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .deployment }}
{{- end }}

{{/*
The two numbers only mean anything together: the process's drain budgets are fixed constants
(hypha/src/lib.rs), and a grace period below their sum SIGKILLs a drain mid-seal, which costs the
next start a full recovery scan.
*/}}
{{ define "hypha.terminationGracePeriod" -}}
{{ $floor := add 35 (int .Values.preStopSleepSeconds) -}}
{{ if lt (int .Values.terminationGracePeriodSeconds) $floor -}}
{{ fail (printf "terminationGracePeriodSeconds (%v) is below %d: 15 s connection drain + 10 s obligation drain + 10 s actor quiesce + %v s preStop delay" .Values.terminationGracePeriodSeconds $floor .Values.preStopSleepSeconds) }}
{{ end -}}
{{ .Values.terminationGracePeriodSeconds }}
{{- end }}

{{/*
TOML types by syntax: a fraction that happens to render without a point (1, 0) parses as an
integer, which the config's f64 fields reject at boot.
*/}}
{{ define "hypha.float" -}}
{{ $s := printf "%v" (float64 .) -}}
{{ if or (contains "." $s) (contains "e" $s) }}{{ $s }}{{ else }}{{ printf "%s.0" $s }}{{ end }}
{{- end }}

{{/*
The series one deployment owns. `job` is its Service name, which the ServiceMonitor carries over, so
its alerts and dashboard never pick up the other deployment's numbers.
*/}}
{{ define "hypha.selector" -}}
job="{{ include "hypha.fullname" . }}",namespace="{{ .root.Release.Namespace }}"
{{- end }}
