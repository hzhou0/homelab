{{/*
The authorization subrequest a published route makes. The response allow-list is explicit because
the empty default copies every header the authorizer returned, including one a client had set.
*/}}
{{ define "cilium.externalAuth" -}}
{{- $e := .Values.externalAuth -}}
- type: ExternalAuth
  externalAuth:
    protocol: HTTP
    backendRef:
      name: {{ $e.service }}
      namespace: {{ $e.namespace }}
      port: {{ $e.port }}
    http:
      path: /api/authz/ext-authz/
      allowedHeaders:
        - accept
        - cookie
        - proxy-authorization
        # A client that cannot hold a session sends a password on every request instead, and this is
        # where it is checked. Only the basic scheme is claimed, so a bearer token passes through.
        - authorization
        # Absent this, the portal infers the scheme from its own plain-HTTP subrequest and sends the
        # browser back to an http:// address after login.
        - x-forwarded-proto
      allowedResponseHeaders:
        - Remote-User
        - Remote-Groups
        - Remote-Name
        - Remote-Email
        - Set-Cookie
        - Location
{{- end }}

{{/*
The namespaces the gateways serve: every route's backend and every raw-TCP listener's target. The
reverse hop and a tunnel peer's reach are both this set, so neither can drift from what is published.
*/}}
{{ define "cilium.gatewayBackends" -}}
{{- $ns := list -}}
{{- range $name, $g := .Values.gateways -}}
{{- if $g.enabled -}}
{{- range $r := $g.routes }}{{ $ns = append $ns $r.backend.namespace }}{{ end -}}
{{- range $l := $g.tcpListeners }}{{ $ns = append $ns $l.namespace }}{{ end -}}
{{- end -}}
{{- end -}}
{{- $ns | uniq | sortAlpha | join "," -}}
{{- end }}
