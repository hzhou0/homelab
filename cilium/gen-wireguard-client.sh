#!/usr/bin/env bash
# Generate a WireGuard client config for the cluster-hosted tunnel (see templates/wireguard.yaml).
#
# It mints a client keypair, writes the client-side .conf (and a QR code if `qrencode` is present),
# and prints the matching [Peer] block to paste under `wireguard.peers` in your values override.
# AllowedIPs defaults to the Gateway VIP + LAN DNS only, matching the server-side egress fence.
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }
command -v wg >/dev/null || die "wireguard-tools (wg) not found"

# Defaults mirror cilium/values.yaml.
dns="10.0.0.1"
allowed_ips="10.0.0.100/32, 10.0.0.1/32"
# Appended to allowed_ips with -s; must match wireguard.apiServer.address (and .enabled server-side).
apiserver="10.0.0.22/32"
keepalive="25"
port="51820"
outdir="."
# Public WAN endpoint (grey-cloud DDNS A record); OPNsense DNATs :port here to the WG LB IP.
endpoint="vpn.haustorium.net"
name="" address="" server_pubkey="" server_privkey=""

usage() {
  cat >&2 <<EOF
usage: $0 -n NAME -a ADDR (-k SERVER_PUBKEY | -K SERVER_PRIVKEY) [opts]

required:
  -n NAME          peer name (also the output filename)
  -a ADDR          peer tunnel address, e.g. 10.9.0.2/32 (must be free in the server subnet)
  -k SERVER_PUBKEY server public key  (\`wg pubkey < privkey\`), OR
  -K SERVER_PRIVKEY server private key (the values \`wireguard.privateKey\`) to derive it from

options:
  -e ENDPOINT      public WAN host the OPNsense forward lands on (default: $endpoint)
  -d DNS           DNS server pushed to the client         (default: $dns)
  -A ALLOWED_IPS   AllowedIPs routed through the tunnel    (default: $allowed_ips)
  -s               also route the k3s API server ($apiserver) for kubectl over the tunnel
                   (requires wireguard.apiServer.enabled server-side)
  -p PORT          endpoint UDP port                       (default: $port)
  -o DIR           output directory                        (default: $outdir)
EOF
  exit 2
}

want_apiserver=""
while getopts "n:a:e:k:K:d:A:p:o:sh" o; do
  case "$o" in
    n) name=$OPTARG ;;  a) address=$OPTARG ;;  e) endpoint=$OPTARG ;;
    k) server_pubkey=$OPTARG ;;  K) server_privkey=$OPTARG ;;
    d) dns=$OPTARG ;;  A) allowed_ips=$OPTARG ;;  p) port=$OPTARG ;;  o) outdir=$OPTARG ;;
    s) want_apiserver=1 ;;
    *) usage ;;
  esac
done
[[ -n $want_apiserver ]] && allowed_ips="$allowed_ips, $apiserver"

[[ -n $name && -n $address ]] || usage
if [[ -z $server_pubkey ]]; then
  [[ -n $server_privkey ]] || die "need -k SERVER_PUBKEY or -K SERVER_PRIVKEY"
  server_pubkey=$(wg pubkey <<<"$server_privkey")
fi
[[ $endpoint == *:* ]] || endpoint="$endpoint:$port"

client_privkey=$(wg genkey)
client_pubkey=$(wg pubkey <<<"$client_privkey")

mkdir -p "$outdir"
conf="$outdir/$name.conf"
umask 077
cat >"$conf" <<EOF
[Interface]
PrivateKey = $client_privkey
Address = $address
DNS = $dns

[Peer]
PublicKey = $server_pubkey
Endpoint = $endpoint
AllowedIPs = $allowed_ips
# Required: the server-side egress fence relies on conntrack for the return path.
PersistentKeepalive = $keepalive
EOF

echo "wrote $conf" >&2
if command -v qrencode >/dev/null; then
  qrencode -t ansiutf8 <"$conf" >&2
fi

cat >&2 <<EOF

Add this peer to wireguard.peers in your values override, then \`helm upgrade\`:

  - name: $name
    publicKey: "$client_pubkey"
    address: "$address"
EOF