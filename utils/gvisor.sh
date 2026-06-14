sh << 'EOF'
set -e
ARCH=$(uname -m)
URL="https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}"
wget -q "${URL}/runsc" "${URL}/runsc.sha512" \
  "${URL}/containerd-shim-runsc-v1" "${URL}/containerd-shim-runsc-v1.sha512"
sha512sum -c runsc.sha512 -c containerd-shim-runsc-v1.sha512
rm -f runsc.sha512 containerd-shim-runsc-v1.sha512
chmod a+rx runsc containerd-shim-runsc-v1
mv runsc containerd-shim-runsc-v1 /usr/local/bin
mkdir -p /var/lib/rancher/k3s/agent/etc/containerd
cat > /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.tmpl << 'TOML'
{{ template "base" . }}

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.gvisor]
  runtime_type = "io.containerd.runsc.v1"
TOML
rc-service k3s restart 2>/dev/null || rc-service k3s-agent restart
kubectl label node "$(hostname)" gvisor=true
EOF