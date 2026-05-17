profile_k3s_server() {
	profile_standard
	modloop_sign=no
	kernel_addons=
	title="K3s Control Plane"
	desc="Alpine Linux image for k3s server node"
	apks="$apks alpine-conf parted grub grub-efi dosfstools ca-certificates"
	apkovl="overlays/k3s-server"
}
