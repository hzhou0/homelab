profile_k3s_compute() {
	profile_standard
	modloop_sign=no
	kernel_addons=
	title="K3s Compute Node"
	desc="Alpine Linux image for k3s compute/agent node"
	apks="$apks alpine-conf parted grub grub-efi dosfstools ca-certificates"
	apkovl="overlays/k3s-compute"
}
