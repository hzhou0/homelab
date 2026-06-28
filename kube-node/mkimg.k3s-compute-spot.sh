profile_k3s_compute_spot() {
	profile_standard
	modloop_sign=no
	kernel_addons=
	title="K3s Compute Spot Node"
	desc="Alpine Linux image for k3s compute/agent spot node (no ceph, DHCP DNS)"
	apks="$apks alpine-conf parted grub grub-efi dosfstools ca-certificates dhcpcd"
	apkovl="overlays/k3s-compute-spot"
}
