profile_k3s_db() {
	profile_standard
	modloop_sign=no
	kernel_addons=
	title="K3s Database Node"
	desc="Alpine Linux image for k3s database node with ZFS"
	apks="$apks alpine-conf parted grub grub-efi dosfstools ca-certificates zfs-lts"
	apkovl="overlays/k3s-db"
}
