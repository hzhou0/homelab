profile_k3s_db() {
	title="K3s Database Node"
	desc="Alpine Linux image for k3s database node with ZFS"
	image_ext="iso"
	arch="x86_64 aarch64"
	kernel_flavors="lts"
	kernel_cmdline="modules=loop,squashfs,sd-mod,usb-storage quiet"
	initfs_features="ata base bootchart cdrom squashfs ext4 mmc nvme scsi usb virtio"
	grub_mod="disk part_gpt part_msdos linux normal configfile search search_label efi_gop fat iso9660 cat echo ls test true help gzio"
	packages="
		alpine-base
		alpine-conf
		e2fsprogs
		parted
		grub
		grub-efi
		dosfstools
		zfs-lts
		chrony
		ca-certificates
	"
	apkovl="overlays/k3s-db"
}
