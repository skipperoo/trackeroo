terraform {
  required_providers {
    libvirt = {
      source  = "dmacvicar/libvirt"
      version = "~> 0.8.0"
    }
  }
}

provider "libvirt" {
  uri = "qemu:///system"
  # uri = "qemu+ssh://gigio@10.27.1.116:2056/system?no_verify=1"
}

locals {
  base_image_path = "cloud-init/debian-13-cloud-init.qcow2"
}

resource "libvirt_volume" "vm_base_disks" {
  for_each = { for vm in var.vms : vm.name => vm }
  name   = "${each.value.name}-base.qcow2"
  pool   = "k3s_cluster"
  source = local.base_image_path
  format = "qcow2"
}

# Iterate over each VM spec
resource "libvirt_volume" "vm_disks" {
  for_each = { for vm in var.vms : vm.name => vm }
  name   = "${each.value.name}.qcow2"
  pool   = "k3s_cluster"
  base_volume_id = libvirt_volume.vm_base_disks[each.key].id
  format = "qcow2"
  size   = each.value.os_storage * 1024 * 1024 * 1024
}

resource "libvirt_volume" "vm_longhorn_disks" {
  for_each = { for vm in var.vms : vm.name => vm if vm.longhorn_storage > 0 }
  name   = "${each.value.name}-longhorn.qcow2"
  pool   = "k3s_cluster"
  format = "qcow2"
  size   = each.value.longhorn_storage * 1024 * 1024 * 1024
}

resource "libvirt_cloudinit_disk" "vm_cloudinit" {
  for_each = { for vm in var.vms : vm.name => vm }
  name       = "${each.value.name}-cloudinit.iso"
  user_data  = file(each.value.machine_type == "server" ? "cloud-init/servers/user-data.yaml" : "cloud-init/nodes/user-data.yaml")
  meta_data  = <<EOF
instance-id: ${each.value.name}-01
local-hostname: ${each.value.name}
EOF
  network_config = file("cloud-init/network-config.yaml")
}

resource "libvirt_domain" "vms" {
  for_each = { for vm in var.vms : vm.name => vm }
  name     = each.value.name
  memory   = each.value.ram
  vcpu     = each.value.cpu

  # Boot configuration
  boot_device {
    dev = ["hd"]
  }

  disk {
    volume_id = libvirt_volume.vm_disks[each.key].id
  }

  # Dynamic disk block for longhorn storage (if exists)
  dynamic "disk" {
    for_each = contains(keys(libvirt_volume.vm_longhorn_disks), each.key) ? [libvirt_volume.vm_longhorn_disks[each.key]] : []
    content {
      volume_id = disk.value.id
    }
  }

  # Dynamic network interface blocks
  dynamic "network_interface" {
    for_each = each.value.networks
    content {
      # Direct network or internal libvirt network
      network_name = lookup(network_interface.value, "network", null)
      bridge       = lookup(network_interface.value, "bridge", null)
      macvtap      = lookup(network_interface.value, "macvtap", null)
      mac          = lookup(network_interface.value, "mac", null)
      hostname     = lookup(network_interface.value, "hostname", null)
      wait_for_lease = lookup(network_interface.value, "wait_for_lease", null)
    }
  }

  cloudinit = libvirt_cloudinit_disk.vm_cloudinit[each.key].id

  graphics {
    type = "spice"
  }

  autostart = true
}
