# =============================================================================
# Packer Configuration for RHEL 9 Template
# =============================================================================
# Builds a RHEL 9 template VM with cloud-init configured for OVF datasource
#
# Usage:
#   packer init rhel9-template.pkr.hcl
#   packer build -var-file=variables.pkrvars.hcl rhel9-template.pkr.hcl
# =============================================================================

packer {
  required_version = ">= 1.9.0"
  required_plugins {
    vsphere = {
      version = ">= 1.2.0"
      source  = "github.com/hashicorp/vsphere"
    }
  }
}

# =============================================================================
# Variables
# =============================================================================

variable "vcenter_server" {
  type        = string
  description = "vCenter server hostname or IP"
}

variable "vcenter_username" {
  type        = string
  description = "vCenter username"
}

variable "vcenter_password" {
  type        = string
  sensitive   = true
  description = "vCenter password"
}

variable "vcenter_datacenter" {
  type        = string
  description = "vCenter datacenter name"
}

variable "vcenter_cluster" {
  type        = string
  description = "vCenter cluster name"
}

variable "vcenter_datastore" {
  type        = string
  description = "vCenter datastore name"
}

variable "vcenter_network" {
  type        = string
  description = "vCenter network name"
}

variable "vcenter_folder" {
  type        = string
  default     = ""
  description = "vCenter folder for the template (optional)"
}

variable "iso_path" {
  type        = string
  description = "Path to RHEL 9 ISO in vCenter datastore (e.g., [datastore] iso/rhel-9.6-x86_64-dvd.iso)"
}

variable "rh_username" {
  type        = string
  description = "Red Hat subscription username"
}

variable "rh_password" {
  type        = string
  sensitive   = true
  description = "Red Hat subscription password"
}

variable "template_name" {
  type        = string
  default     = "rhel9-6-template"
  description = "Name for the resulting template"
}

variable "vm_cpus" {
  type    = number
  default = 2
}

variable "vm_memory" {
  type    = number
  default = 4096
}

variable "vm_disk_size" {
  type    = number
  default = 40960
}

# =============================================================================
# Source - vSphere ISO
# =============================================================================

source "vsphere-iso" "rhel9" {
  # vCenter connection
  vcenter_server      = var.vcenter_server
  username            = var.vcenter_username
  password            = var.vcenter_password
  insecure_connection = true

  # VM location
  datacenter = var.vcenter_datacenter
  cluster    = var.vcenter_cluster
  datastore  = var.vcenter_datastore
  folder     = var.vcenter_folder

  # VM settings
  vm_name              = var.template_name
  guest_os_type        = "rhel9_64Guest"
  CPUs                 = var.vm_cpus
  RAM                  = var.vm_memory
  RAM_reserve_all      = false
  disk_controller_type = ["pvscsi"]

  storage {
    disk_size             = var.vm_disk_size
    disk_thin_provisioned = true
  }

  network_adapters {
    network      = var.vcenter_network
    network_card = "vmxnet3"
  }

  # Boot configuration
  iso_paths = [var.iso_path]

  boot_order = "disk,cdrom"
  boot_wait  = "5s"
  boot_command = [
    "<up><wait>",
    "e<wait>",
    "<down><down><end>",
    " inst.ks=http://{{ .HTTPIP }}:{{ .HTTPPort }}/ks.cfg",
    "<leftCtrlOn>x<leftCtrlOff>"
  ]

  # HTTP server for kickstart
  http_directory = "http"

  # SSH connection for provisioning
  ssh_username = "root"
  ssh_password = "packer"
  ssh_timeout  = "30m"

  # Shutdown
  shutdown_command = "shutdown -h now"

  # Convert to template
  convert_to_template = true

  # OVF properties for cloud-init
  configuration_parameters = {
    "guestinfo.userdata.encoding" = "base64"
    "guestinfo.userdata"          = ""
  }
}

# =============================================================================
# Build
# =============================================================================

build {
  sources = ["source.vsphere-iso.rhel9"]

  # Register with Red Hat and update
  provisioner "shell" {
    inline = [
      "subscription-manager register --username='${var.rh_username}' --password='${var.rh_password}'",
      "subscription-manager attach --auto",
      "dnf update -y"
    ]
  }

  # Final cleanup and seal
  provisioner "shell" {
    inline = [
      # Unregister from Red Hat (clones will re-register)
      "subscription-manager unregister || true",

      # Clean cloud-init
      "cloud-init clean --logs --seed",

      # Clear machine identity
      "truncate -s 0 /etc/machine-id",
      "rm -f /var/lib/dbus/machine-id",

      # Remove SSH host keys
      "rm -f /etc/ssh/ssh_host_*",

      # Clear NetworkManager state
      "rm -f /var/lib/NetworkManager/*",
      "rm -f /etc/NetworkManager/system-connections/*",

      # Clean DNF
      "dnf clean all",

      # Clear logs
      "rm -rf /var/log/*",
      "journalctl --vacuum-time=1s || true",

      # Clear temp and history
      "rm -rf /tmp/* /var/tmp/*",
      "rm -f /root/.bash_history",
      "history -c",

      # Sync
      "sync"
    ]
  }
}
