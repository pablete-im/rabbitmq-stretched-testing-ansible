# Building the RHEL 9 Template with Packer

This directory contains Packer configuration to automatically build the RHEL 9 template VM required by this project.

For a manual approach, see [template-manual-build.md](../template-manual-build.md).

## Prerequisites

- [Packer](https://developer.hashicorp.com/packer/downloads) 1.9.0 or later
- RHEL 9 ISO uploaded to a vCenter datastore
- vCenter credentials with VM creation permissions
- Red Hat subscription (for package updates)

### Installing Packer

Packer is a single binary with no dependencies:

```bash
# macOS
brew install packer

# Linux (download from HashiCorp)
curl -O https://releases.hashicorp.com/packer/1.10.0/packer_1.10.0_linux_amd64.zip
unzip packer_1.10.0_linux_amd64.zip
chmod +x packer
sudo mv packer /usr/local/bin/

# Verify
packer version
```

For restricted environments, download the zip from [releases.hashicorp.com](https://releases.hashicorp.com/packer/) and transfer it to your build machine.

## Usage

### 1. Initialize Packer plugins

```bash
cd docs/packer
packer init rhel9-template.pkr.hcl
```

### 2. Create variables file

```bash
cp variables.pkrvars.hcl.example variables.pkrvars.hcl
```

Edit `variables.pkrvars.hcl` with your environment details:
- vCenter connection info
- Infrastructure names (datacenter, cluster, datastore, network)
- Path to RHEL ISO in datastore
- Red Hat subscription credentials

### 3. Upload RHEL ISO to datastore

Upload the RHEL 9 ISO to your vCenter datastore. Note the path as shown in the datastore browser, e.g.:
```
[vsanDatastore] iso/rhel-9.6-x86_64-dvd.iso
```

### 4. Build the template

```bash
packer build -var-file=variables.pkrvars.hcl rhel9-template.pkr.hcl
```

The build will:
1. Create a VM and boot from the RHEL ISO
2. Run automated installation via kickstart
3. Configure cloud-init for OVF datasource
4. Register with Red Hat and update packages
5. Seal the VM (clean logs, remove SSH keys, etc.)
6. Convert to template

Build time is typically 15-30 minutes depending on network speed.

## What Gets Configured

The template includes:

- **Cloud-init** configured for OVF datasource
- **NetworkManager** with keyfile plugin
- **open-vm-tools** for vSphere integration
- **Seal script** at `/root/seal.sh` for re-sealing after manual changes

## Files

| File | Description |
|------|-------------|
| `rhel9-template.pkr.hcl` | Main Packer configuration |
| `variables.pkrvars.hcl.example` | Example variables (copy and edit) |
| `http/ks.cfg` | Kickstart file for automated installation |
| `scripts/seal.sh` | Seal script (also installed in template) |

## Customization

### Change VM specs

Edit variables in `variables.pkrvars.hcl`:
```hcl
vm_cpus      = 2
vm_memory    = 4096
vm_disk_size = 40960
```

### Add packages

Edit `http/ks.cfg` in the `%packages` section:
```
%packages --ignoremissing
@^minimal-environment
cloud-init
your-package-here
%end
```

### Add provisioning steps

Add provisioner blocks in `rhel9-template.pkr.hcl`:
```hcl
provisioner "shell" {
  inline = [
    "dnf install -y your-package"
  ]
}
```

## Troubleshooting

### Build hangs at "Waiting for SSH"

- Verify network connectivity between Packer host and vCenter
- Check that the VM got an IP via DHCP
- Ensure firewall allows SSH (port 22)

### Kickstart errors

- Check boot command timing (increase `boot_wait` if needed)
- Verify ISO path is correct
- Look at VM console for error messages

### Red Hat subscription fails

- Verify credentials are correct
- Check network connectivity to Red Hat CDN
- Try registering manually first to confirm subscription works

## Re-sealing After Manual Changes

If you convert the template back to a VM to make changes:

1. Make your modifications
2. Run `/root/seal.sh`
3. Convert back to template in vSphere

## Security Notes

- The `variables.pkrvars.hcl` file contains credentials - don't commit it to git
- The temporary root password ("packer") is removed during sealing
- SSH host keys are regenerated on first clone boot
- Machine ID is regenerated on first clone boot
