# Building the RHEL 9 Template VM (Manual)

This guide walks through manually creating the RHEL 9 template VM required for this project. For an automated approach, see the [Packer configuration](packer/README.md).

## Prerequisites

- RHEL 9.x ISO (tested with 9.6)
- vSphere 8 environment with permissions to create VMs and templates
- Red Hat subscription for package updates

## Overview

The template needs:
1. RHEL 9 minimal installation
2. Cloud-init configured for OVF datasource
3. NetworkManager with keyfile plugin
4. Properly sealed (generalized) for cloning

## Step 1: Create the VM

In vSphere:

1. **Create a new VM** with these settings:
   - Guest OS: Red Hat Enterprise Linux 9 (64-bit)
   - CPU: 2 vCPU
   - Memory: 4 GB
   - Disk: 40 GB (thin provisioned)
   - Network: Your management network
   - CD/DVD: Connected to RHEL 9 ISO

2. **Enable OVF properties** (required for cloud-init):
   - Right-click VM → Edit Settings → VM Options → Advanced
   - Configuration Parameters → Edit Configuration
   - Add parameter: `guestinfo.userdata.encoding` = `base64`
   - Add parameter: `guestinfo.userdata` = `` (empty for now)

## Step 2: Install RHEL 9

Boot the VM and install RHEL 9:

1. Select **Minimal Install** (Server without GUI)
2. Configure:
   - **Root password**: Set a temporary password
   - **User creation**: Skip (cloud-init will create users)
   - **Network**: Enable DHCP for initial setup
   - **Partitioning**: Automatic (or customize as needed)

3. Complete installation and reboot

## Step 3: Initial Configuration

SSH into the VM (or use console) and run these commands as root:

### Register with Red Hat (required for packages)

```bash
subscription-manager register --username=YOUR_USERNAME --password=YOUR_PASSWORD
subscription-manager attach --auto
```

### Update system

```bash
dnf update -y
```

### Install required packages

```bash
dnf install -y \
    cloud-init \
    cloud-utils-growpart \
    python3 \
    open-vm-tools
```

## Step 4: Configure Cloud-Init

### Enable OVF datasource

Edit `/etc/cloud/cloud.cfg.d/99_vmware.cfg`:

```yaml
datasource_list: [ OVF, None ]
datasource:
  OVF:
    allow_raw_data: true
```

### Disable network config from cloud-init

We'll let cloud-init userdata handle networking via runcmd. Edit `/etc/cloud/cloud.cfg.d/99_disable_network.cfg`:

```yaml
network:
  config: disabled
```

### Enable cloud-init services

```bash
systemctl enable cloud-init-local
systemctl enable cloud-init
systemctl enable cloud-config
systemctl enable cloud-final
```

## Step 5: Configure NetworkManager

### Switch to keyfile plugin

This makes NetworkManager work better with cloud-init. Edit `/etc/NetworkManager/NetworkManager.conf`:

```ini
[main]
plugins=keyfile

[keyfile]
unmanaged-devices=none
```

### Remove existing connections

```bash
rm -f /etc/NetworkManager/system-connections/*
nmcli connection delete "$(nmcli -t -f NAME connection show)" 2>/dev/null || true
```

## Step 6: Configure VMware Tools

```bash
systemctl enable vmtoolsd
```

## Step 7: (Optional) Pre-install Erlang Dependencies

To speed up RabbitMQ installation, you can pre-install Erlang:

```bash
# Import keys
rpm --import https://github.com/rabbitmq/signing-keys/releases/download/3.0/rabbitmq-release-signing-key.asc
rpm --import https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key

# Add Erlang repo
cat > /etc/yum.repos.d/rabbitmq-erlang.repo << 'EOF'
[rabbitmq-erlang]
name=RabbitMQ Erlang
baseurl=https://yum1.rabbitmq.com/erlang/el/9/$basearch
enabled=1
gpgcheck=1
gpgkey=https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key
EOF

# Install Erlang
dnf install -y erlang
```

## Step 8: Install Seal Script

Copy the seal script to the template so it can be re-sealed after manual changes:

```bash
cat > /root/seal.sh << 'SEAL_SCRIPT'
#!/bin/bash
# RHEL 9 Template Seal Script
# Run before converting VM to template: sudo /root/seal.sh

set -e
echo "Sealing RHEL 9 Template..."

# Clean cloud-init
cloud-init clean --logs --seed

# Clear machine identity
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id

# Remove SSH host keys
rm -f /etc/ssh/ssh_host_*

# Clear NetworkManager state
rm -f /var/lib/NetworkManager/*
rm -f /etc/NetworkManager/system-connections/*

# Clean DNF
dnf clean all

# Clear logs
rm -rf /var/log/*
journalctl --vacuum-time=1s 2>/dev/null || true

# Clear temp and history
rm -rf /tmp/* /var/tmp/*
rm -f /root/.bash_history /home/*/.bash_history
history -c

echo "Seal complete. Shutting down..."
sync
shutdown -h now
SEAL_SCRIPT

chmod +x /root/seal.sh
```

## Step 9: Seal the Template

Run the seal script to prepare for templating:

```bash
/root/seal.sh
```

The VM will shut down automatically.

## Step 10: Convert to Template

In vSphere:

1. Right-click the VM → **Template** → **Convert to Template**
2. Name it `rhel9-6-template` (or update `main.yml` with your chosen name)

## Verification

To verify the template works:

1. Clone a test VM from the template
2. Add userdata via OVF properties (base64 encoded cloud-config)
3. Power on and verify:
   - Cloud-init runs and applies configuration
   - Network configures correctly
   - SSH works with configured users

## Troubleshooting

### Cloud-init doesn't run

- Check OVF properties are set on the VM
- Verify datasource config: `cat /etc/cloud/cloud.cfg.d/99_vmware.cfg`
- Check cloud-init logs: `journalctl -u cloud-init`

### Network doesn't configure

- Verify NetworkManager is using keyfile plugin
- Check for leftover connections: `nmcli connection show`
- Review cloud-init userdata for syntax errors

### SSH host keys not regenerated

- Ensure `/etc/ssh/ssh_host_*` files were removed before templating
- Regenerate manually: `ssh-keygen -A`

## Re-sealing After Changes

If you boot the template VM to make changes:

1. Make your modifications
2. Run `/root/seal.sh`
3. Convert back to template

This ensures the template remains properly generalized.
