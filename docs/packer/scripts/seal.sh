#!/bin/bash
# =============================================================================
# RHEL 9 Template Seal Script
# =============================================================================
# Run this script before converting a VM to a template to ensure it's properly
# generalized. The VM will shut down automatically when complete.
#
# Usage: sudo /root/seal.sh
# =============================================================================

set -e

echo "=========================================="
echo "Sealing RHEL 9 Template"
echo "=========================================="

# Stop services that might regenerate files
echo "[1/10] Stopping services..."
systemctl stop rsyslog 2>/dev/null || true
systemctl stop auditd 2>/dev/null || true

# Clean cloud-init state so it runs on next boot
echo "[2/10] Cleaning cloud-init..."
cloud-init clean --logs --seed

# Clear machine identity - will be regenerated on first boot
echo "[3/10] Clearing machine identity..."
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id
ln -s /etc/machine-id /var/lib/dbus/machine-id 2>/dev/null || true

# Remove SSH host keys - will be regenerated on first boot
echo "[4/10] Removing SSH host keys..."
rm -f /etc/ssh/ssh_host_*

# Clear NetworkManager connection profiles
echo "[5/10] Clearing NetworkManager state..."
rm -f /var/lib/NetworkManager/*
rm -f /etc/NetworkManager/system-connections/*

# Clean DNF cache
echo "[6/10] Cleaning DNF cache..."
dnf clean all
rm -rf /var/cache/dnf/*

# Clear logs
echo "[7/10] Clearing logs..."
rm -rf /var/log/*
find /var/log -type f -exec truncate -s 0 {} \; 2>/dev/null || true
journalctl --rotate 2>/dev/null || true
journalctl --vacuum-time=1s 2>/dev/null || true

# Clear audit logs
echo "[8/10] Clearing audit logs..."
rm -f /var/log/audit/*
> /var/log/audit/audit.log 2>/dev/null || true

# Clear temp files
echo "[9/10] Clearing temporary files..."
rm -rf /tmp/* /var/tmp/*

# Clear shell history
echo "[10/10] Clearing shell history..."
rm -f /root/.bash_history
rm -f /home/*/.bash_history
unset HISTFILE
history -c

echo "=========================================="
echo "Seal complete. Shutting down..."
echo "=========================================="

# Sync filesystem and shutdown
sync
shutdown -h now
