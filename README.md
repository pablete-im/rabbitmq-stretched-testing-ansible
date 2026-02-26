# Tanzu RabbitMQ Ansible Lab (Static Inventory)

Automated deployment of a Tanzu RabbitMQ multi-region lab environment using **existing machines** with:

- **3 Clusters** across 2 regions (Arizona + Texas):
  - **Arizona:** 2 Clusters (az-cluster-1, az-cluster-2), each with 3 nodes (1 per AZ)
  - **Texas:** 1 Cluster (tx-cluster-1), with 3 nodes (1 per AZ)
- **TLS 1.2 Security**: End-to-end encryption for client connections and inter-node communication
- **Network Latency Simulation**: Metro (~3ms) within regions, cross-region (~35ms) between Arizona and Texas
- **Warm Standby DR**: AZ-Cluster-1 replicates to AZ-Cluster-2 (regional standby) and TX-Cluster-1 (cross-region DR)
- **9 Nodes Total**: 6 in Arizona, 3 in Texas

## Architecture

```mermaid
---
title: Cluster Topology (3 Nodes per Cluster, 1 per AZ)
---
flowchart TD
    subgraph az_region["AZ Region (3 AZs)"]
        subgraph az-cluster-1["AZ Cluster 1"]
            azrmq01["AZ RMQ 01 (AZ1)"] --- azrmq02["AZ RMQ 02 (AZ2)"]
            azrmq02 --- azrmq03["AZ RMQ 03 (AZ3)"]
            azrmq03 --- azrmq01
        end
        subgraph az-cluster-2["AZ Cluster 2"]
            azrmq04["AZ RMQ 04 (AZ1)"] --- azrmq05["AZ RMQ 05 (AZ2)"]
            azrmq05 --- azrmq06["AZ RMQ 06 (AZ3)"]
            azrmq06 --- azrmq04
        end
    end
    subgraph tx_region["TX Region (3 AZs)"]
        subgraph tx-cluster-1["TX Cluster 1"]
            txrmq01["TX RMQ 01 (AZ1)"] --- txrmq02["TX RMQ 02 (AZ2)"]
            txrmq02 --- txrmq03["TX RMQ 03 (AZ3)"]
            txrmq03 --- txrmq01
        end
    end
    az_region --- tx_region
```

```mermaid
---
title: Warm Standby Replication Flow
---
flowchart LR
    subgraph az_region["AZ Region"]
        direction LR
        az-cluster-1 -- Warm Standby (Metro ~3ms) --> az-cluster-2
    end
    subgraph tx_region["TX Region"]
        direction LR
        tx-cluster-1
    end
    az-cluster-1 -- Warm Standby (Cross-Region ~35ms) --> tx-cluster-1
```

## Prerequisites

- **Workstation (Controller):**
  - Python 3.9+ with Ansible
  - Access to nodes via SSH (key-based auth recommended)
  - RabbitMQ RPM in `bin/` directory (`tanzu-rabbitmq-server-4.2.3-1.el9.x86_64.rpm`)
  - **TLS PKI**: Complete certificate infrastructure in `pki/` directory:
    - `ca.crt` and `ca.key` (Root CA)
    - `{hostname}_rabbitcert.crt` and `{hostname}_rabbitcert.key` for each node
- **Remote Nodes:**
  - RHEL/Rocky Linux 9.7
  - SSH access for Ansible user (e.g., `root`)
  - Internet access (or configured repos) for dependencies (`dnf install erlang`, `iproute-tc`)
  - OpenSSL for certificate operations

## Quick Start

### 1. Install Ansible dependencies

```bash
pip install ansible
ansible-galaxy collection install -r requirements.yml
```

### 2. Prepare Binaries

Place the Tanzu RabbitMQ RPM in the `bin/` directory:
```
bin/tanzu-rabbitmq-server-4.2.3-1.el9.x86_64.rpm
```

### 3. Configure Inventory

Edit `inventory/hosts.yml` to set your **real IP addresses**:

```yaml
    az-cluster-1:
      hosts:
        az-rmq-01:
          ansible_host: 10.85.10.234 
```

### 4. Prepare TLS Certificates

Ensure your PKI structure is in place in the `pki/` directory:

```
pki/
├── ca.crt                              # Root CA certificate
├── pve-schwab-rmq01_rabbitcert.crt     # Host certificates
├── pve-schwab-rmq01_rabbitcert.key     # Host private keys
├── pve-schwab-rmq02_rabbitcert.crt
├── pve-schwab-rmq02_rabbitcert.key
└── ... (for all 9 nodes)
```

### 5. Deploy

```bash
export ANSIBLE_CONFIG=./ansible.cfg
ansible-playbook site.yml
```

This will:
1. Configure TLS certificates and prepare security infrastructure
2. Install Erlang and RabbitMQ with TLS support
3. Configure network latency simulation
4. Set up warm standby replication over TLS

## Playbooks

| Playbook | Description |
|----------|-------------|
| `site.yml` | Master playbook - runs installation and configuration |
| `playbooks/install_rmq.yml` | Installs Erlang and RabbitMQ from local RPM |
| `playbooks/configure_tls.yml` | **NEW** - Configure TLS 1.2 certificates and security |
| `playbooks/configure_latency.yml` | Setup network latency simulation (Metro/Cross-Region) |
| `playbooks/configure_warm_standby.yml` | Configure warm standby replication DR over TLS |
| `playbooks/verify_tls.yml` | **NEW** - Verify TLS configuration and connectivity |
| `playbooks/verify_warm_standby_tls.yml` | **NEW** - Test warm standby replication over TLS |
| `playbooks/uninstall.yml` | **Uninstall** RabbitMQ and revert configuration |
| `playbooks/health_check.yml` | Verify cluster health |

### Cleanup / Uninstall

To remove RabbitMQ and revert changes on the nodes:

```bash
export ANSIBLE_CONFIG=./ansible.cfg
ansible-playbook playbooks/uninstall.yml
```

## Management UIs

After deployment, access the Management UI:

| Cluster | HTTP (Legacy) | HTTPS (TLS) | Credentials |
|---------|---------------|-------------|-------------|
| AZ-Cluster-1 | http://10.85.10.234:15672 | **https://10.85.10.234:15671** | admin / (check vault or default) |
| AZ-Cluster-2 | http://10.85.10.241:15672 | **https://10.85.10.241:15671** | admin / (check vault or default) |
| TX-Cluster-1 | http://10.85.10.244:15672 | **https://10.85.10.244:15671** | admin / (check vault or default) |

**Note**: TLS-enabled HTTPS management interface is available on port 15671 with proper certificate validation.

## TLS Configuration

### Ports and Protocols

| Service | Non-TLS Port | TLS Port | Protocol |
|---------|-------------|----------|----------|
| AMQP | 5672 | **5671** | TLS 1.2/1.3 |
| Management UI | 15672 | **15671** | HTTPS TLS 1.2/1.3 |
| Stream | 5552 | **5551** | TLS 1.2/1.3 |
| Inter-node | 25672 | **25672** | TLS 1.2/1.3 (Erlang Distribution) |

### Client Connection Examples

```bash
# AMQP TLS connection
amqps://username:password@10.85.10.234:5671/

# Management API over HTTPS
curl -k -u admin:password https://10.85.10.234:15671/api/overview

# Verify TLS configuration
export ANSIBLE_CONFIG=./ansible.cfg
ansible-playbook playbooks/verify_tls.yml
```

## Testing Warm Standby Replication

### Automated TLS Testing
```bash
export ANSIBLE_CONFIG=./ansible.cfg
# Test complete warm standby replication over TLS
ansible-playbook playbooks/verify_warm_standby_tls.yml
```

### Manual Testing
1. Log into AZ-Cluster-1 management UI (HTTPS): `https://10.85.10.234:15671`
2. Create a queue and publish messages
3. Log into AZ-Cluster-2 or TX-Cluster-1 management UI
4. Verify the queue and messages appear via warm standby replication
5. Check replication status: `rabbitmqctl standby_replication_status`

**Note**: All replication traffic now uses TLS encryption:
- Schema replication: AMQP TLS port 5671
- Message replication: Stream TLS port 5551

## Troubleshooting

### Health check
```bash
export ANSIBLE_CONFIG=./ansible.cfg
ansible-playbook playbooks/health_check.yml
```

### Manual cluster status
```bash
ansible az-rmq-01 -m command -a "rabbitmqctl cluster_status" -b
ansible az-rmq-04 -m command -a "rabbitmqctl cluster_status" -b
ansible tx-rmq-01 -m command -a "rabbitmqctl cluster_status" -b
```

### Verify latency simulation
```bash
# Metro latency within Arizona (~3ms)
ansible az-rmq-01 -m command -a "ping -c 3 az-rmq-02" -b

# Cross-region latency Arizona → Texas (~35ms)
ansible az-rmq-01 -m command -a "ping -c 3 tx-rmq-01" -b
```

### Reset admin password
```bash
# Reset on all cluster seeds
ansible az-rmq-01,az-rmq-04,tx-rmq-01 -m command -a "rabbitmqctl change_password admin newpassword" -b
```

### Re-run specific stage
```bash
export ANSIBLE_CONFIG=./ansible.cfg
# Re-configure federation
ansible-playbook playbooks/configure_warm_standby.yml
```

### Schema Replication Issues

If schema replication is stuck in `connecting` or `recover` state:

#### Quick Diagnosis

Run the diagnostic playbook:
```bash
export ANSIBLE_CONFIG=./ansible.cfg
ansible-playbook playbooks/diagnose_schema_replication.yml
```

Or run the diagnostic script directly on a downstream node:
```bash
# Copy script to downstream node
ansible az-cluster-2:tx-cluster-1 -m copy \
  -a "src=scripts/diagnose_schema_replication.sh dest=/tmp/diagnose.sh mode=0755" -b

# Execute it
ansible az-cluster-2:tx-cluster-1 -m shell -a "/tmp/diagnose.sh" -b
```

#### Common Issues and Fixes

**1. State: `connecting` (Cannot reach upstream)**

Causes:
- Network connectivity issues
- TLS certificate verification failing
- Firewall blocking port 5671

Fix:
```bash
# Test TLS connectivity manually on downstream node (1-way TLS)
echo | openssl s_client -connect <upstream-ip>:5671 \
  -CAfile /etc/rabbitmq/tls/ca.crt

# Expected: "Verify return code: 0 (ok)"
# If error 18: Certificate verification failed
# If error 20: Unable to verify issuer
```

**2. State: `recover` (Authentication failing)**

Causes:
- Replication user doesn't exist on upstream
- Wrong password in endpoints configuration
- Missing permissions for replication user

Fix - run the fix playbook:
```bash
export ANSIBLE_CONFIG=./ansible.cfg
ansible-playbook playbooks/fix_schema_replication.yml
```

Or manually on upstream first node:
```bash
# Recreate replication user
rabbitmqctl delete_user replication
rabbitmqctl add_schema_replication_user replication <password>
rabbitmqctl set_permissions -p / replication ".*" ".*" ".*"
rabbitmqctl set_permissions -p rabbitmq_schema_definition_sync replication ".*" ".*" ".*"
```

**3. TLS Certificate Issues**

Fix certificate permissions:
```bash
# On all nodes (only server certificates needed)
chown -R rabbitmq:rabbitmq /etc/rabbitmq/tls/
chmod 750 /etc/rabbitmq/tls/
chmod 644 /etc/rabbitmq/tls/ca.crt
chmod 644 /etc/rabbitmq/tls/server.crt
chmod 600 /etc/rabbitmq/tls/server.key

# Restart RabbitMQ
systemctl restart tanzu-rabbitmq-server
```

**4. Reset Schema Replication**

If all else fails, reset schema replication on downstream:
```bash
# On downstream first node (az-rmq-04 or tx-rmq-01)
rabbitmqctl stop_schema_replication
rabbitmqctl clear_global_parameter schema_definition_sync_upstream_endpoints

# Set endpoints again with correct password
rabbitmqctl set_schema_replication_upstream_endpoints \
  '{"endpoints": ["10.85.10.234:5671", "10.85.10.235:5671", "10.85.10.236:5671"], "username": "replication", "password": "YOUR_PASSWORD_HERE"}'

# Restart RabbitMQ
systemctl restart tanzu-rabbitmq-server

# Wait and check status
sleep 30
rabbitmqctl schema_replication_status
```

#### Verification Commands

Check status on downstream:
```bash
# Schema replication status
rabbitmqctl schema_replication_status

# Expected output when working:
# State: running  (or syncing)

# Check configured endpoints
rabbitmqctl list_global_parameters

# Check standby replication
rabbitmqctl standby_replication_status
```

Check on upstream:
```bash
# List replication user
rabbitmqctl list_users | grep replication

# Check user permissions
rabbitmqctl list_user_permissions replication

# Check schema sync status
rabbitmqctl schema_replication_status
```

## Performance Testing

A test framework is included for validating throughput, latency, and replication under load. See [perf-tests/README.md](perf-tests/README.md) for full details.

```bash
export ANSIBLE_CONFIG=./ansible.cfg
# Install test tools
ansible-playbook playbooks/install_perftest.yml

# Run a baseline test against AZ-Cluster-1
./perf-tests/run-test.sh baseline --host 192.168.20.200

# Compare results
./perf-tests/compare-results.sh
```

## Customization

To adapt for your environment:

1. **Different IP range**: Edit `inventory/hosts.yml`
2. **Different latency values**: Edit `latency.*` in `inventory/group_vars/all/main.yml`
3. **Different cluster sizes**: Modify inventory groups and adjust playbooks
4. **Skip latency simulation**: Remove `configure_latency.yml` from `site.yml`

## Production Sizing

For guidance on running warm standby replication at high message rates (trading-adjacent and financial services workloads), see [High-Throughput Warm Standby Sizing Guide](docs/high-throughput-sizing.md). Covers fan-out architecture constraints, disk/network/WAN sizing, replication lag and RPO analysis, and operational procedures for multi-downstream failover.

## File Structure

```
.
├── ansible.cfg                 # Ansible configuration
├── site.yml                    # Master playbook
├── requirements.yml            # Ansible collection dependencies
├── inventory/
│   ├── hosts.yml              # 9-node inventory (3 clusters)
│   └── group_vars/all/
│       ├── main.yml           # Non-secret configuration
│       ├── vault.yml          # Encrypted secrets (not in git)
│       └── vault.yml.example  # Template for secrets
├── playbooks/
│   ├── install_rmq.yml        # RabbitMQ installation
│   ├── configure_latency.yml  # Network latency (metro + cross-region)
│   ├── configure_warm_standby.yml  # Warm standby replication DR
│   ├── health_check.yml       # Environment verification
│   ├── install_perftest.yml   # Performance test tools
│   └── uninstall.yml          # Cleanup / Revert
├── perf-tests/
│   ├── README.md              # Performance testing guide
│   ├── run-test.sh            # Test runner script
│   ├── compare-results.sh     # Results comparison
│   ├── scenarios/             # Test scenario definitions
│   ├── results/               # Test output (git-ignored)
│   └── tools/                 # Downloaded JARs (git-ignored)
├── bin/
│   └── tanzu-rabbitmq-server...rpm # Local RPM (git-ignored)
├── templates/
│   └── rabbitmq.conf.j2       # RabbitMQ configuration template
└── docs/
    └── high-throughput-sizing.md # Sizing guide
```
