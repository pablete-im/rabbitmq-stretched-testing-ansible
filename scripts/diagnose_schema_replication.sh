#!/bin/bash
# Quick diagnostic script for schema replication issues
# Run this on a downstream node (az-cluster-2 or tx-cluster-1)

echo "============================================"
echo "Schema Replication Quick Diagnostic"
echo "============================================"
echo ""

echo "1. Current Schema Replication Status:"
echo "--------------------------------------"
rabbitmqctl schema_replication_status
echo ""

echo "2. Global Parameters (should show upstream endpoints):"
echo "--------------------------------------"
rabbitmqctl list_global_parameters
echo ""

echo "3. TLS Certificate Files:"
echo "--------------------------------------"
for file in /etc/rabbitmq/tls/ca.crt /etc/rabbitmq/tls/server.crt /etc/rabbitmq/tls/server.key; do
    if [ -f "$file" ]; then
        ls -lh "$file"
    else
        echo "MISSING: $file"
    fi
done
echo ""

echo "4. TLS Connectivity Test to Upstream Nodes:"
echo "--------------------------------------"
# Get upstream IPs from global parameters
UPSTREAM_IPS=$(rabbitmqctl list_global_parameters | grep schema_definition_sync_upstream_endpoints | grep -oP '\d+\.\d+\.\d+\.\d+' | sort -u)

if [ -z "$UPSTREAM_IPS" ]; then
    echo "ERROR: No upstream endpoints configured!"
else
    for ip in $UPSTREAM_IPS; do
        echo "Testing connection to $ip:5671..."
        timeout 5 bash -c "echo | openssl s_client -connect $ip:5671 \
            -CAfile /etc/rabbitmq/tls/ca.crt 2>&1" | grep -E "Verify return code|subject=|issuer=|error"
        echo ""
    done
fi

echo "5. Recent RabbitMQ Log Errors:"
echo "--------------------------------------"
tail -n 50 /var/log/rabbitmq/rabbit@$(hostname -s).log | grep -i "error\|failed\|schema\|replication" | tail -n 20
echo ""

echo "6. RabbitMQ Configuration (schema replication relevant):"
echo "--------------------------------------"
grep -E "schema_definition_sync|standby" /etc/rabbitmq/rabbitmq.conf
echo ""

echo "============================================"
echo "Diagnostic Complete"
echo "============================================"
echo ""
echo "COMMON ISSUES:"
echo "1. State 'connecting' = Cannot reach upstream (network/TLS issue)"
echo "2. State 'recover' = Connected but authentication failed"
echo "3. Missing global parameters = Endpoints not set"
echo "4. 'Verify return code: 18' = Certificate verification failed"
echo "5. 'Verify return code: 0' = TLS connection successful"
echo ""
