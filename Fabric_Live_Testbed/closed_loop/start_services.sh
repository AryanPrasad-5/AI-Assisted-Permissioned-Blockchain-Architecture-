#!/usr/bin/env bash
# Start the EXISTING E1 containers for a closed-loop session. Run inside Ubuntu-24.04 (WSL) after Docker Desktop is up.
# Same compose start as Integration/system_integration/observability_recovery_2026-11-17/start_existing_e1_services.sh
# (start only: never up/create/rm/pull), but the log goes to a NEW file given as $1, and the Caliper container
# caliper-benchmark-001 is started too (as P1b Step 0 did).
#   bash Fabric_Live_Testbed/closed_loop/start_services.sh /home/mailf18/e1/closed-loop-<date>/start_services.log
set -u
LOG="${1:?usage: start_services.sh <new log file>}"
[ -e "$LOG" ] && { echo "refusing to overwrite $LOG"; exit 2; }
mkdir -p "$(dirname "$LOG")"
TN=/home/mailf18/fabric/fabric-samples/test-network
REPO=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-
export DOCKER_SOCK=/var/run/docker.sock
exec > >(tee "$LOG") 2>&1
cd "$TN/compose" || exit 1
COMPOSE=(docker compose -p e1 -f compose-test-net.yaml -f docker/docker-compose-test-net.yaml
         -f "$REPO/fabric-testbed/network/compose-e1-raft3.yaml")
echo "start UTC: $(date -u +%FT%TZ)"
"${COMPOSE[@]}" start orderer.example.com orderer2.example.com orderer3.example.com; echo "orderers rc=$?"
sleep 12
"${COMPOSE[@]}" start peer0.org1.example.com peer0.org2.example.com; echo "peers rc=$?"
sleep 5
docker start caliper-benchmark-001; echo "caliper rc=$?"
docker ps -a --format 'table {{.Names}}\t{{.Status}}'
for p in 9443 9446 9447 9444 9445; do printf 'healthz %s: ' $p; curl -s -o /dev/null -w '%{http_code}\n' "http://localhost:$p/healthz"; done
echo "end UTC: $(date -u +%FT%TZ)"
