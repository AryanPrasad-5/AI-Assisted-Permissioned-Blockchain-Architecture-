#!/usr/bin/env bash
# READ-ONLY readiness check for a closed-loop session (WSL, containers already started). Changes nothing on Fabric:
# docker inspect / ps, file metadata, `--version` calls and one `peer channel list` (a read).
#   bash Fabric_Live_Testbed/closed_loop/preflight_check.sh
set -u
REPO=/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-
BIN=/home/mailf18/fabric/fabric-samples/bin
TN=/home/mailf18/fabric/fabric-samples/test-network
MSP=$TN/organizations/ordererOrganizations/example.com/users/Admin@example.com/msp
echo "UTC $(date -u +%FT%TZ)  boot_id $(cat /proc/sys/kernel/random/boot_id)"
echo "python3: $(python3 --version 2>&1)"
echo "MemAvailable MB: $(awk '/MemAvailable/{print int($2/1024)}' /proc/meminfo)   disk free /home: $(df -BG --output=avail /home | tail -1)"
for n in orderer.example.com orderer2.example.com orderer3.example.com peer0.org1.example.com peer0.org2.example.com caliper-benchmark-001; do
  printf '%-26s %s\n' "$n" "$(docker inspect --format '{{.State.Running}} restarts={{.RestartCount}} started={{.State.StartedAt}}' "$n" 2>&1)"
done
echo "caliper workspace mount: $(docker inspect --format '{{range .Mounts}}{{.Source}}->{{.Destination}} {{end}}' caliper-benchmark-001 2>&1)"
echo "caliper processes: $(docker top caliper-benchmark-001 -o pid,args 2>&1 | tail -n +2 | tr '\n' ';')"
echo "peer: $($BIN/peer version 2>&1 | grep -m1 Version)   configtxlator: $($BIN/configtxlator version 2>&1 | grep -m1 Version)"
echo "orderer admin keystore (actuator needs dir not group/world-writable; key 0600 only if native verification is required):"
stat -c '  %A %U %n' "$MSP/keystore" "$MSP"/keystore/* 2>&1
echo "native verifier prerequisites (needed only for native_verification=required):"
echo "  go: $(command -v go || echo ABSENT)  FABRIC_2_5_16_SOURCE=${FABRIC_2_5_16_SOURCE:-UNSET}"
[ -n "${FABRIC_2_5_16_SOURCE:-}" ] && echo "  source commit: $(git -C "$FABRIC_2_5_16_SOURCE" rev-parse HEAD 2>&1)"
echo "actuator fence dir: $(ls -d /home/mailf18/fabric/fabric-samples/.e1-actuator-fence 2>&1)"
echo "existing e1-cl* channels joined by peer0.org1 (must not include a run's channel):"
( export FABRIC_CFG_PATH=/home/mailf18/fabric/fabric-samples/config CORE_PEER_TLS_ENABLED=true CORE_PEER_LOCALMSPID=Org1MSP \
    CORE_PEER_MSPCONFIGPATH=$TN/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp \
    CORE_PEER_TLS_ROOTCERT_FILE=$TN/organizations/peerOrganizations/org1.example.com/tlsca/tlsca.org1.example.com-cert.pem \
    CORE_PEER_ADDRESS=localhost:7051; $BIN/peer channel list 2>/dev/null | grep -E '^e1-c[ls]' || echo "  none" )
cd "$REPO" && python3 -B -c "import Fabric_Live_Testbed.closed_loop.runner as r; print('runner import OK', r.PACKAGE_VERSION)"
