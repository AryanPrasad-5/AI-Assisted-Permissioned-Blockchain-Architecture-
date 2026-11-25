#!/usr/bin/env bash
# Run "$@" inside WSL with the native-verifier prerequisites (Go 1.26.4, Fabric v2.5.16 source) on the environment.
export PATH=/home/mailf18/sdk/go1.26.4/bin:$PATH
export FABRIC_2_5_16_SOURCE=/home/mailf18/fabric/fabric-src-v2.5.16
cd /mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture- || exit 1
exec "$@"
