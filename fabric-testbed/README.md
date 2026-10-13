# Hyperledger Fabric Local Testbed

This module contains the infrastructure, configurations, and telemetry hooks for running controlled Hyperledger Fabric experiments on a local machine.

```text
fabric-testbed/
├── README.md           # Testbed documentation & setup guide
├── configs/            # Fabric network crypto material & docker-compose configurations
├── workloads/          # Hyperledger Caliper benchmark workload definitions
└── telemetry/          # Prometheus metrics & local execution traces (ignored in git)
```

### Important Notice:
This module is scheduled for **Milestone 8** (`feature/fabric-testbed`). It will only be activated after the statistical auditing, exploratory data analysis, and baseline models on public datasets are validated.
