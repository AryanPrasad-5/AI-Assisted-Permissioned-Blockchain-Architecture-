# Methodology: Performance Prediction and Adaptive Control

## 1. Problem Formulation

Hyperledger Fabric performance depends heavily on the interaction between:
1. **Workload Demands ($W$)**: Transaction arrival rate, transaction type, payload size.
2. **System Configurations ($C$)**: Batch size (`MaxMessageCount`), batch timeout (`BatchTimeout`), block size limits, endorsement policies, peer resources.
3. **Internal State / Dynamics ($S$)**: Queue length, memory usage, CPU load, network latency between nodes.

Our goal is to model the mapping:
$$f(W, C, S) \to (T, L)$$
where $T$ is transaction throughput (TPS) and $L$ is transaction latency (seconds or milliseconds).

---

## 2. The Three Control Paradigms

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Static Control                                           │
│    Configuration C is selected a priori and kept constant   │
│    C_t = C_fixed                                            │
└─────────────────────────────────────────────────────────────┘
                               ▲
                               │ Compare against
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Reactive Control                                         │
│    Threshold check on observed degradation                  │
│    IF L_{t-1} > L_thresh OR Queue_{t-1} > Q_thresh:         │
│         C_t = Adjust(C_{t-1})                               │
└─────────────────────────────────────────────────────────────┘
                               ▲
                               │ Compare against
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. Predictive Control                                       │
│    Forecast future performance or overload risk:            │
│    (T_hat_{t+k}, L_hat_{t+k}) = M(W_t, C_t, S_t)            │
│    C_t = argmax_C [ Utility(T_hat, L_hat) ]                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Modeling Strategies

### Regression Baselines & Non-linear Learners
- **Multiple Linear / Ridge / Lasso Regression**: Establish interpretability baselines and check for linear trends.
- **Tree Ensembles (Random Forest, Gradient Boosting)**: Capture non-linear saturation curves (e.g., knee-points where increased transaction rate saturates ordering capacity and causes latency explosion).
- **Multi-Output Regression vs. Chained Models**: Modeling throughput and latency jointly without leaking measured target variables into one another.

### Leakage-Safe Feature Sets
- **Allowed Predictors**:
  - Workload parameters ($W$): arrival rate, transaction complexity.
  - Configuration parameters ($C$): batch size, batch timeout, organization count.
  - Prior/Lagged system states ($S_{t-1}$): previous queue length, recent rolling CPU.
- **Strictly Prohibited Predictors**:
  - Contemporaneous measured targets: Using contemporaneous measured throughput to predict contemporaneous latency is target leakage unless measured throughput is realistically accessible before latency is determined.

---

## 4. Operational Considerations for Hyperledger Fabric

- **Parameter Settling Time**: In Fabric, updating channel configuration (e.g., `BatchSize` or `BatchTimeout`) requires a configuration transaction through the ordering service, which introduces operational delay.
- **Oscillation Avoidance**: Controllers must incorporate dampening / hysteresis thresholds to prevent rapid flapping between parameter states.
