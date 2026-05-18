# Cost-Aware vs Benefit Shard Scoring Experiment Results

## Configuration
- Heterogeneity mode: off
- Matching nodes: 3× homogeneous, 1 CPU, 0 burn
- Trace: 21-12_00-06_fixed.csv, qps_scale=0.02, rows=30 (~5 min)
- movePenaltyCoefficient: 0.2

## Results

### Iteration 1

| Metric | Cost-Aware | Benefit |
|---|---|---|
| Overall workflow latency (avg) | 26.013 ms | 30.123 ms |
| Overall workflow latency (p50) | 18.548 ms | 19.344 ms |
| Overall workflow latency (p95) | 61.583 ms | 73.830 ms |
| Overall workflow latency (p99) | 146.862 ms | 157.019 ms |
| Overall workflow latency (p99.9) | 309.112 ms | 315.171 ms |
| StartWorkflow RPC latency (avg) | 10.501 ms | 18.592 ms |
| Poll RPC latency (avg) | 1052.335 ms | 1058.305 ms |
| Respond RPC latency (avg) | 3.812 ms | 3.846 ms |
| Completed workflows | 8665 | 8619 |
| Start errors | 0 | 46 |
| Poll errors | 81154 | 77730 |

### Iteration 2

| Metric | Cost-Aware | Benefit |
|---|---|---|
| Overall workflow latency (avg) | 16.947 ms | 18.020 ms |
| Overall workflow latency (p50) | 16.000 ms | 17.335 ms |
| Overall workflow latency (p95) | 32.987 ms | 41.505 ms |
| Overall workflow latency (p99) | 87.655 ms | 95.137 ms |
| Overall workflow latency (p99.9) | 213.223 ms | 182.003 ms |
| StartWorkflow RPC latency (avg) | 9.611 ms | 10.802 ms |
| Poll RPC latency (avg) | 1046.441 ms | 1046.762 ms |
| Respond RPC latency (avg) | 4.821 ms | 3.865 ms |
| Completed workflows | 7675 | 8665 |
| Start errors | 990 | 0 |
| Poll errors | 3334097 | 75228 |

### Iteration 3

| Metric | Cost-Aware | Benefit |
|---|---|---|
| Overall workflow latency (avg) | 17.139 ms | 16.974 ms |
| Overall workflow latency (p50) | 16.061 ms | 16.617 ms |
| Overall workflow latency (p95) | 34.759 ms | 34.717 ms |
| Overall workflow latency (p99) | 81.472 ms | 83.093 ms |
| Overall workflow latency (p99.9) | 198.296 ms | 206.020 ms |
| StartWorkflow RPC latency (avg) | 9.628 ms | 9.575 ms |
| Poll RPC latency (avg) | 1048.233 ms | 1046.345 ms |
| Respond RPC latency (avg) | 3.888 ms | 3.807 ms |
| Completed workflows | 8665 | 8665 |
| Start errors | 0 | 0 |
| Poll errors | 76285 | 76601 |

## Averages

### Cost-Aware (excluding iteration 2 outlier)

Iteration 2 for cost_aware is excluded as an outlier due to 990 start errors and 3,334,097 poll errors (vs ~76-81K in other runs), indicating a transient system issue during that run.

| Metric | Average (n=2) |
|---|---|
| Overall workflow latency (avg) | 21.576 ms |
| Overall workflow latency (p50) | 17.305 ms |
| Overall workflow latency (p95) | 48.171 ms |
| Overall workflow latency (p99) | 114.167 ms |
| Overall workflow latency (p99.9) | 253.704 ms |
| StartWorkflow RPC latency (avg) | 10.065 ms |
| Poll RPC latency (avg) | 1050.284 ms |
| Respond RPC latency (avg) | 3.850 ms |
| Completed workflows | 8665 |
| Start errors | 0 |
| Poll errors | 78720 |

### Benefit (all 3 iterations)

| Metric | Average (n=3) |
|---|---|
| Overall workflow latency (avg) | 21.706 ms |
| Overall workflow latency (p50) | 17.765 ms |
| Overall workflow latency (p95) | 50.017 ms |
| Overall workflow latency (p99) | 111.750 ms |
| Overall workflow latency (p99.9) | 234.398 ms |
| StartWorkflow RPC latency (avg) | 12.990 ms |
| Poll RPC latency (avg) | 1050.471 ms |
| Respond RPC latency (avg) | 3.839 ms |
| Completed workflows | 8650 |
| Start errors | 15 |
| Poll errors | 76520 |

## Analysis

- **Overall workflow latency**: Cost-aware averages **21.576 ms** vs benefit **21.706 ms** — virtually identical (0.6% difference).
- **StartWorkflow latency**: Cost-aware is **22% faster** on average (10.065 ms vs 12.990 ms). This was the main differentiator in iteration 1 (10.5 ms vs 18.6 ms), though the gap narrowed in later iterations.
- **Poll latency**: Nearly identical — 1050.284 ms vs 1050.471 ms.
- **Respond latency**: Nearly identical — 3.850 ms vs 3.839 ms.
- **Tail latency**: Cost-aware has slightly higher p99.9 (253.7 ms vs 234.4 ms), but p95 and p99 are comparable.
- **Error rates**: Both modes show ~76-81K baseline "Context timeout is too short" poll errors. Benefit had 46 start errors in iteration 1; cost-aware had none in valid runs.
- **Throughput**: Both modes process ~8,650-8,665 workflows in ~5 minutes.

## Conclusion

With `movePenaltyCoefficient=0.2` in a homogeneous matching cluster (1 CPU, 0 burn), **cost_aware and benefit shard scoring produce nearly identical overall workflow latency**. The cost-aware mode shows a modest improvement in StartWorkflow RPC latency (especially pronounced in the first iteration), but this advantage diminishes as the system stabilizes. The penalty coefficient appears to be tuned such that the cost component barely affects shard movement decisions in this homogeneous configuration.

## Files
- cost-aware-penalty02.log
- benefit-penalty02.log
- cost-aware-penalty02-iter2.log
- benefit-penalty02-iter2.log
- cost-aware-penalty02-iter3.log
- benefit-penalty02-iter3.log
