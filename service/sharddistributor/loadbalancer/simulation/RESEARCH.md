# Shard Load-Balancing Research Notes

## Constraints From P9-Paper

- Shard moves are expensive because executors must stop processing and rebuild state, caches, and memory on the receiver.
- Raw shard load reports are noisy; balancing decisions should use smoothed load and should not chase transient spikes.
- Stability mechanisms matter: hysteresis bands, per-shard cooldowns, move budgets, and positive cost-benefit gating all reduce churn.
- Future alternatives called out by the paper include explicit move-cost scoring, predictive balancing, and adaptive budgets.

## Baseline: Production Greedy Strategy

Hypothesis: Use the production greedy planner as the comparison point only.

Algorithm: `greedy.PlanRebalance` through the simulation harness.

Replay command:

```bash
go run ./cmd/sharddistributor-lb-replay -csv 21-12_00-06_fixed.csv
```

Metrics:

| Algorithm | Moves | Avg smoothed CV | Worst smoothed CV | Avg reported CV | Worst reported CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| greedy baseline | 90 | 0.0627867884 | 0.1523038376 | 0.1256495540 | 0.4259208954 |

Conclusion: This is a strong baseline for the replay. It keeps sustained smoothed imbalance low with modest churn.

## Experiment 1: Cost-Aware Global Candidate Search

Hypothesis: A global search over all source/destination/shard candidates, with explicit synthetic move cost, per-executor move caps, and adaptive budgets, can reduce churn while preserving similar average balance.

Algorithm change: Added simulation-only `cost-aware` planner. It does not call `greedy.PlanRebalance`; it computes smoothed executor loads, scores every eligible move by SSE benefit minus move cost and overshoot penalty, applies per-shard cooldown, and shrinks the per-row budget when the current CV is near target.

Replay command:

```bash
go run ./cmd/sharddistributor-lb-replay -csv 21-12_00-06_fixed.csv -algorithm cost-aware
```

Metrics:

| Algorithm | Moves | Avg smoothed CV | Worst smoothed CV | Avg reported CV | Worst reported CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| cost-aware default | 110 | 0.0652968275 | 0.1832115686 | 0.1439192574 | 0.3912958173 |

Conclusion: Not a win. It improves reported worst CV but uses more moves and worsens average reported and smoothed balance. The adaptive caps needed to relax during severe imbalance; without relaxation, the worst smoothed CV reached 0.3092970728.

## Experiment 2: Conservative Cost-Aware

Hypothesis: Keeping the global cost-aware scoring but using wider bands and longer cooldown can provide a lower-churn option with acceptable imbalance.

Replay command:

```bash
go run ./cmd/sharddistributor-lb-replay -csv 21-12_00-06_fixed.csv \
  -algorithm cost-aware \
  -hysteresis-upper-band 1.20 \
  -hysteresis-lower-band 0.85 \
  -severe-imbalance-ratio 1.40 \
  -move-budget-proportion 0.005 \
  -per-shard-cooldown 3m
```

Metrics:

| Algorithm | Moves | Avg smoothed CV | Worst smoothed CV | Avg reported CV | Worst reported CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| cost-aware conservative | 41 | 0.0783940545 | 0.2350790854 | 0.1440119409 | 0.3982630358 |

Conclusion: Not a win. Move count falls by 54%, but the average smoothed and reported balance regressions are too large for stateful shards whose objective is sustained load balance without unjustified churn.

## Experiment 3: Aggressive Cost-Aware

Hypothesis: Narrower watermarks can improve sustained imbalance enough to justify more moves.

Replay command:

```bash
go run ./cmd/sharddistributor-lb-replay -csv 21-12_00-06_fixed.csv \
  -algorithm cost-aware \
  -hysteresis-upper-band 1.10 \
  -hysteresis-lower-band 0.90 \
  -severe-imbalance-ratio 1.20 \
  -cost-aware-min-benefit-ratio 0.001
```

Metrics:

| Algorithm | Moves | Avg smoothed CV | Worst smoothed CV | Avg reported CV | Worst reported CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| cost-aware aggressive | 132 | 0.0566585612 | 0.2203659340 | 0.1193653877 | 0.5176576929 |

Conclusion: Not a clean tradeoff. Average CV improves, but moves increase by 47% and worst reported CV is much worse.

## Experiment 4: Threshold Half-Gap Planner

Hypothesis: A simpler dynamic-watermark algorithm that always picks the heaviest source, lightest eligible destination, and shard closest to half the load gap can match greedy balance with clearer explainability.

Algorithm change: Added simulation-only `threshold` planner. It does not call `greedy.PlanRebalance`; it uses watermarks and cooldowns, then chooses the shard whose smoothed load most closely halves the source/destination load gap.

Replay command:

```bash
go run ./cmd/sharddistributor-lb-replay -csv 21-12_00-06_fixed.csv -algorithm threshold
```

Metrics:

| Algorithm | Moves | Avg smoothed CV | Worst smoothed CV | Avg reported CV | Worst reported CV |
| --- | ---: | ---: | ---: | ---: | ---: |
| threshold half-gap | 90 | 0.0627867884 | 0.1523038376 | 0.1256495540 | 0.4259208954 |

Conclusion: Neutral. On this replay, the half-gap choice converges to the same move count and aggregate metrics as the greedy baseline. It is useful as a simpler independent comparison, but it is not a better balance/churn tradeoff.

## Current Conclusion

No simulation-only alternative currently beats the production greedy baseline on the full replay. The most promising direction remains adaptive move budgeting, but the cost-aware implementation needs a better severe-imbalance recovery policy before it can justify replacing the baseline.
