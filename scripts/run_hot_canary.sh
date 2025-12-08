#!/usr/bin/env bash
set -euo pipefail

# run N shard-distributor canary processes with synthetic hot-load settings.
# we log per-canary-process to /tmp/hot-canary-{idx}.log (good for debugging!)
#
# Usage:
#   scripts/run_hot_canary.sh {Number of canary processes to run, i.e. 2 will run two processes that has an executor in each of the two namespaces}
#   And the knobs can be adjusted as needed...

N=${1:-3}

CMD="./sharddistributor-canary start --fixed-namespace shard-distributor-canary --ephemeral-namespace shard-distributor-canary-ephemeral"

if [[ ! -x ./sharddistributor-canary ]]; then
  echo "Building sharddistributor-canary binary..."
  make sharddistributor-canary >/dev/null
fi

export SD_HOT_FRACTION=${SD_HOT_FRACTION:-0.05}
export SD_HOT_MULTIPLIER=${SD_HOT_MULTIPLIER:-8.0}
export SD_HOT_ROTATE_SECONDS=${SD_HOT_ROTATE_SECONDS:-120}
export SD_LOAD_NOISE_PCT=${SD_LOAD_NOISE_PCT:-0.10}

export SD_EPH_HEAVY_PROB=${SD_EPH_HEAVY_PROB:-0.20}
export SD_EPH_HEAVY_MULTIPLIER=${SD_EPH_HEAVY_MULTIPLIER:-6.0}
export SD_EPH_LOAD_NOISE_PCT=${SD_EPH_LOAD_NOISE_PCT:-0.10}

BASE_EXEC_SCALE=${BASE_EXEC_SCALE:-1.0}
EXEC_SCALE_STEP=${EXEC_SCALE_STEP:-0.0}

echo "Launching $N canary processes with follwing hot-load settings:"
for i in $(seq 1 "$N"); do
  scale=$(python3 - <<PY
base = float("$BASE_EXEC_SCALE")
step = float("$EXEC_SCALE_STEP")
i = int("$i")
print(base + step*(i-1))
PY
)
  echo "  proc $i: SD_EXEC_LOAD_SCALE=$scale"
  (
    export SD_EXEC_LOAD_SCALE="$scale"
    exec $CMD >"/tmp/hot-canary-$i.log" 2>&1 &
  )
  sleep 1
done

echo "Done. Tail logs with: tail -f /tmp/hot-canary-*.log"
