for p in cadence-matching-a-0 cadence-matching-b-0 cadence-matching-c-0; do
  echo "== $p tasklist 0/1 =="
  kubectl exec -n cadence-kind-lab "$p" -- sh -c 'curl -fsS localhost:9090/metrics | grep -E "tasklist=\"0\"|tasklist=\"1\"" | grep -E "AddDecisionTask|EstimatedAddTaskQPSGauge|requests_per_tl|syncmatch_latency_per_tl_count|cadence_latency_per_tl_count" | sort'
  echo
done

