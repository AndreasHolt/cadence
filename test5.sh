for p in cadence-matching-a-0 cadence-matching-b-0 cadence-matching-c-0; do
  echo "== $p =="
  kubectl exec -n cadence-kind-lab "$p" -- sh -c 'curl -fsS localhost:9090/metrics | grep "cadence_requests_per_tl" | grep "operation=\"AddDecisionTask\"" | grep -E "tasklist=\"0\"|tasklist=\"1\"" | sort'
  echo
done

