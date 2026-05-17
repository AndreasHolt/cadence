package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/sharddistributor/store/etcd/etcdkeys"
	"github.com/uber/cadence/service/sharddistributor/store/etcd/etcdtypes"
	executorstorecommon "github.com/uber/cadence/service/sharddistributor/store/etcd/executorstore/common"
)

type etcdGetResponse struct {
	Kvs []etcdKV `json:"kvs"`
}

type etcdKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type executorSummary struct {
	ExecutorID      string
	Host            string
	AssignedShards  int
	ReportedShards  int
	ReportedLoadSum float64
	ShardLoads      []shardLoadSummary
}

type shardLoadSummary struct {
	ShardID string
	Load    float64
}

func main() {
	var namespace string
	var prefix string

	flag.StringVar(&namespace, "namespace", "", "shard distributor namespace")
	flag.StringVar(&prefix, "prefix", "", "etcd executor store prefix")
	flag.Parse()

	if namespace == "" || prefix == "" {
		fmt.Fprintln(os.Stderr, "--namespace and --prefix are required")
		os.Exit(2)
	}

	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read stdin: %v\n", err)
		os.Exit(1)
	}

	var resp etcdGetResponse
	if err := json.Unmarshal(input, &resp); err != nil {
		fmt.Fprintf(os.Stderr, "parse etcdctl json: %v\n", err)
		os.Exit(1)
	}

	summaries := map[string]*executorSummary{}
	for _, kv := range resp.Kvs {
		keyBytes, err := base64.StdEncoding.DecodeString(kv.Key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "decode key: %v\n", err)
			os.Exit(1)
		}
		key := string(keyBytes)

		executorID, keyType, err := etcdkeys.ParseExecutorKey(prefix, namespace, key)
		if err != nil {
			continue
		}

		summary := summaries[executorID]
		if summary == nil {
			summary = &executorSummary{ExecutorID: executorID}
			summaries[executorID] = summary
		}

		valueBytes, err := base64.StdEncoding.DecodeString(kv.Value)
		if err != nil {
			fmt.Fprintf(os.Stderr, "decode value for %s: %v\n", key, err)
			os.Exit(1)
		}

		switch keyType {
		case etcdkeys.ExecutorAssignedStateKey:
			var assignedState etcdtypes.AssignedState
			if err := executorstorecommon.DecompressAndUnmarshal(valueBytes, &assignedState); err != nil {
				fmt.Fprintf(os.Stderr, "decode assigned_state for %s: %v\n", executorID, err)
				os.Exit(1)
			}
			summary.AssignedShards = len(assignedState.AssignedShards)

		case etcdkeys.ExecutorReportedShardsKey:
			var reported map[string]*types.ShardStatusReport
			if err := executorstorecommon.DecompressAndUnmarshal(valueBytes, &reported); err != nil {
				fmt.Fprintf(os.Stderr, "decode reported_shards for %s: %v\n", executorID, err)
				os.Exit(1)
			}
			summary.ReportedShards = len(reported)
			var loadSum float64
			shardLoads := make([]shardLoadSummary, 0, len(reported))
			for shardID, report := range reported {
				if report != nil {
					loadSum += report.ShardLoad
					shardLoads = append(shardLoads, shardLoadSummary{
						ShardID: shardID,
						Load:    report.ShardLoad,
					})
				}
			}
			summary.ReportedLoadSum = loadSum
			sort.Slice(shardLoads, func(i, j int) bool {
				if shardLoads[i].Load == shardLoads[j].Load {
					return shardLoads[i].ShardID < shardLoads[j].ShardID
				}
				return shardLoads[i].Load > shardLoads[j].Load
			})
			summary.ShardLoads = shardLoads

		case etcdkeys.ExecutorMetadataKey:
			metadataKey := strings.TrimPrefix(key, etcdkeys.BuildMetadataKey(prefix, namespace, executorID, ""))
			if metadataKey == "host" || metadataKey == "hostIP" {
				summary.Host = string(valueBytes)
			}
		}
	}

	ordered := make([]*executorSummary, 0, len(summaries))
	for _, summary := range summaries {
		ordered = append(ordered, summary)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Host == ordered[j].Host {
			return ordered[i].ExecutorID < ordered[j].ExecutorID
		}
		return ordered[i].Host < ordered[j].Host
	})

	fmt.Printf("%-40s %-24s %8s %8s %12s\n", "EXECUTOR_ID", "HOST", "ASSIGNED", "REPORTED", "LOAD_SUM")
	for _, summary := range ordered {
		fmt.Printf(
			"%-40s %-24s %8d %8d %12.3f\n",
			summary.ExecutorID,
			summary.Host,
			summary.AssignedShards,
			summary.ReportedShards,
			summary.ReportedLoadSum,
		)
		for _, shardLoad := range summary.ShardLoads {
			fmt.Printf("  %-38s -> %.3f\n", shardLoad.ShardID, shardLoad.Load)
		}
	}
}
