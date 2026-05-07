// Package optimal provides a fast local-search approximation for the optimal
// shard-to-executor assignment problem. It minimises the L2-norm (sum of squared
// executor loads) using best-fit-decreasing initialisation followed by iterative
// improvement via single moves and pairwise swaps.
package optimal

import (
	"cmp"
	"math"
	"math/rand"
	"slices"
)

// Assignment maps executor ID -> list of shard IDs.
type Assignment map[string][]string

// Solve finds a near-optimal assignment of shards to executors that minimises the
// sum of squared executor loads (L2 norm). It uses best-fit-decreasing for a strong
// initial solution and then refines it with a fast local-search loop.
//
// shardLoads  : map[shardID] -> reported/raw load
// executorIDs : list of active executor identifiers
// start       : current assignment (may be nil / empty)
func Solve(shardLoads map[string]float64, executorIDs []string, start Assignment) Assignment {
	if len(executorIDs) == 0 {
		return Assignment{}
	}

	// ---------- helpers ----------
	executorLoad := func(a Assignment) map[string]float64 {
		loads := make(map[string]float64, len(executorIDs))
		for _, e := range executorIDs {
			loads[e] = 0
		}
		for e, shards := range a {
			for _, s := range shards {
				loads[e] += shardLoads[s]
			}
		}
		return loads
	}

	objective := func(loads map[string]float64) float64 {
		var sum float64
		for _, l := range loads {
			sum += l * l
		}
		return sum
	}

	clone := func(a Assignment) Assignment {
		c := make(Assignment, len(a))
		for e, shards := range a {
			cp := make([]string, len(shards))
			copy(cp, shards)
			c[e] = cp
		}
		return c
	}

	// ---------- deterministic BFD initial solution ----------
	var current Assignment
	if len(start) > 0 {
		current = clone(start)
	} else {
		current = make(Assignment)
		for _, e := range executorIDs {
			current[e] = nil
		}
		// Sort shards descending by load.
		type shard struct {
			id   string
			load float64
		}
		shards := make([]shard, 0, len(shardLoads))
		for id, load := range shardLoads {
			shards = append(shards, shard{id: id, load: load})
		}
		slices.SortFunc(shards, func(a, b shard) int {
			if a.load > b.load {
				return -1
			}
			if a.load < b.load {
				return 1
			}
			return cmp.Compare(a.id, b.id)
		})

		loads := executorLoad(current)
		for _, s := range shards {
			best := executorIDs[0]
			bestLoad := loads[best]
			for _, e := range executorIDs[1:] {
				if loads[e] < bestLoad || (loads[e] == bestLoad && e < best) {
					best = e
					bestLoad = loads[e]
				}
			}
			current[best] = append(current[best], s.id)
			loads[best] += s.load
		}
	}

	best := clone(current)
	bestObj := objective(executorLoad(best))

	// ---------- local search (moves + swaps) ----------
	localSearch := func(a Assignment) (Assignment, float64) {
		improved := true
		for improved {
			improved = false
			loads := executorLoad(a)

			// single moves
		outerMove:
			for src, shards := range a {
				for i, sid := range shards {
					sl := shardLoads[sid]
					for _, dst := range executorIDs {
						if src == dst {
							continue
						}
						// Delta of moving sl from src to dst on objective.
						// new_obj - old_obj = 2*sl*(dstLoad - srcLoad) + 2*sl*sl
						delta := 2*sl*(loads[dst]-loads[src]) + 2*sl*sl
						if delta < -1e-12 {
							a[src] = append(shards[:i], shards[i+1:]...)
							a[dst] = append(a[dst], sid)
							loads[src] -= sl
							loads[dst] += sl
							shards = a[src]
							improved = true
							break outerMove
						}
					}
				}
			}
			if improved {
				continue
			}

			// pairwise swaps
		outerSwap:
			for e1, s1s := range a {
				for i1, sid1 := range s1s {
					l1 := shardLoads[sid1]
					for e2, s2s := range a {
						if e1 >= e2 {
							continue
						}
						for i2, sid2 := range s2s {
							l2 := shardLoads[sid2]
							diff := l2 - l1
							// Delta of swapping l1 on e1 with l2 on e2.
							// new_e1 = e1 - l1 + l2, new_e2 = e2 - l2 + l1
							// delta = (new_e1^2 + new_e2^2) - (e1^2 + e2^2)
							//       = 2*diff^2 + 2*(e1-e2)*diff
							delta := 2*diff*diff + 2*(loads[e1]-loads[e2])*diff
							if delta < -1e-12 {
								a[e1][i1] = sid2
								a[e2][i2] = sid1
								loads[e1] += diff
								loads[e2] -= diff
								improved = true
								break outerSwap
							}
						}
					}
				}
			}
		}
		obj := objective(executorLoad(a))
		return clone(a), obj
	}

	best, bestObj = localSearch(best)

	// ---------- random restarts (5 iterations) ----------
	for r := 0; r < 5; r++ {
		shuffled := make([]string, 0, len(shardLoads))
		for id := range shardLoads {
			shuffled = append(shuffled, id)
		}
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

		restart := make(Assignment)
		for _, e := range executorIDs {
			restart[e] = nil
		}
		loads := executorLoad(restart)
		for _, sid := range shuffled {
			bestE := executorIDs[0]
			bestLoad := loads[bestE]
			for _, e := range executorIDs[1:] {
				if loads[e] < bestLoad || (loads[e] == bestLoad && e < bestE) {
					bestE = e
					bestLoad = loads[e]
				}
			}
			restart[bestE] = append(restart[bestE], sid)
			loads[bestE] += shardLoads[sid]
		}

		restart, obj := localSearch(restart)
		if obj < bestObj {
			best = restart
			bestObj = obj
		}
	}

	return best
}

// ComputeMetrics returns imbalance statistics for a given assignment.
type Metrics struct {
	L2Norm      float64 // sum of squared loads (objective minimised by Solve)
	MeanLoad    float64
	MaxLoad     float64
	MinLoad     float64
	StdDev      float64
	CV          float64 // coefficient of variation = stddev / mean
	MaxOverMean float64
	MinOverMean float64
}

func ComputeMetrics(a Assignment, shardLoads map[string]float64) Metrics {
	loads := make(map[string]float64, len(a))
	var total float64
	for e, shards := range a {
		for _, s := range shards {
			loads[e] += shardLoads[s]
		}
	}
	for _, l := range loads {
		total += l
	}
	count := float64(len(loads))
	mean := total / count

	var maxL, minL float64
	first := true
	for _, l := range loads {
		if first || l > maxL {
			maxL = l
		}
		if first || l < minL {
			minL = l
		}
		first = false
	}

	var variance, l2 float64
	for _, l := range loads {
		d := l - mean
		variance += d * d
		l2 += l * l
	}
	stddev := math.Sqrt(variance / count)
	cv := 0.0
	if mean > 0 {
		cv = stddev / mean
	}

	m := Metrics{
		L2Norm:   l2,
		MeanLoad: mean,
		MaxLoad:  maxL,
		MinLoad:  minL,
		StdDev:   stddev,
		CV:       cv,
	}
	if mean > 0 {
		m.MaxOverMean = maxL / mean
		m.MinOverMean = minL / mean
	}
	return m
}

// DiffAssignments computes how many shards would need to move to turn
// 'from' into 'to'.
func DiffAssignments(from, to Assignment) int {
	posFrom := make(map[string]string) // shard -> executor
	for e, shards := range from {
		for _, s := range shards {
			posFrom[s] = e
		}
	}
	moves := 0
	for e, shards := range to {
		for _, s := range shards {
			if posFrom[s] != e {
				moves++
			}
		}
	}
	return moves
}
