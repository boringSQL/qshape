package qshape

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type (
	// shapedQuery is one query's share of the parse work, computed once.
	shapedQuery struct {
		fingerprint string
		canonical   string
		err         error
	}
)

const (
	// Below this many queries the goroutine setup outweighs the parse work.
	parallelShapeMin = 8
)

// fingerprintAll fingerprints every query, in parallel, returning results in input
// order. Workers pull indexes off a shared counter because per-query cost varies by
// orders of magnitude within one capture. Concurrency relies on libpg_query's
// thread-local memory contexts (pg_query_go v6); re-check on a major bump.
func fingerprintAll(queries []Query) []shapedQuery {
	out := make([]shapedQuery, len(queries))

	workers := runtime.GOMAXPROCS(0)
	if len(queries) < parallelShapeMin || workers < 2 {
		for i := range queries {
			out[i].fingerprint, out[i].canonical, out[i].err = fingerprintNormalized(queries[i].Raw)
		}
		return out
	}
	if workers > len(queries) {
		workers = len(queries)
	}

	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(queries) {
					return
				}
				// each worker writes its own index; no shared state
				out[i].fingerprint, out[i].canonical, out[i].err = fingerprintNormalized(queries[i].Raw)
			}
		}()
	}
	wg.Wait()
	return out
}
