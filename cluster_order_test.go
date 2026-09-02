package qshape

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/boringsql/qshape/tags"
)

// Unparseable clusters all carry Fingerprint "", so tied unparseable singletons used to
// come out in unstable order; members differ by QueryID, so the permutation changed the
// marshalled bytes. Reproducing it needs parseable clusters too: they range out of a map
// in randomized order, and it is that shifting layout around the tie that permutes it.
func TestGroupOrdersTiedUnparseableClusters(t *testing.T) {
	var queries []Query
	id := int64(0)
	for i := 0; i < 40; i++ {
		id++
		queries = append(queries, Query{
			Raw:             fmt.Sprintf("SELECT c FROM t_%d WHERE k = $1", i),
			QueryID:         id,
			Calls:           5,
			TotalExecTimeMs: 12.5,
		})
	}
	// tied unparseable singletons: identical calls and timing, differing only by QueryID
	for round := 0; round < 6; round++ {
		for _, raw := range []string{
			"SELECT (ARRAY[$1, $2])[$3] FROM t", // normalizes to text that will not re-parse
			"NOT SQL AT ALL",
			"SELECT FROM WHERE",
			"(((",
		} {
			id++
			queries = append(queries, Query{Raw: raw, QueryID: id, Calls: 5, TotalExecTimeMs: 12.5})
		}
	}

	first, err := GroupWithPolicy(queries, tags.DefaultPolicy())
	if err != nil {
		t.Fatalf("GroupWithPolicy: %v", err)
	}
	unparseable := 0
	for _, c := range first {
		if c.Fingerprint == "" {
			unparseable++
		}
	}
	if unparseable < 12 {
		t.Fatalf("expected the unparseable seeds to tie as singletons, got %d such clusters "+
			"(a libpg_query change may have closed the deparse gap one of them relies on)", unparseable)
	}
	baseline, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 50; run++ {
		got, err := GroupWithPolicy(queries, tags.DefaultPolicy())
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != string(baseline) {
			t.Fatalf("run %d permuted tied clusters; the sort key is not a total order", run)
		}
	}
}
