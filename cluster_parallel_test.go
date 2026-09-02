package qshape

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/boringsql/qshape/tags"
)

// groupSerialReference is the pre-parallel implementation, kept as the oracle:
// GroupWithPolicy must agree with it cluster for cluster, member for member.
func groupSerialReference(queries []Query, policy *tags.Policy) []Cluster {
	groups := make(map[string]*Cluster)
	var unparseable []Cluster

	for _, q := range queries {
		fp, err := Fingerprint(q.Raw)
		if err != nil {
			unparseable = append(unparseable, Cluster{
				Canonical:       q.Raw,
				Members:         []Query{q},
				TotalCalls:      q.Calls,
				TotalExecTimeMs: q.TotalExecTimeMs,
				Rows:            q.Rows,
			})
			continue
		}
		c, ok := groups[fp]
		if !ok {
			c = &Cluster{Fingerprint: fp}
			groups[fp] = c
		}
		c.Members = append(c.Members, q)
		c.TotalCalls += q.Calls
		c.TotalExecTimeMs += q.TotalExecTimeMs
		c.Rows += q.Rows
	}
	for _, c := range groups {
		c.TempBlksRead = sumTempBlocks(c.Members, func(q Query) *int64 { return q.TempBlksRead })
		c.TempBlksWritten = sumTempBlocks(c.Members, func(q Query) *int64 { return q.TempBlksWritten })
	}
	for i := range unparseable {
		unparseable[i].TempBlksRead = unparseable[i].Members[0].TempBlksRead
		unparseable[i].TempBlksWritten = unparseable[i].Members[0].TempBlksWritten
		if unparseable[i].TotalCalls > 0 {
			unparseable[i].MeanExecTimeMs = unparseable[i].TotalExecTimeMs / float64(unparseable[i].TotalCalls)
		}
	}
	if policy == nil {
		policy = tags.DefaultPolicy()
	}
	out := make([]Cluster, 0, len(groups)+len(unparseable))
	for _, c := range groups {
		if c.TotalCalls > 0 {
			c.MeanExecTimeMs = c.TotalExecTimeMs / float64(c.TotalCalls)
		}
		sortMembers(c)
		canonical, err := Normalize(c.Members[0].Raw)
		if err != nil {
			canonical = c.Members[0].Raw
		}
		c.Canonical = canonical
		applyTags(c, policy)
		out = append(out, *c)
	}
	out = append(out, unparseable...)

	hasTiming := false
	for _, c := range out {
		if c.TotalExecTimeMs > 0 {
			hasTiming = true
			break
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if hasTiming && out[i].TotalExecTimeMs != out[j].TotalExecTimeMs {
			return out[i].TotalExecTimeMs > out[j].TotalExecTimeMs
		}
		if out[i].TotalCalls != out[j].TotalCalls {
			return out[i].TotalCalls > out[j].TotalCalls
		}
		return out[i].Fingerprint < out[j].Fingerprint
	})
	return out
}

// corpusQueries builds a capture-shaped input: corpus statements repeated with fresh
// QueryIDs, plus literal variants (same cluster) and unparseable statements (must not
// cluster).
func corpusQueries(t *testing.T) []Query {
	t.Helper()
	stmts := corpusStatements(t)

	var out []Query
	id := int64(0)
	for i, s := range stmts {
		for r := 0; r < 2; r++ {
			id++
			out = append(out, Query{
				Raw:             s,
				QueryID:         id,
				Calls:           int64(10 + i%7),
				TotalExecTimeMs: float64((i*13+r*29)%97) / 3,
				Rows:            int64(i % 11),
			})
		}
	}
	// literal variants of one shape: same cluster, different raw text
	for i, lit := range []string{"1", "42", "7"} {
		id++
		out = append(out, Query{
			Raw:             fmt.Sprintf("SELECT id FROM users WHERE id = %s", lit),
			QueryID:         id,
			Calls:           int64(3 + i),
			TotalExecTimeMs: float64(i),
		})
	}
	// unparseable: singleton clusters with an empty fingerprint
	for i, bad := range []string{"NOT SQL AT ALL", "SELECT FROM WHERE", "((("} {
		id++
		out = append(out, Query{Raw: bad, QueryID: id, Calls: int64(2 + i), TotalExecTimeMs: float64(i) / 2})
	}
	return out
}

func TestGroupMatchesSerialReference(t *testing.T) {
	queries := corpusQueries(t)
	if len(queries) < 2*len(prettifySeeds) {
		t.Fatalf("corpus too small to be meaningful: %d queries", len(queries))
	}

	got, err := GroupWithPolicy(queries, tags.DefaultPolicy())
	if err != nil {
		t.Fatalf("GroupWithPolicy: %v", err)
	}
	want := groupSerialReference(queries, tags.DefaultPolicy())

	if len(got) != len(want) {
		t.Fatalf("cluster count: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("cluster %d differs from the serial reference:\n got=%+v\nwant=%+v", i, got[i], want[i])
		}
	}
	// Canonical is the field the fast path caches; check it is actually normalized
	// and not silently left as raw SQL.
	normalized := 0
	for _, c := range got {
		if c.Fingerprint != "" && c.Canonical != "" && c.Canonical != c.Members[0].Raw {
			normalized++
		}
	}
	if normalized == 0 {
		t.Error("no cluster carries a normalized Canonical; the cache is returning raw SQL")
	}
}

// The parallel stage must not leak into the output: same input, same bytes, every run.
func TestGroupIsDeterministic(t *testing.T) {
	queries := corpusQueries(t)

	first, err := GroupWithPolicy(queries, tags.DefaultPolicy())
	if err != nil {
		t.Fatalf("GroupWithPolicy: %v", err)
	}
	baseline, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 5; run++ {
		got, err := GroupWithPolicy(queries, tags.DefaultPolicy())
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != string(baseline) {
			t.Fatalf("run %d differs from the first run; grouping is not deterministic", run)
		}
	}
}

// Members are appended in input order and only then sorted, so feed them shuffled.
func TestGroupPreservesMembers(t *testing.T) {
	const stmt = "SELECT id FROM orders WHERE id = $1"
	shuffled := []int64{41, 7, 63, 2, 19, 55, 30, 11, 48, 26, 3, 60}
	var queries []Query
	for _, id := range shuffled {
		queries = append(queries, Query{Raw: stmt, QueryID: id, Calls: 1})
	}
	got, err := GroupWithPolicy(queries, tags.DefaultPolicy())
	if err != nil {
		t.Fatalf("GroupWithPolicy: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one cluster, got %d", len(got))
	}
	if len(got[0].Members) != len(queries) {
		t.Fatalf("member count: got=%d want=%d", len(got[0].Members), len(queries))
	}
	want := append([]int64(nil), shuffled...)
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	for i, m := range got[0].Members {
		if m.QueryID != want[i] {
			t.Fatalf("member %d: QueryID=%d want=%d (members must end sorted by QueryID)", i, m.QueryID, want[i])
		}
	}
}

// fingerprintAll has a serial path below parallelShapeMin; both must agree.
func TestFingerprintAllMatchesSerial(t *testing.T) {
	stmts := corpusStatements(t)
	if len(stmts) < parallelShapeMin {
		t.Skip("corpus smaller than the parallel threshold")
	}
	var queries []Query
	for _, s := range stmts {
		queries = append(queries, Query{Raw: s})
	}

	parallel := fingerprintAll(queries)
	if len(parallel) != len(queries) {
		t.Fatalf("fingerprintAll returned %d results for %d queries", len(parallel), len(queries))
	}
	for i, q := range queries {
		fp, canonical, err := fingerprintNormalized(q.Raw)
		if fp != parallel[i].fingerprint || canonical != parallel[i].canonical {
			t.Fatalf("query %d: serial and parallel disagree\n serial=%q/%q\nparallel=%q/%q",
				i, fp, canonical, parallel[i].fingerprint, parallel[i].canonical)
		}
		if (err == nil) != (parallel[i].err == nil) {
			t.Fatalf("query %d: error disagreement serial=%v parallel=%v", i, err, parallel[i].err)
		}
	}
}

// Both sides of parallelShapeMin, and a single-core runner, must produce the same shapes.
func TestFingerprintAllAcrossThresholdAndGOMAXPROCS(t *testing.T) {
	stmts := corpusStatements(t)
	if len(stmts) < parallelShapeMin+2 {
		t.Skip("corpus too small to straddle the threshold")
	}
	var queries []Query
	for _, s := range stmts[:parallelShapeMin+2] {
		queries = append(queries, Query{Raw: s})
	}

	want := make([]shapedQuery, len(queries))
	for i, q := range queries {
		want[i].fingerprint, want[i].canonical, want[i].err = fingerprintNormalized(q.Raw)
	}

	restore := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(restore) })

	for _, procs := range []int{1, 2, 3, len(queries), len(queries) + 4} {
		runtime.GOMAXPROCS(procs)
		for n := 0; n <= len(queries); n++ {
			got := fingerprintAll(queries[:n])
			if len(got) != n {
				t.Fatalf("GOMAXPROCS=%d n=%d: got %d results", procs, n, len(got))
			}
			for i := 0; i < n; i++ {
				if got[i].fingerprint != want[i].fingerprint || got[i].canonical != want[i].canonical {
					t.Fatalf("GOMAXPROCS=%d n=%d index %d: got=%q/%q want=%q/%q",
						procs, n, i, got[i].fingerprint, got[i].canonical, want[i].fingerprint, want[i].canonical)
				}
			}
		}
	}
}

// The canonical fingerprintNormalized hands back must be the text the fingerprint was
// computed from, and an unparseable query must return no canonical.
func TestFingerprintNormalizedReturnsItsOwnInput(t *testing.T) {
	for _, s := range corpusStatements(t) {
		fp, canonical, err := fingerprintNormalized(s)
		if err != nil {
			if fp != "" || canonical != "" {
				t.Fatalf("error case for %.60q returned fp=%q canonical=%q, want both empty", s, fp, canonical)
			}
			continue
		}
		want, nerr := Normalize(s)
		if nerr != nil {
			want = s
		}
		if canonical != want {
			t.Fatalf("canonical for %.60q:\n got=%q\nwant=%q", s, canonical, want)
		}
		// the fingerprint must be the one for THAT text, not for the raw SQL
		again, err := Fingerprint(canonical)
		if err != nil {
			t.Fatalf("refingerprinting the canonical of %.60q failed: %v", s, err)
		}
		if again != fp {
			t.Fatalf("fingerprint for %.60q is not the canonical's own: got=%q want=%q", s, fp, again)
		}
	}
}

func BenchmarkGroupWithPolicy(b *testing.B) {
	// A capture-shaped batch: parse cost tracks SQL length, so width is what matters.
	for _, width := range []struct {
		name string
		cols int
	}{{"narrow", 8}, {"wide", 120}} {
		var queries []Query
		for i := 0; i < 500; i++ {
			var sb strings.Builder
			sb.WriteString("SELECT ")
			for c := 0; c < width.cols; c++ {
				if c > 0 {
					sb.WriteString(", ")
				}
				fmt.Fprintf(&sb, "t.col_%d AS a_%d", c, c)
			}
			fmt.Fprintf(&sb, " FROM tbl_%d t JOIN other o ON o.id = t.fk WHERE t.tenant = $1 AND t.k = $2", i%50)
			queries = append(queries, Query{Raw: sb.String(), QueryID: int64(i), Calls: 10, TotalExecTimeMs: float64(i)})
		}
		size := 0
		for _, q := range queries {
			size += len(q.Raw)
		}
		b.Run(fmt.Sprintf("%s/%dB", width.name, size/len(queries)), func(b *testing.B) {
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				if _, err := GroupWithPolicy(queries, tags.DefaultPolicy()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
