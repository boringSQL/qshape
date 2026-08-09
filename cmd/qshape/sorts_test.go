package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boringsql/qshape"
)

// clustersFile writes a clusters.json the loader will accept.
func clustersFile(t *testing.T, clusters []qshape.Cluster) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "clusters.json")
	doc := clustersDoc{SchemaVersion: currentSchemaVersion, Clusters: clusters}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// cluster builds one member of a sort family: same statement, different ORDER BY.
func cluster(sql string, calls int64, ms float64) qshape.Cluster {
	return qshape.Cluster{
		Fingerprint:     sql,
		Canonical:       sql,
		TotalCalls:      calls,
		TotalExecTimeMs: ms,
	}
}

const (
	base      = "SELECT id, name FROM public.task WHERE workspace_id = $1"
	byID      = base + " ORDER BY id ASC"
	byName    = base + " ORDER BY name ASC, id ASC"
	byDate    = base + " ORDER BY created_at DESC, id ASC"
	otherStmt = "SELECT id FROM public.project WHERE workspace_id = $1 ORDER BY id ASC"
)

// The command exists to answer "which ordering actually costs me". The cheap
// ordering having the most calls is the case that misleads a call-count read,
// and is exactly the shape that cost a wrong optimization target in the field.
func TestSortsRanksOrderingsByCostNotCalls(t *testing.T) {
	in := clustersFile(t, []qshape.Cluster{
		cluster(byName, 900_000, 500),  // most calls, cheapest
		cluster(byID, 100_000, 14_000), // fewest calls, dominant cost
		cluster(byDate, 10_000, 30),
	})

	var out bytes.Buffer
	if err := runSorts(&out, in, 10, 2, false, 72); err != nil {
		t.Fatalf("runSorts: %v", err)
	}
	got := out.String()

	if !strings.Contains(got, "3 distinct orderings") {
		t.Errorf("did not group the three sorts into one statement:\n%s", got)
	}
	iID, iName := strings.Index(got, "id ASC"), strings.Index(got, "name ASC")
	if iID < 0 || iName < 0 {
		t.Fatalf("orderings missing from output:\n%s", got)
	}
	if iID > iName {
		t.Errorf("ranked by calls, not cost: the 14s ordering must come before the 0.5s one:\n%s", got)
	}
}

// A statement with a single ordering has nothing to choose between, so it is
// noise by default — but --min-sorts 1 must still show it.
func TestSortsMinSortsFilter(t *testing.T) {
	in := clustersFile(t, []qshape.Cluster{cluster(otherStmt, 10, 5)})

	var out bytes.Buffer
	if err := runSorts(&out, in, 10, 2, false, 72); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "no statement has 2 or more distinct orderings") {
		t.Errorf("single-ordering statement should be filtered out by default:\n%s", out.String())
	}

	out.Reset()
	if err := runSorts(&out, in, 10, 1, false, 72); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "distinct ordering") {
		t.Errorf("--min-sorts 1 should show it:\n%s", out.String())
	}
}

// Separate statements must not be merged just because they share a sort key.
func TestSortsKeepsDistinctStatementsApart(t *testing.T) {
	in := clustersFile(t, []qshape.Cluster{
		cluster(byID, 100, 100), cluster(byName, 100, 100),
		cluster(otherStmt, 100, 100),
	})

	var out bytes.Buffer
	if err := runSorts(&out, in, 10, 1, false, 72); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(out.String(), "distinct ordering"); n != 2 {
		t.Errorf("expected 2 statements reported, got %d:\n%s", n, out.String())
	}
}

func TestSortsJSONIsMachineReadable(t *testing.T) {
	in := clustersFile(t, []qshape.Cluster{
		cluster(byID, 10, 900), cluster(byName, 10, 100),
		cluster(otherStmt, 10, 5), // single ordering: filtered from text, not JSON
	})

	var out bytes.Buffer
	if err := runSorts(&out, in, 10, 2, true, 72); err != nil {
		t.Fatal(err)
	}
	var profiles []qshape.SortProfile
	if err := json.Unmarshal(out.Bytes(), &profiles); err != nil {
		t.Fatalf("--json did not emit valid SortProfile JSON: %v\n%s", err, out.String())
	}
	// --json is the lossless form: --min-sorts 2 must not drop the
	// single-ordering statement the text report hides.
	if len(profiles) != 2 {
		t.Fatalf("--json must ignore --min-sorts, got %d profiles: %+v", len(profiles), profiles)
	}
	if profiles[0].DistinctSorts != 2 {
		t.Errorf("unexpected first profile: %+v", profiles[0])
	}
}

// Two orderings that differ only past the truncation width render as identical
// rows; the row must carry a fingerprint tag so they stay distinguishable.
func TestSortsTagsTruncatedAmbiguousVariants(t *testing.T) {
	byLong1 := cluster(base+" ORDER BY col_a, col_b, col_c, col_d, col_e, col_f ASC", 10, 900)
	byLong1.Fingerprint = "sha1:11111111aaaa"
	byLong2 := cluster(base+" ORDER BY col_a, col_b, col_c, col_d, col_e, col_g ASC", 10, 100)
	byLong2.Fingerprint = "sha1:22222222bbbb"
	in := clustersFile(t, []qshape.Cluster{byLong1, byLong2})

	var out bytes.Buffer
	if err := runSorts(&out, in, 10, 2, false, 20); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, "sha1:11111111") || !strings.Contains(got, "sha1:22222222") {
		t.Errorf("truncated ambiguous variants must carry a fingerprint tag:\n%s", got)
	}
}

func TestTruncate(t *testing.T) {
	for name, tc := range map[string]struct {
		in, want string
		n        int
		elided   bool
	}{
		"short is untouched":   {in: "id ASC", want: "id ASC", n: 20},
		"long is elided":       {in: "aaaaaaaaaaaaaaaaaaaa", want: "aaaaaaa...", n: 10, elided: true},
		"whitespace collapses": {in: "id\n  ASC", want: "id ASC", n: 20},
		// 11 runes but 22 bytes: a byte-wise cut would split a rune and
		// emit invalid UTF-8.
		"runes not bytes": {in: "ααααααααααα", want: "ααααααα...", n: 10, elided: true},
	} {
		t.Run(name, func(t *testing.T) {
			got, elided := truncate(tc.in, tc.n)
			if got != tc.want || elided != tc.elided {
				t.Errorf("truncate(%q, %d) = (%q, %v), want (%q, %v)",
					tc.in, tc.n, got, elided, tc.want, tc.elided)
			}
		})
	}
}
