package qshape

import "testing"

func TestGroupAggregatesCalls(t *testing.T) {
	in := []Query{
		{Raw: "SELECT id FROM users WHERE id = 1", Calls: 100},
		{Raw: "SELECT id FROM users WHERE id = 99", Calls: 200},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(out))
	}
	if out[0].TotalCalls != 300 {
		t.Errorf("TotalCalls = %d, want 300", out[0].TotalCalls)
	}
	if len(out[0].Members) != 2 {
		t.Errorf("Members len = %d, want 2", len(out[0].Members))
	}
}

func TestGroupAggregatesTiming(t *testing.T) {
	in := []Query{
		{Raw: "SELECT id FROM users WHERE id = 1", Calls: 100, TotalExecTimeMs: 250.0, Rows: 100},
		{Raw: "SELECT id FROM users WHERE id = 99", Calls: 400, TotalExecTimeMs: 750.0, Rows: 400},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(out))
	}
	if out[0].TotalExecTimeMs != 1000.0 {
		t.Errorf("TotalExecTimeMs = %v, want 1000.0", out[0].TotalExecTimeMs)
	}
	if out[0].Rows != 500 {
		t.Errorf("Rows = %d, want 500", out[0].Rows)
	}
	wantMean := 1000.0 / 500.0
	if out[0].MeanExecTimeMs != wantMean {
		t.Errorf("MeanExecTimeMs = %v, want %v", out[0].MeanExecTimeMs, wantMean)
	}
}

func TestGroupSortsByTimingWhenPresent(t *testing.T) {
	in := []Query{
		{Raw: "SELECT id FROM users", Calls: 1000, TotalExecTimeMs: 50.0},
		{Raw: "SELECT name FROM users", Calls: 10, TotalExecTimeMs: 5000.0},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(out))
	}
	if out[0].TotalExecTimeMs < out[1].TotalExecTimeMs {
		t.Errorf("expected sort by TotalExecTimeMs desc, got %v then %v",
			out[0].TotalExecTimeMs, out[1].TotalExecTimeMs)
	}
}

func TestGroupOrdering(t *testing.T) {
	in := []Query{
		{Raw: "SELECT name FROM users", Calls: 10},
		{Raw: "SELECT id FROM users", Calls: 500},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 clusters, got %d", len(out))
	}
	if out[0].TotalCalls < out[1].TotalCalls {
		t.Errorf("clusters not sorted by TotalCalls desc: %d then %d",
			out[0].TotalCalls, out[1].TotalCalls)
	}
}

// Documents MVP behavior: alias stripping is deferred, so ORM variants
// that differ only in aliases currently produce more than one cluster.
// When Phase 0.x lands alias-strip, tighten this to `== 1`.
func TestGroupORMVariantsCurrentBehavior(t *testing.T) {
	in := []Query{
		{Raw: "SELECT id, name FROM users WHERE id = $1", Calls: 1},
		{Raw: "SELECT u.id, u.name FROM users u WHERE u.id = $1", Calls: 1},
		{Raw: "SELECT id, name FROM users WHERE id = $1 LIMIT $2", Calls: 1},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 2 {
		t.Errorf("expected >= 2 clusters in MVP (alias strip deferred), got %d", len(out))
	}
	total := int64(0)
	for _, c := range out {
		total += c.TotalCalls
	}
	if total != 3 {
		t.Errorf("total calls across clusters = %d, want 3", total)
	}
}

func TestGroupUnparseable(t *testing.T) {
	in := []Query{
		{Raw: "SELECT FROM WHERE", Calls: 5},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(out))
	}
	if out[0].Fingerprint != "" {
		t.Errorf("unparseable cluster should have empty fingerprint, got %q", out[0].Fingerprint)
	}
	if out[0].Canonical != "SELECT FROM WHERE" {
		t.Errorf("unparseable Canonical should be raw, got %q", out[0].Canonical)
	}
}

func TestGroupEmpty(t *testing.T) {
	out, err := Group(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty slice, got %d clusters", len(out))
	}
}
