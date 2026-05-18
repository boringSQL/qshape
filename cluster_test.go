package qshape

import (
	"encoding/json"
	"testing"
)

var (
	jsonMarshal   = json.Marshal
	jsonUnmarshal = json.Unmarshal
)

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

// Alias-only variants collapse (reshape strips decorative aliases);
// the LIMIT variant stays in its own cluster because LIMIT changes plan
// shape and LIMIT subsumption is intentionally out of scope.
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
	if len(out) != 2 {
		t.Errorf("expected 2 clusters (alias variants collapse, LIMIT stays separate), got %d", len(out))
	}
	total := int64(0)
	for _, c := range out {
		total += c.TotalCalls
	}
	if total != 3 {
		t.Errorf("total calls across clusters = %d, want 3", total)
	}
}

// Safe ORM variants — alias-only, optional AS, AND-predicate reorder —
// must collapse to a single canonical fingerprint.
func TestGroupORMVariantsCollapse(t *testing.T) {
	in := []Query{
		{Raw: "SELECT id, name FROM users WHERE id = $1 AND status = $2", Calls: 1},
		{Raw: "SELECT id, name FROM users WHERE status = $2 AND id = $1", Calls: 1},
		{Raw: "SELECT u.id, u.name FROM users u WHERE u.id = $1 AND u.status = $2", Calls: 1},
		{Raw: "SELECT u.id, u.name FROM users AS u WHERE u.status = $2 AND u.id = $1", Calls: 1},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 cluster, got %d: %+v", len(out), out)
	}
	if out[0].TotalCalls != int64(len(in)) {
		t.Errorf("TotalCalls = %d, want %d", out[0].TotalCalls, len(in))
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

// TestGroupPopulatesTagsFromFirstMember verifies the integration glue
// in Group(): the first member's raw SQL is the one fed through
// tags.Extract+Classify, and the result lands on the three new cluster
// fields. This is the "Tier 1 lottery-winner" attribution path
// — pg_stat_statements has already chosen a single SQL per bucket, so
// member[0] is the only sample we'll ever see.
func TestGroupPopulatesTagsFromFirstMember(t *testing.T) {
	in := []Query{
		{Raw: "/*application:billing,controller:orders*/ SELECT id FROM users WHERE id = 1", Calls: 50},
		{Raw: "/*application:billing,controller:orders*/ SELECT id FROM users WHERE id = 999", Calls: 50},
	}
	out, err := Group(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(out))
	}
	if out[0].Owners["application"] != "billing" {
		t.Errorf("Owners[application] = %q, want billing", out[0].Owners["application"])
	}
	if out[0].Owners["controller"] != "orders" {
		t.Errorf("Owners[controller] = %q, want orders", out[0].Owners["controller"])
	}
}

// TestGroupTagsOmittedForUntagged: when SQL has no tags, the three
// new fields must remain nil so the JSON encoder omits them via
// `omitempty`. Otherwise every untagged cluster bloats clusters.json
// with empty objects — annoying for diffs and pointless on the wire.
func TestGroupTagsOmittedForUntagged(t *testing.T) {
	in := []Query{{Raw: "SELECT 1 FROM users", Calls: 1}}
	out, _ := Group(in)
	if len(out) != 1 {
		t.Fatalf("expected 1 cluster")
	}
	if out[0].Owners != nil || out[0].RegresqlMeta != nil || out[0].DynamicTagKeys != nil {
		t.Errorf("untagged cluster should have nil tag fields, got %+v", out[0])
	}
}

// TestGroupDynamicTagKeysSorted locks in determinism for
// DynamicTagKeys — emitted JSON must be byte-stable across runs
// regardless of map iteration order. Without sorting, two captures
// of the same DB could diff on a clusters.json that ought to be
// identical.
func TestGroupDynamicTagKeysSorted(t *testing.T) {
	// sqlcommenter with multiple dynamic keys forces the sort path
	in := []Query{{
		Raw:   "SELECT 1 /*traceparent='abc',span_id='def',request_id='xyz'*/",
		Calls: 1,
	}}
	out, _ := Group(in)
	keys := out[0].DynamicTagKeys
	for i := 1; i < len(keys); i++ {
		if keys[i-1].Key > keys[i].Key {
			t.Errorf("DynamicTagKeys not sorted: %+v", keys)
		}
	}
}

// TestClusterRoundTripJSON is the cross-language contract guard.
// dryrun-rs's qshape_bridge consumes clusters.json from Rust; the
// only way that's safe long-term is if the wire format is byte-stable.
// Marshal → unmarshal → marshal must return the same bytes. If a
// field gets reordered or a new non-omitempty field sneaks in, this
// test catches it before it breaks a downstream consumer.
func TestClusterRoundTripJSON(t *testing.T) {
	in := []Query{{
		Raw:   "/*application:billing*/ SELECT 1 FROM users WHERE id = 1",
		Calls: 10,
	}}
	clusters, _ := Group(in)
	first, err := jsonMarshal(clusters)
	if err != nil {
		t.Fatal(err)
	}
	var roundTripped []Cluster
	if err := jsonUnmarshal(first, &roundTripped); err != nil {
		t.Fatal(err)
	}
	second, err := jsonMarshal(roundTripped)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Errorf("round-trip mismatch:\nfirst:  %s\nsecond: %s", first, second)
	}
}
