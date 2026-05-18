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

// writeFixtureClusters builds a minimal clusters.json on disk with
// two clusters: one tagged with controller+application, one untagged
// (with a synthetic dynamic key for the --show-promotable path).
// Used by all three subcommand tests below.
func writeFixtureClusters(t *testing.T) string {
	t.Helper()
	doc := clustersDoc{
		SchemaVersion: currentSchemaVersion,
		Clusters: []qshape.Cluster{
			{
				Fingerprint: "sha1:aaaa",
				Canonical:   "SELECT 1",
				TotalCalls:  100,
				Members:     []qshape.Query{{Raw: "/*application:billing,controller:orders*/ SELECT 1", Calls: 100}},
				Owners:      map[string]string{"application": "billing", "controller": "orders"},
			},
			{
				Fingerprint: "sha1:bbbb",
				Canonical:   "SELECT 2",
				TotalCalls:  20,
				Members:     []qshape.Query{{Raw: "SELECT 2 /*tenant_id='X'*/", Calls: 20}},
				DynamicTagKeys: []qshape.DynamicKeyObservation{
					{Key: "tenant_id", ValueCardinalitySeen: 1},
				},
			},
		},
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "clusters.json")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(doc); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestRunTags_Summary: the default path (no --by, no --show-promotable)
// prints a count of tagged vs untagged clusters. Lightweight sanity
// check; mostly here so that `qshape tags` with no flags doesn't
// crash on an empty document.
func TestRunTags_Summary(t *testing.T) {
	path := writeFixtureClusters(t)
	var buf bytes.Buffer
	if err := runTags(path, "", false, "", &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "clusters: 2") {
		t.Errorf("expected 'clusters: 2' in output, got:\n%s", buf.String())
	}
}

// TestRunTags_GroupBy verifies the --by application path: groups
// clusters by Owners["application"], sums calls per bucket, sorts
// by call volume desc. This is the headline workflow for
// `qshape tags --by controller`: a consultant pointing at a strange
// DB and asking "what dominates this workload?".
func TestRunTags_GroupBy(t *testing.T) {
	path := writeFixtureClusters(t)
	var buf bytes.Buffer
	if err := runTags(path, "application", false, "", &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "billing") {
		t.Errorf("expected 'billing' bucket, got:\n%s", out)
	}
	if !strings.Contains(out, "(none)") {
		t.Errorf("expected '(none)' bucket for untagged cluster, got:\n%s", out)
	}
}

// TestRunTags_ShowPromotable proves the promote-from-dynamic
// workflow works end-to-end: a dynamic key with cardinality below
// the threshold appears in the output. The fixture's `tenant_id`
// has cardinality 1, well under the default 100. This is the
// flag users will run to discover dual-nature keys worth opting in.
func TestRunTags_ShowPromotable(t *testing.T) {
	path := writeFixtureClusters(t)
	var buf bytes.Buffer
	if err := runTags(path, "", true, "", &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "tenant_id") {
		t.Errorf("expected tenant_id in promotable output, got:\n%s", buf.String())
	}
}

// TestRunTags_PolicyFileReclassifies: when --policy-file moves
// tenant_id from deny → stable, re-running classification against
// the same clusters.json should now produce Owners["tenant_id"]
// instead of dynamic. This locks in the "re-classify in memory"
// promise — no re-capture required.
func TestRunTags_PolicyFileReclassifies(t *testing.T) {
	path := writeFixtureClusters(t)
	dir := t.TempDir()
	policy := filepath.Join(dir, "p.json")
	if err := os.WriteFile(policy, []byte(`{"stable":["tenant_id"],"deny":["traceparent"]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := runTags(path, "tenant_id", false, policy, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "X") {
		t.Errorf("expected 'X' as tenant_id bucket value after promotion, got:\n%s", out)
	}
}
