package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadPolicy_NoPath: the no-flag case must hand back DefaultPolicy
// untouched. Capture and tags both rely on this fallback — they
// always call loadPolicy(path) and only diverge based on path being
// "" vs not.
func TestLoadPolicy_NoPath(t *testing.T) {
	p, err := loadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Stable["controller"]; !ok {
		t.Errorf("default Stable should include 'controller'")
	}
}

// TestLoadPolicy_Override locks in the merge semantics: each present
// field in the JSON file REPLACES the corresponding default entirely
// (it doesn't union). This is the contract documented in
// docs/tagging.md and consumers depend on it. If a user writes
// `{"stable": ["tenant_id"]}` they want JUST tenant_id, not
// tenant_id-plus-all-the-defaults.
func TestLoadPolicy_Override(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.json")
	body := `{
	  "stable": ["tenant_id"],
	  "deny": ["traceparent"],
	  "vendor_map": {"x.y": "controller"},
	  "cardinality_promote_threshold": 50
	}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := loadPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Stable["tenant_id"]; !ok {
		t.Errorf("Stable should contain tenant_id")
	}
	if _, ok := p.Stable["controller"]; ok {
		t.Errorf("Stable override should REPLACE defaults, but 'controller' is still present")
	}
	if p.CardinalityPromoteThreshold != 50 {
		t.Errorf("threshold = %d, want 50", p.CardinalityPromoteThreshold)
	}
	if p.VendorMap["x.y"] != "controller" {
		t.Errorf("vendor_map override missing")
	}
}

// TestLoadPolicy_BadFile: a non-existent path produces an error, not
// a silent fallback. Silent fallback would let typo'd --policy-file
// args produce confusing default-classified output without warning.
func TestLoadPolicy_BadFile(t *testing.T) {
	_, err := loadPolicy("/nonexistent/path.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}
