package main

import (
	"encoding/json"
	"testing"

	"github.com/boringsql/qshape"
)

func TestAttributeCondAliasedEqual(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{
		"u": {Schema: "auth", Table: "user_account"},
	}
	attributeCond("(u.user_id = $1)", aliases, "auth", "user_account", ctx)

	a, ok := ctx.byPosition[1]
	if !ok {
		t.Fatal("expected param 1 attributed")
	}
	if a.Table != "user_account" || a.Column != "user_id" || a.Schema != "auth" {
		t.Errorf("wrong attribution: %+v", a)
	}
	if a.Confidence != "exact" {
		t.Errorf("expected exact confidence, got %s", a.Confidence)
	}
}

// PG emits bare column names in plan text when the scan is unambiguous
// (e.g. Filter on a single-table Index Scan). The plan node pins the
// column to its relation — that's exact attribution, not a guess.
func TestAttributeCondUnqualifiedOnScanIsExact(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{
		"session": {Schema: "auth", Table: "session"},
	}
	attributeCond("(id = $1)", aliases, "auth", "session", ctx)

	a, ok := ctx.byPosition[1]
	if !ok {
		t.Fatal("expected param 1 attributed")
	}
	if a.Table != "session" || a.Column != "id" || a.Schema != "auth" {
		t.Errorf("wrong attribution: %+v", a)
	}
	if a.Confidence != "exact" {
		t.Errorf("expected exact confidence for unqualified col on a scan node, got %s", a.Confidence)
	}
}

// A qualifier that doesn't resolve (outer-scope ref, schema-qualified name,
// subplan name) — attribute to the current relation as a best guess and
// flag it as heuristic.
func TestAttributeCondMismatchedQualifierIsHeuristic(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{}
	attributeCond("(outer_alias.id = $1)", aliases, "auth", "session", ctx)

	a, ok := ctx.byPosition[1]
	if !ok {
		t.Fatal("expected param 1 attributed")
	}
	if a.Confidence != "heuristic" {
		t.Errorf("expected heuristic for mismatched qualifier, got %s", a.Confidence)
	}
}

func TestAttributeCondMultipleParams(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{
		"t": {Schema: "auth", Table: "oauth_token"},
	}
	attributeCond("((t.access_sha = $2) AND (t.access_hash = hashtext($1)))", aliases, "auth", "oauth_token", ctx)

	if a, ok := ctx.byPosition[2]; !ok || a.Column != "access_sha" {
		t.Errorf("param 2 wrong: %+v ok=%v", a, ok)
	}
	// $1 is wrapped in hashtext(...) — no direct column comparison, so we
	// don't attribute it. That's fine: unattributed, not incorrect.
	if _, ok := ctx.byPosition[1]; ok {
		t.Logf("note: $1 got attributed even though wrapped in function — acceptable but brittle")
	}
}

// PG plans system views like pg_catalog.pg_settings as a Function Scan.
// The plan node has no "Relation Name" but carries the view name in
// "Alias". walkPlan must still attribute conds on that node.
func TestWalkPlanFunctionScanWithAliasOnly(t *testing.T) {
	// Index Scan on a function-backed view: Relation Name absent, Alias set.
	plan := json.RawMessage(`{
		"Node Type": "Function Scan",
		"Function Name": "pg_show_all_settings",
		"Alias": "pg_settings",
		"Filter": "(name = $1)"
	}`)
	c := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	walkPlan(plan, "", "", c)

	a, ok := c.byPosition[1]
	if !ok {
		t.Fatal("expected param 1 attributed via Alias fallback")
	}
	if a.Table != "pg_settings" || a.Column != "name" {
		t.Errorf("wrong attribution: %+v", a)
	}
	if a.Confidence != "exact" {
		t.Errorf("expected exact, got %s", a.Confidence)
	}
}

func TestAttributeCondPreservesExactOverHeuristic(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{
		"u": {Schema: "auth", Table: "user_account"},
	}
	// First hit: exact
	attributeCond("(u.user_id = $1)", aliases, "", "", ctx)
	// Second hit that would be heuristic on a different relation
	attributeCond("(user_id = $1)", map[string]tableRef{}, "public", "other_table", ctx)

	a := ctx.byPosition[1]
	if a.Table != "user_account" || a.Confidence != "exact" {
		t.Errorf("exact attribution overwritten by heuristic: %+v", a)
	}
}
