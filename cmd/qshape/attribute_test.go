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
	attributeCond("(u.user_id = $1)", aliases, "auth", "user_account", "exact", ctx)

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
	attributeCond("(id = $1)", aliases, "auth", "session", "exact", ctx)

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
	attributeCond("(outer_alias.id = $1)", aliases, "auth", "session", "exact", ctx)

	a, ok := ctx.byPosition[1]
	if !ok {
		t.Fatal("expected param 1 attributed")
	}
	if a.Confidence != "heuristic" {
		t.Errorf("expected heuristic for mismatched qualifier, got %s", a.Confidence)
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
	walkPlan(plan, c)

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

// A CTE or Subquery Scan alias is not a table statistics exist for, so an
// unqualified column there is a guess, not an exact attribution.
func TestWalkPlanCTEScanIsHeuristic(t *testing.T) {
	plan := json.RawMessage(`{
		"Node Type": "CTE Scan",
		"CTE Name": "recent",
		"Alias": "recent",
		"Filter": "(account_id = $1)"
	}`)
	c := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	walkPlan(plan, c)

	a, ok := c.byPosition[1]
	if !ok {
		t.Fatal("expected param 1 attributed")
	}
	if a.Confidence != "heuristic" {
		t.Errorf("expected heuristic for CTE Scan alias, got %s", a.Confidence)
	}
}

func TestAttributeCondPreservesExactOverHeuristic(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{
		"u": {Schema: "auth", Table: "user_account"},
	}
	// First hit: exact
	attributeCond("(u.user_id = $1)", aliases, "", "", "exact", ctx)
	// Second hit that would be heuristic on a different relation
	attributeCond("(user_id = $1)", map[string]tableRef{}, "public", "other_table", "exact", ctx)

	a := ctx.byPosition[1]
	if a.Table != "user_account" || a.Confidence != "exact" {
		t.Errorf("exact attribution overwritten by heuristic: %+v", a)
	}
}

// A plan can only attribute parameters that appear in a condition. One that
// sits in a target list is still emitted, as confidence:"none", so the
// cluster is never dropped (FR ask 4).
func TestAttributeFromPlanFillsMissing(t *testing.T) {
	planJSON := []byte(`[{"Plan": {"Node Type": "Seq Scan", "Schema": "public", "Relation Name": "events", "Filter": "(account_id = $1)"}}]`)
	canonical := "SELECT $2 FROM events WHERE account_id = $1"

	got := attributeFromPlan(planJSON, paramPositions(canonical))
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(got), got)
	}
	want := []qshape.ParamAttribution{
		{Position: 1, Schema: "public", Table: "events", Column: "account_id", Confidence: "exact"},
		{Position: 2, Confidence: "none"},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// On PG < 17 ruleutils prints InitPlan outputs as `$N` in the same namespace
// as the query's own parameters. When `$1` is an InitPlan output, the
// external `$1` must not be attributed to the node that references the
// InitPlan.
func TestAttributeFromPlanInitPlanAmbiguityPG16(t *testing.T) {
	planJSON := []byte(`[{"Plan": {
		"Node Type": "Seq Scan", "Schema": "public", "Relation Name": "events",
		"Filter": "((account_id = $0) AND (tenant_id = $1))",
		"Plans": [
			{"Node Type": "Seq Scan", "Schema": "public", "Relation Name": "accounts",
			 "Parent Relationship": "InitPlan", "Subplan Name": "InitPlan 1 (returns $0)",
			 "Filter": "((status)::text = $1)"},
			{"Node Type": "Seq Scan", "Schema": "public", "Relation Name": "accounts",
			 "Parent Relationship": "InitPlan", "Subplan Name": "InitPlan 2 (returns $1)",
			 "Filter": "((status)::text = $2)"}
		]}}]`)
	canonical := "SELECT * FROM events WHERE account_id = (SELECT account_id FROM accounts WHERE status = $1) AND tenant_id = (SELECT account_id FROM accounts WHERE status = $2)"

	got := attributeFromPlan(planJSON, paramPositions(canonical))
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(got), got)
	}
	if got[0].Confidence != "none" {
		t.Errorf("$1 collides with an InitPlan output and must be none, got %+v", got[0])
	}
	if got[1].Confidence != "exact" || got[1].Column != "status" {
		t.Errorf("$2 should attribute to accounts.status, got %+v", got[1])
	}
}

// Confidence ranks exact > expression > heuristic: a later hit only replaces
// an earlier one it outranks.
func TestAttributeCondRankMerge(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{"e": {Schema: "public", Table: "events"}}
	steps := []struct{ cond, want string }{
		{"(outer.x = $1)", "heuristic"},
		{"(e.created_at > now() - $1)", "expression"},
		{"(outer.y = $1)", "expression"},
		{"(e.updated_at = $1)", "exact"},
		{"(e.created_at > now() - $1)", "exact"},
	}
	for _, st := range steps {
		attributeCond(st.cond, aliases, "public", "other", "exact", ctx)
		if got := ctx.byPosition[1].Confidence; got != st.want {
			t.Fatalf("after %s: confidence = %s, want %s", st.cond, got, st.want)
		}
	}
}

// On PG 17+ InitPlan outputs are referenced as `(InitPlan N).colN`, so the
// external `$1`/`$2` are unambiguous and both attribute.
func TestAttributeFromPlanInitPlanPG18(t *testing.T) {
	planJSON := []byte(`[{"Plan": {
		"Node Type": "Seq Scan", "Schema": "public", "Relation Name": "events",
		"Filter": "((account_id = (InitPlan 1).col1) AND (tenant_id = (InitPlan 2).col1))",
		"Plans": [
			{"Node Type": "Seq Scan", "Schema": "public", "Relation Name": "accounts",
			 "Parent Relationship": "InitPlan", "Subplan Name": "InitPlan 1",
			 "Filter": "((status)::text = $1)"},
			{"Node Type": "Seq Scan", "Schema": "public", "Relation Name": "accounts",
			 "Parent Relationship": "InitPlan", "Subplan Name": "InitPlan 2",
			 "Filter": "((status)::text = $2)"}
		]}}]`)
	canonical := "SELECT * FROM events WHERE account_id = (SELECT account_id FROM accounts WHERE status = $1) AND tenant_id = (SELECT account_id FROM accounts WHERE status = $2)"

	got := attributeFromPlan(planJSON, paramPositions(canonical))
	for i, a := range got {
		if a.Confidence != "exact" || a.Column != "status" {
			t.Errorf("entry %d = %+v, want exact accounts.status", i, a)
		}
	}
}
