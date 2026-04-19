package main

import (
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

func TestAttributeCondUnaliasedFallback(t *testing.T) {
	ctx := &attrCtx{byPosition: map[int]*qshape.ParamAttribution{}}
	aliases := map[string]tableRef{}
	attributeCond("(id = $1)", aliases, "auth", "session", ctx)

	a, ok := ctx.byPosition[1]
	if !ok {
		t.Fatal("expected param 1 attributed")
	}
	if a.Table != "session" || a.Column != "id" {
		t.Errorf("wrong fallback attribution: %+v", a)
	}
	if a.Confidence != "heuristic" {
		t.Errorf("expected heuristic confidence, got %s", a.Confidence)
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
