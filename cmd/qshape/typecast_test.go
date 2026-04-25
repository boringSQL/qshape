package main

import (
	"context"
	"testing"
)

func TestCastFuncParamRefsWrapsUnqualifiedParams(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{
		"toggl|is_login_restricted_by_sso|1": {"text"},
	}}
	in := `SELECT toggl.is_login_restricted_by_sso($1)`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT toggl.is_login_restricted_by_sso($1::text)`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestCastFuncParamRefsMultipleArgsMixed(t *testing.T) {
	// Non-param arg stays untouched; param args get typed
	cache := &typecastCache{cache: map[string][]string{
		"auth|clean_up_sessions|2": {"int8", "int4"},
	}}
	in := `SELECT auth.clean_up_sessions($1, 100)`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT auth.clean_up_sessions($1::int8, 100)`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestCastFuncParamRefsQualifiedType(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{
		"public|do_thing|1": {"public.my_type"},
	}}
	in := `SELECT public.do_thing($1)`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT public.do_thing($1::public.my_type)`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestCastFuncParamRefsSkipsUnresolved(t *testing.T) {
	// Empty cache → negative cache entry would be added by lookup, but
	// since no conn is set, query() returns nil. Function should be left
	// alone and canonical returned unchanged.
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT unknown.f($1)`
	got := castFuncParamRefs(context.Background(), cache, in)
	if got != in {
		t.Errorf("expected passthrough, got: %q", got)
	}
}

func TestCastFuncParamRefsSkipsExtract(t *testing.T) {
	// pg_catalog.extract is a COERCE_SQL_SYNTAX form; its first arg is a
	// field identifier, not an overload-resolvable param. Must not cast.
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT extract (epoch FROM $1::interval)`
	got := castFuncParamRefs(context.Background(), cache, in)
	if got != in {
		t.Errorf("expected passthrough for extract, got: %q", got)
	}
}

func TestMaxParamNumber(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"SELECT 1", 0},
		{"SELECT $1", 1},
		{"SELECT $1, $2, $3", 3},
		{"SELECT $10 + $2", 10},
		{"SELECT '$1 inside string'", 0},
		{"SELECT 'it''s $5' || $3", 3},
	}
	for _, tc := range cases {
		if got := maxParamNumber(tc.in); got != tc.want {
			t.Errorf("maxParamNumber(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestCastFuncParamRefsLeavesColumnContextParams(t *testing.T) {
	// No FuncCall with ParamRef arg — nothing to do. $1 in WHERE clause
	// is resolved by the planner from the column type
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT id FROM users WHERE id = $1`
	got := castFuncParamRefs(context.Background(), cache, in)
	if got != in {
		t.Errorf("expected passthrough, got: %q", got)
	}
}

// BoolExpr (AND/OR/NOT) args must be boolean. Bare ParamRef there defaults
// to unknown/text and breaks planning with "argument of OR must be type
// boolean, not type integer".
func TestCastBoolExprParamToBool(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT * FROM users WHERE $1 OR active`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT * FROM users WHERE $1::bool OR active`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

// VARIADIC ANY functions like json_build_object have pronargs=0 in pg_proc
// so castFuncCall can't resolve them. The keys (odd positions) must be text.
func TestCastJsonBuildObjectKeysToText(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT json_build_object($1, user_id, $2, name)`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT json_build_object($1::text, user_id, $2::text, name)`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

// EXISTS (SELECT $N FROM ...) — app parameterized the SELECT 1. PG still
// needs to type $N; cast to int so the subquery plans.
func TestCastExistsSelectListParamToInt(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT EXISTS (SELECT $1 FROM users WHERE id = $2)`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT EXISTS (SELECT $1::int4 FROM users WHERE id = $2)`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

// LEFT JOIN t ON $N — JOIN qualifier must be boolean. Bare ParamRef here
// plans as unknown/text and fails with "argument of JOIN/ON must be type
// boolean". Seen in multi-CTE admin_users query that LEFT JOINs an
// aggregate-only CTE with a parameterized ON.
func TestCastJoinOnParamToBool(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT * FROM a LEFT JOIN b ON $1`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT * FROM a LEFT JOIN b ON $1::bool`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

// Searched CASE — each WHEN is a predicate, must be boolean.
// Simple CASE (with Arg) uses WHEN as a value to compare, so must stay
// typed to Arg's type. Only the searched form gets the bool cast.
func TestCastSearchedCaseWhenParamToBool(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT CASE WHEN $1 THEN 'a' ELSE 'b' END`
	got := castFuncParamRefs(context.Background(), cache, in)
	want := `SELECT CASE WHEN $1::bool THEN 'a' ELSE 'b' END`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

// Simple CASE arg WHEN val — the WHEN value shouldn't get a bool cast.
func TestCastSimpleCaseWhenParamUncast(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT CASE col WHEN $1 THEN 'a' ELSE 'b' END FROM t`
	got := castFuncParamRefs(context.Background(), cache, in)
	if got != in {
		t.Errorf("simple CASE WHEN value should stay uncast, got: %q", got)
	}
}

// castFuncParamRefs must descend into CoalesceExpr to reach nested
// FuncCalls. COALESCE is a SQL construct, not a FuncCall — the walker
// needs an explicit case. json_build_object buried inside COALESCE was
// missed before the fix and caused "could not determine data type of
// parameter" errors on complex aggregate queries.
func TestCastDescendsIntoCoalesceExpr(t *testing.T) {
	cache := &typecastCache{cache: map[string][]string{}}
	in := `SELECT COALESCE(json_agg(json_build_object($1, col)), $2::pg_catalog.json) FROM t`
	got := castFuncParamRefs(context.Background(), cache, in)
	// $1 is a key (odd position in json_build_object) → must be text.
	// $2 already has explicit ::json cast, left alone.
	want := `SELECT COALESCE(json_agg(json_build_object($1::text, col)), $2::pg_catalog.json) FROM t`
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}
