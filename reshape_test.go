package qshape

import (
	"strings"
	"testing"
)

func TestReshapeStripsSingleTableAlias(t *testing.T) {
	got, err := Normalize("SELECT u.id, u.name FROM users u WHERE u.id = $1")
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT id, name FROM users WHERE id = $1"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeLeavesUnaliasedUnchanged(t *testing.T) {
	got, err := Normalize("SELECT id FROM users WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT id FROM users WHERE id = $1" {
		t.Errorf("unexpected rewrite: %q", got)
	}
}

func TestReshapeORMVariantsCollapse(t *testing.T) {
	fpA, err := Fingerprint("SELECT id, name FROM users WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	fpB, err := Fingerprint("SELECT u.id, u.name FROM users u WHERE u.id = $1")
	if err != nil {
		t.Fatal(err)
	}
	if fpA != fpB {
		t.Errorf("aliased and unaliased should fingerprint the same:\n  bare:    %s\n  aliased: %s", fpA, fpB)
	}
}

func TestReshapeSortsFlatAndTree(t *testing.T) {
	// The two queries differ only in AND-conjunct ordering; they should
	// reshape to the same form
	a, err := Normalize("SELECT id FROM users WHERE a = $1 AND b = $2")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Normalize("SELECT id FROM users WHERE b = $2 AND a = $1")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("reordered AND conjuncts did not collapse:\n  a: %q\n  b: %q", a, b)
	}
}

func TestReshapeIdempotent(t *testing.T) {
	cases := []string{
		"SELECT u.id FROM users u WHERE u.id = $1 AND u.tenant_id = $2",
		"SELECT id FROM users",
		"SELECT id FROM users WHERE b = $1 AND a = $2 AND c = $3",
		"UPDATE users SET name = $1 WHERE id = $2",
		"SELECT u.id, o.total FROM users u JOIN orders o ON o.user_id = u.id",
		"SELECT a.id FROM users a JOIN users b ON a.id = b.parent_id",
		"WITH recent AS (SELECT id FROM users) SELECT id FROM recent",
	}
	for _, in := range cases {
		first, err := Normalize(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		second, err := Normalize(first)
		if err != nil {
			t.Fatalf("%q (second pass): %v", first, err)
		}
		if first != second {
			t.Errorf("not idempotent for %q:\n  first:  %q\n  second: %q", in, first, second)
		}
	}
}

func TestReshapeJoinCanonicalisesAliasesToRelname(t *testing.T) {
	// In multi-relation scopes we can't strip to bare (would be ambiguous),
	// but we can canonicalise `u.col` / `users.col` to the same form
	got, err := Normalize("SELECT u.id, o.total FROM users u INNER JOIN orders o ON o.user_id = u.id WHERE u.tenant = $1")
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT users.id, orders.total FROM users JOIN orders ON orders.user_id = users.id WHERE users.tenant = $1"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeJoinAliasedAndUnaliasedCollapse(t *testing.T) {
	a, err := Fingerprint("SELECT u.id, o.total FROM users u JOIN orders o ON o.user_id = u.id")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Fingerprint("SELECT users.id, orders.total FROM users JOIN orders ON orders.user_id = users.id")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("alias and relname variants should collapse:\n  aliased: %s\n  bare:    %s", a, b)
	}
}

func TestReshapeSelfJoinPreservesAliases(t *testing.T) {
	got, err := Normalize("SELECT a.id FROM users a INNER JOIN users b ON a.id = b.parent_id")
	if err != nil {
		t.Fatal(err)
	}
	// Both aliases must stay — otherwise the join is ambiguous
	if got != "SELECT a.id FROM users a JOIN users b ON a.id = b.parent_id" {
		t.Errorf("self-join aliases got stripped: %q", got)
	}
}

func TestReshapeRangeSubselectAliasRequired(t *testing.T) {
	// SQL syntactically requires the alias on a subselect in FROM
	got, err := Normalize("SELECT s.id FROM (SELECT id FROM users) s")
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT s.id FROM (SELECT id FROM users) s" {
		t.Errorf("subselect alias got stripped: %q", got)
	}
}

func TestReshapeUpdateStripsAlias(t *testing.T) {
	got, err := Normalize("UPDATE users u SET name = $1 WHERE u.id = $2")
	if err != nil {
		t.Fatal(err)
	}
	if got != "UPDATE users SET name = $1 WHERE id = $2" {
		t.Errorf("got: %q", got)
	}
}

func TestReshapeDeleteStripsAlias(t *testing.T) {
	got, err := Normalize("DELETE FROM users u WHERE u.id = $1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "DELETE FROM users WHERE id = $1" {
		t.Errorf("got: %q", got)
	}
}

func TestReshapeCorrelatedSubqueryRewritesOuterRef(t *testing.T) {
	got, err := Normalize("SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)")
	if err != nil {
		t.Fatal(err)
	}
	// Outer `u` stripped; correlated `u.id` inside subquery must also
	// be rewritten, or the deparsed SQL references a missing alias
	want := "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeCTEGetsOwnScope(t *testing.T) {
	got, err := Normalize("WITH recent AS (SELECT u.id FROM users u WHERE u.created_at > $1) SELECT r.id FROM recent r")
	if err != nil {
		t.Fatal(err)
	}
	want := "WITH recent AS (SELECT id FROM users WHERE created_at > $1) SELECT id FROM recent"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeCTEUsedInOuterJoinKeepsAliases(t *testing.T) {
	// Outer scope has the CTE ref + another table; both expose user_id.
	// Stripping the `s` alias would leave an ambiguous bare `user_id`
	in := "WITH ur AS (SELECT ua.user_id FROM auth.user_account ua WHERE ua.user_id = $1) " +
		"SELECT ur.user_id FROM ur JOIN sessions s ON s.user_id = ur.user_id"
	got, err := Normalize(in)
	if err != nil {
		t.Fatal(err)
	}
	// The outer join condition must remain qualified on the sessions side
	want := "WITH ur AS (SELECT user_id FROM auth.user_account WHERE user_id = $1) " +
		"SELECT ur.user_id FROM ur JOIN sessions ON sessions.user_id = ur.user_id"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeCommaJoinCanonicalisesAliases(t *testing.T) {
	got, err := Normalize("SELECT a.id FROM users a, orders b WHERE a.id = b.user_id")
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT users.id FROM users, orders WHERE users.id = orders.user_id"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeUpdateWithFromCanonicalisesAliases(t *testing.T) {
	// UPDATE ... FROM introduces a second relation into the WHERE scope
	got, err := Normalize("UPDATE users u SET name = $1 FROM orders o WHERE o.user_id = u.id")
	if err != nil {
		t.Fatal(err)
	}
	want := "UPDATE users SET name = $1 FROM orders WHERE orders.user_id = users.id"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeExtractFieldStaysIdentifier(t *testing.T) {
	// pg_query parses `extract(epoch FROM x)` with `epoch` as a string
	// constant; its deparser would render it as `'epoch'`, which downstream
	// parameterisers then rewrite to `$N` and break the EXTRACT syntax. The
	// reshape must keep the field as a bare identifier.
	got, err := Normalize("SELECT auth.f(extract(epoch FROM '1 hour'::interval)::bigint, 100)")
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT auth.f(extract (epoch FROM '1 hour'::interval)::bigint, 100)"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeExtractParamFieldRecovered(t *testing.T) {
	// If the input already had the field parameterised (seen in the wild
	// from pg_stat_statements pipelines that normalise `'epoch'` -> `$1`),
	// the canonical must still be executable. Substitute a stable ident.
	got, err := Normalize("SELECT auth.clean_up_sessions(extract($1 FROM $2::interval)::bigint, $3)")
	if err != nil {
		t.Fatal(err)
	}
	// After recovery, remaining params must be renumbered contiguously
	// from $1 — EXPLAIN (GENERIC_PLAN) rejects gaps
	want := "SELECT auth.clean_up_sessions(extract (epoch FROM $1::interval)::bigint, $2)"
	if got != want {
		t.Errorf("got:  %q\nwant: %q", got, want)
	}
}

func TestReshapeCorrelatedRefInNestedSubqueryFrom(t *testing.T) {
	// Outer alias `u` (decorative, stripped in outer scope) is referenced
	// from a derived-table WHERE that lives inside a correlated subquery's
	// FROM. rewriteInSelect must walk the subquery's FROM, not just its
	// TargetList/WHERE, or the outer ref is left dangling
	in := `SELECT u.id, (SELECT x FROM (SELECT a.x FROM t a WHERE a.uid = u.id) sub) FROM updated u JOIN t2 p ON p.uid = u.id`
	got, err := Normalize(in)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "u.") {
		t.Errorf("outer alias `u` leaked into rewritten SQL: %q", got)
	}
}

// COALESCE is parsed as CoalesceExpr, not FuncCall. rewriteRefs must walk
// its Args or aliases inside COALESCE survive after FROM-alias stripping
// and the deparsed SQL references a missing table. Seen in a production
// capture (attribute returned "missing FROM-clause entry for table \"o\"").
func TestReshapeCoalesceArgsGetRewritten(t *testing.T) {
	got, err := Normalize("SELECT COALESCE(CAST(o.options -> $1 AS boolean), $2) FROM org.organization o LEFT JOIN org.workspace ON organization.id = workspace.org_id")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "o.options") {
		t.Errorf("o.options not rewritten inside COALESCE: %q", got)
	}
}

// sum(pg_stat_get_live_tuples(c.oid)) is a FuncCall nested in a FuncCall
// nested in a CoalesceExpr. Before the fix, the outer CoalesceExpr was
// skipped so c.oid was never reached.
func TestReshapeCoalesceWithNestedFuncCallRef(t *testing.T) {
	got, err := Normalize("SELECT COALESCE(sum(pg_stat_get_live_tuples(c.oid)), $1) FROM pg_class c LEFT JOIN pg_namespace ON pg_namespace.oid = c.relnamespace WHERE c.relkind = $2")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "c.oid") {
		t.Errorf("c.oid not rewritten: %q", got)
	}
}

// FILTER (WHERE ...) on an aggregate lands in FuncCall.AggFilter, not Args.
// Also exercises alias refs inside a CASE expression that's an array_agg arg.
func TestReshapeAggFilterAndCaseExprRefsGetRewritten(t *testing.T) {
	got, err := Normalize(`WITH r AS (SELECT COALESCE(array_agg(DISTINCT CASE wu.role WHEN $1 THEN 'a' END) FILTER (WHERE wu.role IS NOT NULL), ARRAY[]::text[]) AS roles FROM org.workspace_user wu JOIN org.organization_user ON wu.org_user_id = organization_user.id GROUP BY wu.org_user_id) SELECT * FROM r`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "wu.") {
		t.Errorf("wu. refs not rewritten (FILTER and/or CASE arg): %q", got)
	}
}

func TestReshapeUnionBothArms(t *testing.T) {
	a, err := Normalize("SELECT u.id FROM users u UNION SELECT o.user_id FROM orders o")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Normalize("SELECT id FROM users UNION SELECT user_id FROM orders")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("UNION arms did not collapse to the same form:\n  a: %q\n  b: %q", a, b)
	}
}

// INSERT statements have their own WithClause — the fixup walker needs to
// descend into it. Regression: extract($N FROM …) inside a CTE on an
// INSERT survived normalize and later broke PREPARE with "syntax error
// at or near $N" (the EXTRACT field must be an identifier, not a param).
func TestReshapeInsertWithClauseGetsFixups(t *testing.T) {
	sql := `WITH c AS (SELECT r FROM t gs WHERE extract($1 FROM gs) NOT IN ($2, $3)) INSERT INTO x SELECT * FROM c`
	got, err := Normalize(sql)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "extract ($") || strings.Contains(got, "extract($") {
		t.Errorf("extract($N …) not fixed inside INSERT's CTE: %q", got)
	}
}

// extract() nested inside COALESCE/round() — walkFixups didn't descend
// into CoalesceExpr / RowExpr / MinMaxExpr / AArrayExpr or into
// SubLink/RangeSubselect/RangeFunction/JoinExpr. Real corpus hit this as
// COALESCE(round(extract($N FROM upper(col)))).
func TestReshapeExtractInsideCoalesceGetsFixed(t *testing.T) {
	got, err := Normalize(`SELECT COALESCE(round(extract($1 FROM col)), 0) FROM t`)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "extract ($") || strings.Contains(got, "extract($") {
		t.Errorf("extract($N) inside COALESCE(round(...)) not fixed: %q", got)
	}
}
