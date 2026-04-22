package qshape

import "testing"

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
