package qshape

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// The guarantee that makes Prettify safe to put in front of a user: it moves
// whitespace and nothing else, so the statement it returns is the statement it was
// given. Checked over the whole captured corpus, against the fingerprint rather than
// against a golden layout, because layout is allowed to change and meaning is not.
func TestPrettifyPreservesFingerprint(t *testing.T) {
	var checked int
	for _, sql := range corpusStatements(t) {
		want, err := Fingerprint(sql)
		if err != nil {
			continue // qshape cannot fingerprint it either
		}
		pretty, err := Prettify(sql)
		if err != nil {
			t.Errorf("Prettify rejected a statement: %v\n%s", err, sql)
			continue
		}
		got, err := Fingerprint(pretty)
		if err != nil {
			t.Errorf("prettified form no longer parses: %v\n%s", err, pretty)
			continue
		}
		if got != want {
			t.Errorf("Prettify changed the statement.\nfingerprint %s -> %s\nbefore: %s\nafter:\n%s",
				want, got, sql, pretty)
			continue
		}
		// Content, not just meaning: the fingerprint is blind to comments, so a dropped
		// comment would pass the check above while changing what the user is shown.
		if strip(pretty) != strip(sql) {
			t.Errorf("Prettify changed content.\nbefore: %q\nafter:  %q", sql, pretty)
			continue
		}
		checked++
	}
	if checked < len(prettifySeeds)/2 {
		t.Fatalf("only %d statements checked; the corpus is not exercising this", checked)
	}
	t.Logf("fingerprint and content preserved across %d statements", checked)
}

// scanTokens returns the scanner's token texts, which is the real preservation property:
// Prettify re-joins tokens with its own spacing, so the way it could corrupt a statement
// is by merging two into one (`a - -b` re-joined as `a--b` is a line comment that eats
// the rest of the line), splitting one, or dropping one. Comparing token streams catches
// all three, and unlike the fingerprint check it works on text that scans but does not
// parse — which is exactly what a truncated pg_stat_statements entry is.
func scanTokens(t *testing.T, sql string) []string {
	t.Helper()
	r, err := pg_query.Scan(sql)
	if err != nil {
		t.Fatalf("scan %q: %v", sql, err)
	}
	out := make([]string, 0, len(r.Tokens))
	for _, tok := range r.Tokens {
		out = append(out, sql[tok.Start:tok.End])
	}
	return out
}

func assertTokensPreserved(t *testing.T, sql string) {
	t.Helper()
	before := scanTokens(t, sql)
	pretty, err := Prettify(sql)
	if err != nil {
		t.Errorf("Prettify(%q): %v", sql, err)
		return
	}
	after := scanTokens(t, pretty)
	if len(before) != len(after) {
		t.Errorf("token count %d -> %d\n in:  %q\n out: %q", len(before), len(after), sql, pretty)
		return
	}
	for i := range before {
		if before[i] != after[i] {
			t.Errorf("token %d %q -> %q\n in:  %q\n out: %q", i, before[i], after[i], sql, pretty)
			return
		}
	}
}

// Adjacent tokens must not fuse when the layout drops the space between them. The
// dangerous pairs are the ones that spell something else together: two minus signs are a
// comment, and a comment would swallow the rest of the statement.
func TestPrettifyNeverFusesAdjacentTokens(t *testing.T) {
	for _, sql := range []string{
		"SELECT a - -b FROM t",
		"SELECT a - -1 FROM t",
		"SELECT (- -1) FROM t",
		"SELECT +1, -1, 1 - -1 FROM t",
		"SELECT a::int, b::text FROM t",
		"SELECT a || b, c != d, e <> f FROM t",
		"SELECT j->>'k', j->'k', j#>>'{k}' FROM t",
		"SELECT * FROM t WHERE a @> b AND c <@ d",
		"SELECT * FROM t WHERE x ~ '^a' AND y !~~ 'b'",
		`SELECT E'\n', U&'d\0061t' FROM t`,
		"SELECT $$a$$ || $tag$b$tag$ FROM t",
		"SELECT 1.5, .5, 1e10 FROM t",
		`SELECT "weird""quote" FROM t`,
		"SELECT ARRAY[1,2][1] FROM t",
		"SELECT a /* c1 */ /* c2 */ FROM t",
		"SELECT a-- comment\nFROM t",
		"SELECT CAST(a AS int) FROM t",
		"SELECT a FROM t WHERE b IN (1,2) AND c BETWEEN 1 AND 2",
	} {
		assertTokensPreserved(t, sql)
	}
}

// A string continuation is ONE token, not two, so laying the statement out cannot break
// it. Postgres concatenates `'a'` and `'b'` only when a newline separates them, which
// would make collapsing that newline to a space a syntax error; the lexer resolves the
// continuation itself and hands back a single SCONST whose text spans the break, and
// Prettify copies token text verbatim. Pinned because the hazard is real and the reason
// it does not bite is not obvious from Prettify's code.
func TestPrettifyKeepsStringContinuationsIntact(t *testing.T) {
	const sql = "SELECT 'book'\n'end' AS x FROM t"
	want, err := Fingerprint(sql)
	if err != nil {
		t.Fatalf("Fingerprint: %v", err)
	}
	pretty, err := Prettify(sql)
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	got, err := Fingerprint(pretty)
	if err != nil {
		t.Fatalf("prettified string continuation no longer parses: %v\n%s", err, pretty)
	}
	if got != want {
		t.Fatalf("string continuation changed meaning:\n%s", pretty)
	}
}

// The same properties over a corpus. corpusStatements is committed, so this actually
// runs; clusters.json is a real captured corpus but gitignored, so it can only ever be
// an extra on the machine that has it — a guarantee that skips in CI is not a guarantee.
func TestPrettifyPreservesTokensAcrossCorpus(t *testing.T) {
	for _, sql := range corpusStatements(t) {
		assertTokensPreserved(t, sql)
	}
	// A statement pg_stat_statements truncated at track_activity_query_size: it will not
	// parse, but it still scans, and what the page shows must still be what was stored.
	assertTokensPreserved(t, "SELECT id FROM orders WHERE id IN ($1, $2, $")
}

// FuzzPrettify holds the whole contract at once: layout never drops, adds, splits or
// fuses a token, and never changes what a statement means. Runs its seed corpus as an
// ordinary test in CI, and goes deeper on demand with -fuzz.
func FuzzPrettify(f *testing.F) {
	for _, sql := range prettifySeeds {
		f.Add(sql)
	}
	f.Fuzz(func(t *testing.T, sql string) {
		before, err := pg_query.Scan(sql)
		if err != nil {
			return // not SQL the scanner can read; Prettify is allowed to refuse it
		}
		pretty, err := Prettify(sql)
		if err != nil {
			t.Fatalf("Prettify refused scannable input %q: %v", sql, err)
		}
		after, err := pg_query.Scan(pretty)
		if err != nil {
			t.Fatalf("prettified output no longer scans\n in:  %q\n out: %q\n err: %v", sql, pretty, err)
		}
		if len(before.Tokens) != len(after.Tokens) {
			t.Fatalf("token count %d -> %d\n in:  %q\n out: %q",
				len(before.Tokens), len(after.Tokens), sql, pretty)
		}
		for i := range before.Tokens {
			b := sql[before.Tokens[i].Start:before.Tokens[i].End]
			a := pretty[after.Tokens[i].Start:after.Tokens[i].End]
			if a != b {
				t.Fatalf("token %d %q -> %q\n in:  %q\n out: %q", i, b, a, sql, pretty)
			}
		}
		// Meaning, for anything that parses at all.
		if want, err := Fingerprint(sql); err == nil {
			got, err := Fingerprint(pretty)
			if err != nil {
				t.Fatalf("prettified output no longer parses\n in:  %q\n out: %q\n err: %v", sql, pretty, err)
			}
			if got != want {
				t.Fatalf("fingerprint %s -> %s\n in:  %q\n out: %q", want, got, sql, pretty)
			}
		}
	})
}

// Whitespace-only means every non-space character survives in order. A stricter check
// than the fingerprint one: it also catches a dropped comment, which does not change
// the parse tree but does change what the user is shown.
func TestPrettifyDropsNothing(t *testing.T) {
	cases := []string{
		"SELECT a, b FROM t WHERE x = $1 AND y = $2",
		"SELECT a /* keep me */ FROM t",
		"SELECT a -- keep me too\nFROM t",
		`SELECT E'\n' AS escaped, U&'d\0061t' AS uni FROM t`,
		"SELECT a || b, c != d, j->>'k' FROM t",
		"SELECT 'book'\n'end' AS continued FROM t",
		"SELECT \"ident with spaces\", \"ünïcodé\" FROM t",
		"SELECT 'a literal with FROM and WHERE inside' FROM t",
		"SELECT $$dollar quoted with ) paren$$ FROM t",
		`SELECT "select", "from" FROM "where"`,
		"UPDATE t SET x = $1 WHERE id = $2 RETURNING id",
		"INSERT INTO t (a, b) VALUES ($1, $2) ON CONFLICT (a) DO NOTHING",
		"WITH x AS (SELECT id FROM t) SELECT * FROM x JOIN y ON y.id = x.id",
	}
	for _, sql := range cases {
		pretty, err := Prettify(sql)
		if err != nil {
			t.Errorf("Prettify(%q): %v", sql, err)
			continue
		}
		if strip(pretty) != strip(sql) {
			t.Errorf("Prettify changed content.\nin:  %q -> %q\nout: %q -> %q",
				sql, strip(sql), pretty, strip(pretty))
		}
	}
}

func strip(s string) string {
	return strings.Join(strings.Fields(s), "")
}

// A line comment runs to end of line, so a token placed after one on the same line
// would be commented out. Layout is correctness here, not taste.
func TestPrettifyKeepsLineCommentsTerminated(t *testing.T) {
	const sql = "SELECT a -- note\nFROM t"
	pretty, err := Prettify(sql)
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	// The comment must still be there. Without this the test passes when the comment is
	// DELETED (no line contains "--", the loop below finds nothing, and the fingerprint
	// cannot see comments at all).
	if strip(pretty) != strip(sql) {
		t.Fatalf("content changed; the comment was dropped or altered:\n in:  %q\n out: %q", sql, pretty)
	}
	for _, line := range strings.Split(pretty, "\n") {
		if _, after, found := strings.Cut(line, "--"); found && strings.TrimSpace(after) != "note" {
			t.Fatalf("a token followed a line comment on its own line:\n%s", pretty)
		}
	}
	if _, err := Fingerprint(pretty); err != nil {
		t.Fatalf("prettified form no longer parses: %v\n%s", err, pretty)
	}
}

// The point of the whole exercise: a wall of text becomes readable lines.
func TestPrettifyBreaksClauses(t *testing.T) {
	const sql = "SELECT a, b FROM orders JOIN users ON users.id = orders.user_id WHERE a = $1 AND b = $2 GROUP BY a ORDER BY b LIMIT $3"
	pretty, err := Prettify(sql)
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	lines := strings.Split(pretty, "\n")
	if len(lines) < 6 {
		t.Fatalf("expected a line per clause, got %d:\n%s", len(lines), pretty)
	}
	for _, want := range []string{"FROM orders", "JOIN users ON", "WHERE a =", "AND b =", "GROUP BY a", "ORDER BY b", "LIMIT"} {
		if !strings.Contains(pretty, want) {
			t.Errorf("missing %q in:\n%s", want, pretty)
		}
	}
	// The JOIN phrase starts its own line rather than trailing the FROM line.
	for _, line := range lines {
		if strings.Contains(line, "JOIN") && !strings.HasPrefix(strings.TrimSpace(line), "JOIN") {
			t.Errorf("JOIN did not start its line: %q", line)
		}
	}
}

// A join phrase breaks before the whole thing, not between its words.
func TestPrettifyBreaksBeforeJoinPrefix(t *testing.T) {
	pretty, err := Prettify("SELECT a FROM t LEFT OUTER JOIN u ON u.id = t.id")
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	if !strings.Contains(pretty, "\nLEFT OUTER JOIN u ON") {
		t.Fatalf("want the join phrase whole on its own line, got:\n%s", pretty)
	}
}

// BETWEEN's AND bounds one predicate; breaking on it would split an expression rather
// than a clause list.
func TestPrettifyKeepsBetweenIntact(t *testing.T) {
	pretty, err := Prettify("SELECT a FROM t WHERE d BETWEEN $1 AND $2 AND x = $3")
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	if !strings.Contains(pretty, "BETWEEN $1 AND $2") {
		t.Fatalf("BETWEEN was split across lines:\n%s", pretty)
	}
	if !strings.Contains(pretty, "\nAND x = $3") {
		t.Fatalf("the real AND should still start a line:\n%s", pretty)
	}
}

// A keyword's letters used as an identifier must not split a line: the scanner is what
// makes this knowable, and it is exactly what a pattern-matching formatter gets wrong.
func TestPrettifyIgnoresKeywordsUsedAsNames(t *testing.T) {
	pretty, err := Prettify(`SELECT "set", "from" FROM "where"`)
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	if strings.Count(pretty, "\n") != 1 {
		t.Fatalf("quoted identifiers were treated as clauses:\n%s", pretty)
	}
	if !strings.Contains(pretty, `FROM "where"`) {
		t.Fatalf("identifier mangled:\n%s", pretty)
	}
}

// A short function call stays on one line; only groups that actually wrap put their
// closing paren on its own.
func TestPrettifyKeepsShortCallsInline(t *testing.T) {
	pretty, err := Prettify("SELECT count(*), coalesce(a, $1) FROM t")
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	if !strings.Contains(pretty, "count(*)") || !strings.Contains(pretty, "coalesce(a, $1)") {
		t.Fatalf("function calls were broken up:\n%s", pretty)
	}
}

// A subquery indents and its closing paren returns to the outer level.
func TestPrettifyIndentsSubqueries(t *testing.T) {
	pretty, err := Prettify("SELECT * FROM (SELECT id FROM t WHERE x = $1) sub")
	if err != nil {
		t.Fatalf("Prettify: %v", err)
	}
	if !strings.Contains(pretty, "\n  SELECT id") {
		t.Fatalf("subquery was not indented:\n%s", pretty)
	}
	if !strings.Contains(pretty, "\n) sub") {
		t.Fatalf("closing paren did not return to the outer level:\n%s", pretty)
	}
}

// Invalid SQL errors rather than returning half a statement, so a caller falls back to
// showing the stored text as-is.
func TestPrettifyRejectsUnscannable(t *testing.T) {
	if _, err := Prettify("SELECT 'unterminated"); err == nil {
		t.Fatal("Prettify accepted an unterminated literal")
	}
}
