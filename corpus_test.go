package qshape

import (
	"encoding/json"
	"os"
	"testing"
)

// prettifySeeds is the committed corpus the layout and extraction properties are checked
// against, and the fuzz seed set.
//
// It exists because clusters.json — the real captured corpus — is gitignored, so any test
// that depends on it skips everywhere except the machine that produced it. A guarantee
// that silently skips in CI is not a guarantee, and these are the properties the product
// leans on hardest: that laying a statement out never changes it, and that table
// extraction sees what the SQL actually names.
//
// Chosen for the shapes that break naive implementations, not for coverage of SQL: the
// lexical hazards (strings that contain keywords, dollar quoting, string continuation,
// comments, quoted identifiers, operators that could fuse) and the structural ones (DML
// targets that hang off the statement rather than a FROM list, CTEs, set operations,
// LATERAL, MERGE).
//
// Every entry must be valid SQL, not merely scannable: Prettify only needs the scanner,
// but Tables needs the parser, and both properties are checked over this list.
var prettifySeeds = []string{
	// Plain shapes.
	"SELECT id FROM orders",
	"SELECT a, b FROM orders JOIN users ON users.id = orders.user_id WHERE a = $1 AND b = $2 GROUP BY a ORDER BY b LIMIT $3",
	"SELECT count(*), coalesce(sum(total), $1) FROM orders WHERE created_at > $2",

	// DML: the target relation hangs off the statement, not off a FROM list.
	"INSERT INTO orders (id, total) VALUES ($1, $2)",
	"INSERT INTO orders (id) SELECT id FROM staging_orders ON CONFLICT (id) DO UPDATE SET id = $1",
	"UPDATE orders SET total = $1 FROM users WHERE users.id = orders.user_id",
	"DELETE FROM orders USING users WHERE users.id = orders.user_id",
	"MERGE INTO orders USING staging_orders ON orders.id = staging_orders.id WHEN MATCHED THEN UPDATE SET total = staging_orders.total",

	// Structure.
	"WITH recent AS (SELECT id FROM orders WHERE created_at > $1) SELECT * FROM recent JOIN users ON users.id = recent.id",
	"WITH RECURSIVE tree AS (SELECT id, parent_id FROM project WHERE parent_id IS NULL UNION ALL SELECT p.id, p.parent_id FROM project p JOIN tree ON p.parent_id = tree.id) SELECT * FROM tree",
	"SELECT id FROM orders UNION ALL SELECT id FROM archived_orders ORDER BY id",
	"SELECT * FROM (SELECT user_id FROM time_entry) sub JOIN users ON users.id = sub.user_id",
	"SELECT v FROM task CROSS JOIN LATERAL unnest(task.assignee_user_ids) v",
	"SELECT * FROM orders WHERE EXISTS (SELECT $1 FROM goal WHERE goal.user_id = orders.user_id)",
	"SELECT a, CASE WHEN b > $1 THEN $2 ELSE $3 END FROM t",
	"SELECT count(*) FILTER (WHERE a > $1) OVER (PARTITION BY b ORDER BY c) FROM t",

	// Lexical hazards: the reason this runs off the real scanner and not a pattern match.
	"SELECT 'a literal with FROM and WHERE and JOIN inside' FROM t",
	"SELECT $$dollar quoted with ) paren and FROM$$ FROM t",
	"SELECT $tag$tagged $$ nested$tag$ FROM t",
	"SELECT 'book'\n'end' AS continued FROM t",
	`SELECT "select", "from", "weird""quote" FROM "where"`,
	"SELECT a /* block comment */ FROM t",
	"SELECT a -- line comment\nFROM t",
	`SELECT E'\n', U&'d\0061t' FROM t`,
	"SELECT a - -b, +1, -1, 1 - -1 FROM t",
	"SELECT a::int, b::text, cast(c AS numeric) FROM t",
	"SELECT a || b, c != d, e <> f, g @> h, i <@ j FROM t",
	"SELECT j->>'k', j->'k', j#>>'{k}' FROM t",
	"SELECT * FROM t WHERE x ~ '^a' AND y !~~ 'b'",
	"SELECT (ARRAY[$1, $2])[$3] FROM t",
	"SELECT 1.5, .5, 1e10 FROM t",
	"SELECT a FROM t WHERE b IN ($1, $2) AND c BETWEEN $3 AND $4",
	"SELECT * FROM public.orders JOIN tenant_42.users ON true",
}

// corpusStatements returns the committed seeds plus, when it happens to be present, the
// real captured corpus. The seeds are what makes the property tests run everywhere;
// clusters.json is a bonus for whoever has it.
func corpusStatements(t *testing.T) []string {
	t.Helper()
	out := append([]string(nil), prettifySeeds...)

	raw, err := os.ReadFile("clusters.json")
	if err != nil {
		return out
	}
	var corpus struct {
		Clusters []struct {
			Canonical string `json:"canonical"`
		} `json:"clusters"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Logf("captured corpus present but unreadable, using seeds only: %v", err)
		return out
	}
	for _, c := range corpus.Clusters {
		if c.Canonical != "" {
			out = append(out, c.Canonical)
		}
	}
	t.Logf("corpus: %d committed seeds + %d captured statements", len(prettifySeeds), len(out)-len(prettifySeeds))
	return out
}
