package qshape

import (
	"reflect"
	"testing"
)

// tableNames renders refs for comparison. Nil for an empty result, not an empty slice,
// so a case asserting "references nothing" compares against want: nil and fails only
// when Tables is actually wrong.
func tableNames(refs []TableRef) []string {
	var out []string
	for _, r := range refs {
		if r.Schema != "" {
			out = append(out, r.Schema+"."+r.Name)
			continue
		}
		out = append(out, r.Name)
	}
	return out
}

func TestTables(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "simple select",
			sql:  "SELECT id FROM orders",
			want: []string{"orders"},
		},
		{
			name: "join",
			sql:  "SELECT * FROM orders JOIN users ON users.id = orders.user_id",
			want: []string{"orders", "users"},
		},
		{
			name: "schema qualified keeps the schema",
			sql:  "SELECT * FROM public.orders JOIN tenant_42.users ON true",
			want: []string{"public.orders", "tenant_42.users"},
		},
		{
			// The gap that rules out reusing the param walker: the target relation hangs
			// off InsertStmt.Relation, not off any FROM list.
			name: "insert target",
			sql:  "INSERT INTO orders (id) VALUES ($1)",
			want: []string{"orders"},
		},
		{
			name: "insert from select",
			sql:  "INSERT INTO orders (id) SELECT id FROM staging_orders",
			want: []string{"orders", "staging_orders"},
		},
		{
			name: "insert on conflict",
			sql:  "INSERT INTO orders (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id = $2",
			want: []string{"orders"},
		},
		{
			name: "update target and from",
			sql:  "UPDATE orders SET total = $1 FROM users WHERE users.id = orders.user_id",
			want: []string{"orders", "users"},
		},
		{
			name: "delete target and using",
			sql:  "DELETE FROM orders USING users WHERE users.id = orders.user_id",
			want: []string{"orders", "users"},
		},
		{
			name: "merge target and source",
			sql: `MERGE INTO orders USING staging_orders ON orders.id = staging_orders.id
			      WHEN MATCHED THEN UPDATE SET total = staging_orders.total`,
			want: []string{"orders", "staging_orders"},
		},
		{
			name: "subquery in from",
			sql:  "SELECT * FROM (SELECT user_id FROM time_entry) sub JOIN users ON users.id = sub.user_id",
			want: []string{"time_entry", "users"},
		},
		{
			name: "sublink in where",
			sql:  "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM goal WHERE goal.user_id = orders.user_id)",
			want: []string{"goal", "orders"},
		},
		{
			name: "sublink in target list",
			sql:  "SELECT (SELECT count(*) FROM ghosts) AS n, id FROM task",
			want: []string{"ghosts", "task"},
		},
		{
			name: "set operation arms",
			sql:  "SELECT id FROM orders UNION ALL SELECT id FROM archived_orders",
			want: []string{"archived_orders", "orders"},
		},
		{
			name: "lateral function argument",
			sql:  "SELECT v FROM task CROSS JOIN LATERAL unnest(task.assignee_user_ids) v",
			want: []string{"task"},
		},
		{
			name: "cte name is not a table",
			sql:  "WITH recent AS (SELECT id FROM orders) SELECT * FROM recent",
			want: []string{"orders"},
		},
		{
			name: "recursive cte self reference",
			sql: `WITH RECURSIVE tree AS (
			        SELECT id, parent_id FROM project WHERE parent_id IS NULL
			        UNION ALL
			        SELECT p.id, p.parent_id FROM project p JOIN tree ON p.parent_id = tree.id)
			      SELECT * FROM tree`,
			want: []string{"project"},
		},
		{
			name: "cte inside dml",
			sql:  "WITH gone AS (DELETE FROM sessions RETURNING user_id) INSERT INTO audit (user_id) SELECT user_id FROM gone",
			want: []string{"audit", "sessions"},
		},
		{
			// A qualified name is an exact identity, so it survives even when a CTE
			// shares the bare name.
			name: "qualified name survives a cte of the same name",
			sql:  "WITH orders AS (SELECT 1 AS id) SELECT * FROM public.orders JOIN orders ON true",
			want: []string{"public.orders"},
		},
		{
			name: "repeated references dedupe",
			sql:  "SELECT * FROM orders a JOIN orders b ON a.parent_id = b.id",
			want: []string{"orders"},
		},
		{
			name: "self-join across schemas stays distinct",
			sql:  "SELECT * FROM public.orders JOIN tenant_1.orders ON true",
			want: []string{"public.orders", "tenant_1.orders"},
		},
		{
			// An empty result is a real answer, not a failure to read the query. Pinned
			// so nobody downstream starts treating len == 0 as "unparseable".
			name: "references no relation at all",
			sql:  "SELECT $1",
			want: nil,
		},
		{
			name: "values list references no relation",
			sql:  "VALUES ($1, $2)",
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Tables(tc.sql)
			if err != nil {
				t.Fatalf("Tables(%q) errored: %v", tc.sql, err)
			}
			if !reflect.DeepEqual(tableNames(got), tc.want) {
				t.Fatalf("Tables(%q) = %v, want %v", tc.sql, tableNames(got), tc.want)
			}
		})
	}

	// The cloud extracts from qshape's own deparse, never from the operator's SQL, so
	// every case must survive the round trip through Normalize. This is what actually
	// exercises the extractor against reshaped predicates, renumbered params and
	// rewritten aliases rather than against hand-written SQL.
	for _, tc := range cases {
		t.Run("canonical/"+tc.name, func(t *testing.T) {
			canonical, err := Normalize(tc.sql)
			if err != nil {
				t.Fatalf("Normalize(%q): %v", tc.sql, err)
			}
			got, err := Tables(canonical)
			if err != nil {
				t.Fatalf("Tables(canonical %q): %v", canonical, err)
			}
			if !reflect.DeepEqual(tableNames(got), tc.want) {
				t.Fatalf("Tables(canonical %q) = %v, want %v", canonical, tableNames(got), tc.want)
			}
		})
	}
}

// The documented cost of excluding CTE names by name rather than by lexical scope: a
// real table shadowed by a same-named CTE in an unrelated scope is dropped. Pinned so
// the loss is a decision on record rather than a surprise, and so anyone who implements
// real scoping has a test that tells them it changed.
func TestTablesDropsRealTableShadowedByCTEName(t *testing.T) {
	const sql = `SELECT * FROM orders
	             JOIN (WITH orders AS (SELECT $1 AS id) SELECT id FROM orders) s ON s.id = orders.id`
	got, err := Tables(sql)
	if err != nil {
		t.Fatalf("Tables: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Tables = %v; the outer `orders` is expected to be lost to the inner CTE's name."+
			" If this now returns it, scoping was implemented: update this test and the doc on Tables.",
			tableNames(got))
	}
}

// SQL the parser rejects errors rather than returning an empty list, so a caller can
// tell "could not read this query" from "this query references nothing".
//
// A parse error is NOT a truncation detector, and callers must not read it as one:
// pg_stat_statements truncates at track_activity_query_size wherever the byte limit
// lands, which often leaves a prefix that still parses (`... FROM orders JOIN users`
// is valid SQL) and sometimes one that does not. Detecting truncation needs the stored
// length against the setting, on the capture side.
func TestTablesRejectsUnparseable(t *testing.T) {
	if _, err := Tables("SELECT id FROM orders WHERE id IN ($1, $2, $"); err == nil {
		t.Fatal("Tables accepted invalid SQL; want a parse error so callers can mark the shape")
	}
}

// The reflective walk earns its keep by surviving node kinds nobody enumerated, so it
// has to face queries nobody wrote for it. clusters.json is a real captured corpus of
// canonical forms; every one of them is qshape's own deparse, so Tables must read all
// of them without panicking or erroring. Hand-picked cases cannot make this claim,
// which is the same coverage gap that let the FROM-clause walker miss DML targets.
func TestTablesSurvivesCapturedCorpus(t *testing.T) {
	corpus := corpusStatements(t)
	var withTables int
	for _, sql := range corpus {
		tables, err := Tables(sql)
		if err != nil {
			t.Errorf("Tables rejected a corpus statement: %v\n%s", err, sql)
			continue
		}
		if len(tables) > 0 {
			withTables++
		}
		for _, tr := range tables {
			if tr.Name == "" {
				t.Errorf("nameless relation from %q: %+v", sql, tr)
			}
		}
	}
	// A corpus of real application SQL where nothing references a relation would mean
	// the walk silently returns nothing, which is exactly the failure this test exists
	// to catch: no error, no panic, no data.
	if withTables == 0 {
		t.Fatal("nothing in the corpus yielded a table; the walk is finding nothing")
	}
	t.Logf("%d/%d corpus statements reference at least one relation", withTables, len(corpus))
}
