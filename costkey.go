package qshape

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// CostKeyPrefix marks a sort-insensitive cost key. Deliberately not "sha1:"
// so joining a cost key against a fingerprint column returns empty rather
// than a false match. Own version digit, independent of GroupingVersion.
const CostKeyPrefix = "qsort1:"

// costKeyFallbackPrefix marks a key that fell back to the fine fingerprint.
const costKeyFallbackPrefix = CostKeyPrefix + "fp:"

// CostKey groups sort permutations of one statement under a single key for
// cost attribution. Attribution only: never EXPLAIN it, never use it as a
// Fingerprint — grouped members differ in sort order, which decides index
// column order and direction.
//
// Same reshaped tree and fingerprint as Fingerprint, with statement-level
// ORDER BY dropped, so the key is never finer than Fingerprint. Aggregate
// and window ORDER BY survive (forEachChild does not reach them); LIMIT/
// OFFSET are kept (top-N heapsort vs full sort is a real plan difference).
// The deparsed text is a hashing input only — its $N positions no longer
// line up with the fine canonical, so it is never returned or planned.
func CostKey(sql string) (string, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return fallbackCostKey(sql)
	}
	if err := reshape(tree); err != nil {
		return fallbackCostKey(sql)
	}
	for _, raw := range tree.Stmts {
		if raw != nil {
			stripSortClause(raw.Stmt)
		}
	}
	stripped, err := pg_query.Deparse(tree)
	if err != nil {
		return fallbackCostKey(sql)
	}
	fp, err := pg_query.Fingerprint(stripped)
	if err != nil {
		return fallbackCostKey(sql)
	}
	return CostKeyPrefix + fp, nil
}

func fallbackCostKey(sql string) (string, error) {
	fp, err := pg_query.Fingerprint(sql)
	if err != nil {
		return "", err
	}
	return costKeyFallbackPrefix + fp, nil
}

// stripSortClause clears statement-level ORDER BY everywhere forEachChild
// reaches. Statement types it misses (DECLARE CURSOR, CREATE TABLE AS, COPY,
// MERGE, ...) keep their ORDER BY and fragment — a miss costs a merge, never
// a split.
func stripSortClause(n *pg_query.Node) {
	if n == nil {
		return
	}
	if s, ok := n.Node.(*pg_query.Node_SelectStmt); ok && s.SelectStmt != nil {
		s.SelectStmt.SortClause = nil
	}
	forEachChild(n, stripSortClause)
}
