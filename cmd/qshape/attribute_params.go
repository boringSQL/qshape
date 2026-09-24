package main

import (
	"sort"

	"github.com/boringsql/qshape"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// paramPositions returns the distinct $N positions in canonical, ascending.
// WalkNodes covers every node type; an unparsable canonical falls back to
// scanning $N tokens.
func paramPositions(canonical string) []int {
	tree, err := pg_query.Parse(canonical)
	if err != nil {
		return fallbackPositions(canonical)
	}
	seen := map[int]bool{}
	for _, raw := range tree.Stmts {
		if raw == nil || raw.Stmt == nil {
			continue
		}
		qshape.WalkNodes(raw.Stmt, func(n *pg_query.Node) {
			if p, ok := n.Node.(*pg_query.Node_ParamRef); ok {
				seen[int(p.ParamRef.Number)] = true
			}
		})
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

func fallbackPositions(canonical string) []int {
	max := maxParamNumber(canonical)
	if max <= 0 {
		return nil
	}
	out := make([]int, max)
	for i := range out {
		out[i] = i + 1
	}
	return out
}

// fillPositions returns one entry per canonical position, in order. A matched
// entry outside positions (an internal InitPlan/SubPlan param) is dropped.
func fillPositions(matched map[int]*qshape.ParamAttribution, positions []int) []qshape.ParamAttribution {
	out := make([]qshape.ParamAttribution, 0, len(positions))
	for _, pos := range positions {
		if a, ok := matched[pos]; ok {
			out = append(out, *a)
			continue
		}
		out = append(out, qshape.ParamAttribution{Position: pos, Confidence: "none"})
	}
	return out
}

func noneEntries(positions []int, note string) []qshape.ParamAttribution {
	out := make([]qshape.ParamAttribution, 0, len(positions))
	for _, pos := range positions {
		out = append(out, qshape.ParamAttribution{Position: pos, Confidence: "none", Note: note})
	}
	return out
}
