package main

import (
	"sort"

	"github.com/boringsql/qshape"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// paramsFromTree returns the distinct $N positions ascending plus LIMIT/OFFSET
// kinds; an unparsable canonical falls back to scanning $N tokens.
func paramsFromTree(canonical string) (positions []int, limits map[int]string) {
	tree, err := pg_query.Parse(canonical)
	if err != nil {
		return fallbackPositions(canonical), nil
	}
	seen := map[int]bool{}
	limits = map[int]string{}
	for _, raw := range tree.Stmts {
		if raw == nil || raw.Stmt == nil {
			continue
		}
		qshape.WalkNodes(raw.Stmt, func(n *pg_query.Node) {
			switch v := n.Node.(type) {
			case *pg_query.Node_ParamRef:
				seen[int(v.ParamRef.Number)] = true
			case *pg_query.Node_SelectStmt:
				selectLimits(v.SelectStmt, limits)
			}
		})
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Ints(out)
	return out, limits
}

// selectLimits records LIMIT/OFFSET params of s and its set-op arms, which
// WalkNodes does not visit. LIMIT wins a shared $N.
func selectLimits(s *pg_query.SelectStmt, limits map[int]string) {
	if s == nil {
		return
	}
	if n, ok := limitParam(s.LimitOffset); ok {
		if _, dup := limits[n]; !dup {
			limits[n] = "offset"
		}
	}
	if n, ok := limitParam(s.LimitCount); ok {
		limits[n] = "limit"
	}
	selectLimits(s.Larg, limits)
	selectLimits(s.Rarg, limits)
}

// limitParam unwraps casts and returns the $N of a bare param.
func limitParam(n *pg_query.Node) (int, bool) {
	for tc := n.GetTypeCast(); tc != nil; tc = n.GetTypeCast() {
		n = tc.Arg
	}
	if p := n.GetParamRef(); p != nil {
		return int(p.Number), true
	}
	return 0, false
}

// markLimits overwrites every LIMIT/OFFSET entry from the parse tree, keeping
// any EXPLAIN error note.
func markLimits(params []qshape.ParamAttribution, limits map[int]string) {
	for i, p := range params {
		kind, ok := limits[p.Position]
		if !ok {
			continue
		}
		a := qshape.ParamAttribution{Position: p.Position, Kind: kind, Confidence: "exact"}
		if p.Confidence == "none" {
			a.Note = p.Note
		}
		params[i] = a
	}
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
