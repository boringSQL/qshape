package qshape

import (
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

const (
	sortHostPrefix   = "SELECT 1 ORDER BY "
	unrenderableSort = "<unrenderable>"
)

type (
	SortVariant struct {
		Fingerprint     string   `json:"fingerprint"`
		SortClauses     []string `json:"sort_clauses,omitempty"`
		Calls           int64    `json:"calls,omitempty"`
		TotalExecTimeMs float64  `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs  float64  `json:"mean_exec_time_ms,omitempty"`
		Rows            int64    `json:"rows,omitempty"`
	}

	SortProfile struct {
		CostKey         string        `json:"cost_key"`
		DistinctSorts   int           `json:"distinct_sorts"`
		Variants        []SortVariant `json:"variants"`
		TotalCalls      int64         `json:"total_calls"`
		TotalExecTimeMs float64       `json:"total_exec_time_ms,omitempty"`
		MeanExecTimeMs  float64       `json:"mean_exec_time_ms,omitempty"`
		Rows            int64         `json:"rows,omitempty"`
	}
)

// SortProfiles groups clusters by CostKey and reports the distinct orderings in each
func SortProfiles(clusters []Cluster) []SortProfile {
	groups := make(map[string]*SortProfile)
	var order []string
	var unkeyed []SortProfile

	for _, c := range clusters {
		v := SortVariant{
			Fingerprint:     c.Fingerprint,
			SortClauses:     sortClausesOf(c.Canonical),
			Calls:           c.TotalCalls,
			TotalExecTimeMs: c.TotalExecTimeMs,
			MeanExecTimeMs:  c.MeanExecTimeMs,
			Rows:            c.Rows,
		}
		key, err := CostKey(c.Canonical)
		if err != nil {
			unkeyed = append(unkeyed, SortProfile{
				DistinctSorts:   1,
				Variants:        []SortVariant{v},
				TotalCalls:      c.TotalCalls,
				TotalExecTimeMs: c.TotalExecTimeMs,
				MeanExecTimeMs:  c.MeanExecTimeMs,
				Rows:            c.Rows,
			})
			continue
		}
		p, ok := groups[key]
		if !ok {
			p = &SortProfile{CostKey: key}
			groups[key] = p
			order = append(order, key)
		}
		p.Variants = append(p.Variants, v)
		p.TotalCalls += c.TotalCalls
		p.TotalExecTimeMs += c.TotalExecTimeMs
		p.Rows += c.Rows
	}

	out := make([]SortProfile, 0, len(groups)+len(unkeyed))
	for _, key := range order {
		p := groups[key]
		if p.TotalCalls > 0 {
			p.MeanExecTimeMs = p.TotalExecTimeMs / float64(p.TotalCalls)
		}
		sortVariants(p.Variants)
		p.DistinctSorts = distinctSorts(p.Variants)
		out = append(out, *p)
	}
	out = append(out, unkeyed...)

	hasTiming := false
	for _, p := range out {
		if p.TotalExecTimeMs > 0 {
			hasTiming = true
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if hasTiming && out[i].TotalExecTimeMs != out[j].TotalExecTimeMs {
			return out[i].TotalExecTimeMs > out[j].TotalExecTimeMs
		}
		if out[i].TotalCalls != out[j].TotalCalls {
			return out[i].TotalCalls > out[j].TotalCalls
		}
		if out[i].CostKey != out[j].CostKey {
			return out[i].CostKey < out[j].CostKey
		}
		return firstFingerprint(out[i]) < firstFingerprint(out[j])
	})
	return out
}

func firstFingerprint(p SortProfile) string {
	if len(p.Variants) == 0 {
		return ""
	}
	return p.Variants[0].Fingerprint
}

func sortVariants(vs []SortVariant) {
	sort.Slice(vs, func(i, j int) bool {
		if vs[i].TotalExecTimeMs != vs[j].TotalExecTimeMs {
			return vs[i].TotalExecTimeMs > vs[j].TotalExecTimeMs
		}
		if vs[i].Calls != vs[j].Calls {
			return vs[i].Calls > vs[j].Calls
		}
		return vs[i].Fingerprint < vs[j].Fingerprint
	})
}

func distinctSorts(vs []SortVariant) int {
	seen := make(map[string]bool, len(vs))
	for _, v := range vs {
		if len(v.SortClauses) == 0 {
			continue
		}
		seen[strings.Join(v.SortClauses, "\x00")] = true
	}
	return len(seen)
}

// sortClausesOf collects the ORDER BY of every SelectStmt in sql, outermost first
func sortClausesOf(sql string) []string {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil
	}
	var out []string
	var walk func(n *pg_query.Node)
	walk = func(n *pg_query.Node) {
		if n == nil {
			return
		}
		if _, ok := n.Node.(*pg_query.Node_SortBy); ok {
			return
		}
		if s, ok := n.Node.(*pg_query.Node_SelectStmt); ok && s.SelectStmt != nil && len(s.SelectStmt.SortClause) > 0 {
			text := deparseSortClause(s.SelectStmt.SortClause)
			if text == "" {
				text = unrenderableSort
			}
			out = append(out, text)
		}
		forEachChild(n, walk)
	}
	for _, raw := range tree.Stmts {
		if raw != nil {
			walk(raw.Stmt)
		}
	}
	return out
}

func deparseSortClause(clause []*pg_query.Node) string {
	host, err := pg_query.Parse("SELECT 1")
	if err != nil || len(host.Stmts) == 0 {
		return ""
	}
	sel := host.Stmts[0].Stmt.GetSelectStmt()
	if sel == nil {
		return ""
	}
	sel.SortClause = clause
	out, err := pg_query.Deparse(host)
	if err != nil {
		return ""
	}
	if !strings.HasPrefix(out, sortHostPrefix) {
		return ""
	}
	return strings.TrimPrefix(out, sortHostPrefix)
}
