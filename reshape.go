package qshape

import (
	"sort"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// reshape rewrites a parsed tree so that decorative table aliases are
// stripped and sort AND conditions
func reshape(tree *pg_query.ParseResult) error {
	if tree == nil {
		return nil
	}

	for _, raw := range tree.Stmts {
		if raw == nil || raw.Stmt == nil {
			continue
		}
		reshapeNode(raw.Stmt)
	}

	return nil
}

func reshapeNode(n *pg_query.Node) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_SelectStmt:
		reshapeSelect(v.SelectStmt)
	case *pg_query.Node_UpdateStmt:
		reshapeUpdate(v.UpdateStmt)
	case *pg_query.Node_DeleteStmt:
		reshapeDelete(v.DeleteStmt)
	}
}

type scopeEntry struct {
	relname   string
	aliasName string
	required  bool
}

func reshapeSelect(s *pg_query.SelectStmt) {
	if s == nil {
		return
	}

	// Recurse into nested scopes first so they reshape themselves
	reshapeNestedFrom(s.FromClause)
	reshapeWithClause(s.WithClause)
	reshapeSelect(s.Larg)
	reshapeSelect(s.Rarg)
	reshapeSublinksInSelect(s)

	// This scope
	entries := collectScope(s.FromClause)
	dec := decorativeAliases(entries)
	if len(dec) > 0 {
		stripAliasesInFrom(s.FromClause, dec)
		for _, n := range s.TargetList {
			rewriteRefs(n, dec)
		}
		rewriteRefs(s.WhereClause, dec)
		rewriteRefs(s.HavingClause, dec)
		for _, n := range s.GroupClause {
			rewriteRefs(n, dec)
		}
		for _, n := range s.SortClause {
			rewriteRefs(n, dec)
		}
		for _, n := range s.DistinctClause {
			rewriteRefs(n, dec)
		}
		rewriteRefs(s.LimitOffset, dec)
		rewriteRefs(s.LimitCount, dec)
		// JoinExpr.Quals live inside FROM clause nodes
		for _, n := range s.FromClause {
			rewriteRefs(n, dec)
		}
	}

	sortAndTree(s.WhereClause)
	sortAndTree(s.HavingClause)
}

func reshapeUpdate(u *pg_query.UpdateStmt) {
	if u == nil {
		return
	}

	reshapeWithClause(u.WithClause)
	reshapeNestedFrom(u.FromClause)
	for _, n := range u.TargetList {
		reshapeSublinks(n)
	}
	reshapeSublinks(u.WhereClause)
	for _, n := range u.ReturningList {
		reshapeSublinks(n)
	}

	var entries []scopeEntry
	if u.Relation != nil {
		entries = append(entries, rangeVarEntry(u.Relation))
	}
	for _, n := range u.FromClause {
		collectFromItem(n, &entries)
	}
	dec := decorativeAliases(entries)
	if len(dec) > 0 {
		if u.Relation != nil && u.Relation.Alias != nil {
			if _, ok := dec[u.Relation.Alias.Aliasname]; ok {
				u.Relation.Alias = nil
			}
		}
		stripAliasesInFrom(u.FromClause, dec)
		for _, n := range u.TargetList {
			rewriteRefs(n, dec)
		}
		rewriteRefs(u.WhereClause, dec)
		for _, n := range u.ReturningList {
			rewriteRefs(n, dec)
		}
		for _, n := range u.FromClause {
			rewriteRefs(n, dec)
		}
	}

	sortAndTree(u.WhereClause)
}

func reshapeDelete(d *pg_query.DeleteStmt) {
	if d == nil {
		return
	}

	reshapeWithClause(d.WithClause)
	reshapeNestedFrom(d.UsingClause)
	reshapeSublinks(d.WhereClause)
	for _, n := range d.ReturningList {
		reshapeSublinks(n)
	}

	var entries []scopeEntry
	if d.Relation != nil {
		entries = append(entries, rangeVarEntry(d.Relation))
	}
	for _, n := range d.UsingClause {
		collectFromItem(n, &entries)
	}
	dec := decorativeAliases(entries)
	if len(dec) > 0 {
		if d.Relation != nil && d.Relation.Alias != nil {
			if _, ok := dec[d.Relation.Alias.Aliasname]; ok {
				d.Relation.Alias = nil
			}
		}
		stripAliasesInFrom(d.UsingClause, dec)
		rewriteRefs(d.WhereClause, dec)
		for _, n := range d.ReturningList {
			rewriteRefs(n, dec)
		}
		for _, n := range d.UsingClause {
			rewriteRefs(n, dec)
		}
	}

	sortAndTree(d.WhereClause)
}

func reshapeWithClause(w *pg_query.WithClause) {
	if w == nil {
		return
	}
	for _, cte := range w.Ctes {
		if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
			reshapeNode(c.CommonTableExpr.Ctequery)
		}
	}
}

func reshapeNestedFrom(items []*pg_query.Node) {
	for _, n := range items {
		reshapeNestedFromItem(n)
	}
}

func reshapeNestedFromItem(n *pg_query.Node) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_RangeSubselect:
		reshapeNode(v.RangeSubselect.Subquery)
	case *pg_query.Node_JoinExpr:
		reshapeNestedFromItem(v.JoinExpr.Larg)
		reshapeNestedFromItem(v.JoinExpr.Rarg)
	}
}

func reshapeSublinksInSelect(s *pg_query.SelectStmt) {
	reshapeSublinks(s.WhereClause)
	reshapeSublinks(s.HavingClause)

	for _, n := range s.TargetList {
		reshapeSublinks(n)
	}
	for _, n := range s.FromClause {
		reshapeJoinQualsSublinks(n)
	}
}

func reshapeJoinQualsSublinks(n *pg_query.Node) {
	if n == nil {
		return
	}
	if v, ok := n.Node.(*pg_query.Node_JoinExpr); ok {
		reshapeSublinks(v.JoinExpr.Quals)
		reshapeJoinQualsSublinks(v.JoinExpr.Larg)
		reshapeJoinQualsSublinks(v.JoinExpr.Rarg)
	}
}

func reshapeSublinks(n *pg_query.Node) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_SubLink:
		reshapeNode(v.SubLink.Subselect)
		reshapeSublinks(v.SubLink.Testexpr)
	case *pg_query.Node_AExpr:
		reshapeSublinks(v.AExpr.Lexpr)
		reshapeSublinks(v.AExpr.Rexpr)
	case *pg_query.Node_BoolExpr:
		for _, a := range v.BoolExpr.Args {
			reshapeSublinks(a)
		}
	case *pg_query.Node_FuncCall:
		for _, a := range v.FuncCall.Args {
			reshapeSublinks(a)
		}
	case *pg_query.Node_ResTarget:
		reshapeSublinks(v.ResTarget.Val)
	case *pg_query.Node_TypeCast:
		reshapeSublinks(v.TypeCast.Arg)
	case *pg_query.Node_List:
		for _, a := range v.List.Items {
			reshapeSublinks(a)
		}
	case *pg_query.Node_NullTest:
		reshapeSublinks(v.NullTest.Arg)
	case *pg_query.Node_BooleanTest:
		reshapeSublinks(v.BooleanTest.Arg)
	case *pg_query.Node_CaseExpr:
		reshapeSublinks(v.CaseExpr.Arg)
		reshapeSublinks(v.CaseExpr.Defresult)
		for _, a := range v.CaseExpr.Args {
			reshapeSublinks(a)
		}
	case *pg_query.Node_CaseWhen:
		reshapeSublinks(v.CaseWhen.Expr)
		reshapeSublinks(v.CaseWhen.Result)
	case *pg_query.Node_SortBy:
		reshapeSublinks(v.SortBy.Node)
	}
}

// collectScope gathers RangeVar-like entries from a FROM list without
// descending into sub-select scopes
func collectScope(items []*pg_query.Node) []scopeEntry {
	var out []scopeEntry
	for _, n := range items {
		collectFromItem(n, &out)
	}
	return out
}

func collectFromItem(n *pg_query.Node, out *[]scopeEntry) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_RangeVar:
		*out = append(*out, rangeVarEntry(v.RangeVar))
	case *pg_query.Node_JoinExpr:
		collectFromItem(v.JoinExpr.Larg, out)
		collectFromItem(v.JoinExpr.Rarg, out)
	case *pg_query.Node_RangeSubselect:
		alias := ""
		if v.RangeSubselect.Alias != nil {
			alias = v.RangeSubselect.Alias.Aliasname
		}
		*out = append(*out, scopeEntry{aliasName: alias, required: true})
	case *pg_query.Node_RangeFunction:
		alias := ""
		if v.RangeFunction.Alias != nil {
			alias = v.RangeFunction.Alias.Aliasname
		}
		*out = append(*out, scopeEntry{aliasName: alias, required: true})
	}
}

func rangeVarEntry(rv *pg_query.RangeVar) scopeEntry {
	alias := ""
	if rv.Alias != nil && len(rv.Alias.Colnames) == 0 {
		alias = rv.Alias.Aliasname
	}
	return scopeEntry{relname: rv.Relname, aliasName: alias}
}

// decorativeAliases maps each strippable alias to its replacement:
// nil for single-table scopes (drop), relname for multi-table scopes
// (rewrite `u.col` to `users.col`)
func decorativeAliases(entries []scopeEntry) map[string][]*pg_query.Node {
	relCount := map[string]int{}
	for _, e := range entries {
		if e.relname != "" {
			relCount[e.relname]++
		}
	}

	singleRelation := len(entries) == 1

	out := map[string][]*pg_query.Node{}
	for _, e := range entries {
		if e.aliasName == "" || e.required {
			continue
		}
		if e.relname != "" && relCount[e.relname] > 1 {
			continue
		}
		if singleRelation {
			out[e.aliasName] = nil
		} else if e.relname != "" {
			out[e.aliasName] = []*pg_query.Node{stringNode(e.relname)}
		}
	}
	return out
}

func stringNode(s string) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: s}}}
}

func stripAliasesInFrom(items []*pg_query.Node, dec map[string][]*pg_query.Node) {
	for _, n := range items {
		stripAliasInFromItem(n, dec)
	}
}

func stripAliasInFromItem(n *pg_query.Node, dec map[string][]*pg_query.Node) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_RangeVar:
		if v.RangeVar.Alias != nil {
			if _, ok := dec[v.RangeVar.Alias.Aliasname]; ok {
				v.RangeVar.Alias = nil
			}
		}
	case *pg_query.Node_JoinExpr:
		stripAliasInFromItem(v.JoinExpr.Larg, dec)
		stripAliasInFromItem(v.JoinExpr.Rarg, dec)
	}
}

// rewriteRefs rewrites ColumnRefs whose leading field is a decorative
// alias, splicing in the replacement from dec
func rewriteRefs(n *pg_query.Node, dec map[string][]*pg_query.Node) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_ColumnRef:
		if len(v.ColumnRef.Fields) >= 2 {
			if s, ok := v.ColumnRef.Fields[0].Node.(*pg_query.Node_String_); ok {
				if repl, present := dec[s.String_.Sval]; present {
					rest := v.ColumnRef.Fields[1:]
					v.ColumnRef.Fields = append(append([]*pg_query.Node{}, repl...), rest...)
				}
			}
		}
	case *pg_query.Node_AExpr:
		rewriteRefs(v.AExpr.Lexpr, dec)
		rewriteRefs(v.AExpr.Rexpr, dec)
	case *pg_query.Node_BoolExpr:
		for _, a := range v.BoolExpr.Args {
			rewriteRefs(a, dec)
		}
	case *pg_query.Node_ResTarget:
		rewriteRefs(v.ResTarget.Val, dec)
	case *pg_query.Node_FuncCall:
		for _, a := range v.FuncCall.Args {
			rewriteRefs(a, dec)
		}
	case *pg_query.Node_List:
		for _, a := range v.List.Items {
			rewriteRefs(a, dec)
		}
	case *pg_query.Node_SortBy:
		rewriteRefs(v.SortBy.Node, dec)
	case *pg_query.Node_TypeCast:
		rewriteRefs(v.TypeCast.Arg, dec)
	case *pg_query.Node_NullTest:
		rewriteRefs(v.NullTest.Arg, dec)
	case *pg_query.Node_BooleanTest:
		rewriteRefs(v.BooleanTest.Arg, dec)
	case *pg_query.Node_CaseExpr:
		rewriteRefs(v.CaseExpr.Arg, dec)
		rewriteRefs(v.CaseExpr.Defresult, dec)
		for _, w := range v.CaseExpr.Args {
			rewriteRefs(w, dec)
		}
	case *pg_query.Node_CaseWhen:
		rewriteRefs(v.CaseWhen.Expr, dec)
		rewriteRefs(v.CaseWhen.Result, dec)
	case *pg_query.Node_SubLink:
		if sub, ok := v.SubLink.Subselect.Node.(*pg_query.Node_SelectStmt); ok {
			rewriteInSelect(sub.SelectStmt, dec)
		}
		rewriteRefs(v.SubLink.Testexpr, dec)
	case *pg_query.Node_JoinExpr:
		rewriteRefs(v.JoinExpr.Quals, dec)
		rewriteRefs(v.JoinExpr.Larg, dec)
		rewriteRefs(v.JoinExpr.Rarg, dec)
	}
}

// rewriteInSelect rewrites correlated refs inside a nested SelectStmt
// but skips its FROM clause (its own aliases are handled separately)
func rewriteInSelect(s *pg_query.SelectStmt, dec map[string][]*pg_query.Node) {
	if s == nil {
		return
	}

	for _, n := range s.TargetList {
		rewriteRefs(n, dec)
	}

	rewriteRefs(s.WhereClause, dec)
	rewriteRefs(s.HavingClause, dec)

	for _, n := range s.GroupClause {
		rewriteRefs(n, dec)
	}

	for _, n := range s.SortClause {
		rewriteRefs(n, dec)
	}

	for _, n := range s.DistinctClause {
		rewriteRefs(n, dec)
	}
}

func sortAndTree(n *pg_query.Node) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_BoolExpr:
		for _, a := range v.BoolExpr.Args {
			sortAndTree(a)
		}
		if v.BoolExpr.Boolop == pg_query.BoolExprType_AND_EXPR {
			sort.SliceStable(v.BoolExpr.Args, func(i, j int) bool {
				return argSortKey(v.BoolExpr.Args[i]) < argSortKey(v.BoolExpr.Args[j])
			})
		}
	case *pg_query.Node_AExpr:
		sortAndTree(v.AExpr.Lexpr)
		sortAndTree(v.AExpr.Rexpr)
	}
}

func argSortKey(n *pg_query.Node) string {
	if n == nil {
		return ""
	}
	b, err := proto.Marshal(n)
	if err != nil {
		return ""
	}
	return string(b)
}
