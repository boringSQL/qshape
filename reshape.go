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
		walkFixups(raw.Stmt)
		renumberParams(raw.Stmt)
	}

	return nil
}

// renumberParams closes gaps in $N numbering left by fixups that dropped
// ParamRefs (e.g. an EXTRACT field that used to be parameterised). EXPLAIN
// (GENERIC_PLAN) won't plan a query that uses $2 without a $1.
func renumberParams(root *pg_query.Node) {
	used := map[int32]bool{}
	collectParams(root, used)
	if len(used) == 0 {
		return
	}
	nums := make([]int32, 0, len(used))
	for n := range used {
		nums = append(nums, n)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	remap := map[int32]int32{}
	contiguous := true
	for i, n := range nums {
		newN := int32(i + 1)
		if n != newN {
			contiguous = false
		}
		remap[n] = newN
	}
	if contiguous {
		return
	}
	applyParamRemap(root, remap)
}

func collectParams(n *pg_query.Node, out map[int32]bool) {
	if n == nil {
		return
	}
	if p, ok := n.Node.(*pg_query.Node_ParamRef); ok {
		out[p.ParamRef.Number] = true
	}
	forEachChild(n, func(c *pg_query.Node) { collectParams(c, out) })
}

func applyParamRemap(n *pg_query.Node, remap map[int32]int32) {
	if n == nil {
		return
	}
	if p, ok := n.Node.(*pg_query.Node_ParamRef); ok {
		if nn, has := remap[p.ParamRef.Number]; has {
			p.ParamRef.Number = nn
		}
	}
	forEachChild(n, func(c *pg_query.Node) { applyParamRemap(c, remap) })
}

// forEachChild invokes f on every immediate child Node of n that can
// contain a ParamRef. This is intentionally broader than the reshape
// walkers (no scope logic needed here) but still a fixed set of cases —
// new pg_query Node types just mean params inside them won't be renumbered
func forEachChild(n *pg_query.Node, f func(*pg_query.Node)) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_SelectStmt:
		s := v.SelectStmt
		for _, t := range s.TargetList {
			f(t)
		}
		for _, fc := range s.FromClause {
			f(fc)
		}
		f(s.WhereClause)
		f(s.HavingClause)
		for _, g := range s.GroupClause {
			f(g)
		}
		for _, o := range s.SortClause {
			f(o)
		}
		for _, d := range s.DistinctClause {
			f(d)
		}
		f(s.LimitOffset)
		f(s.LimitCount)
		for _, v := range s.ValuesLists {
			f(v)
		}
		if s.WithClause != nil {
			for _, cte := range s.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					f(c.CommonTableExpr.Ctequery)
				}
			}
		}
		if s.Larg != nil {
			f(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Larg}})
		}
		if s.Rarg != nil {
			f(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Rarg}})
		}
	case *pg_query.Node_UpdateStmt:
		u := v.UpdateStmt
		for _, t := range u.TargetList {
			f(t)
		}
		f(u.WhereClause)
		for _, fc := range u.FromClause {
			f(fc)
		}
		for _, r := range u.ReturningList {
			f(r)
		}
		if u.WithClause != nil {
			for _, cte := range u.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					f(c.CommonTableExpr.Ctequery)
				}
			}
		}
	case *pg_query.Node_DeleteStmt:
		d := v.DeleteStmt
		f(d.WhereClause)
		for _, u := range d.UsingClause {
			f(u)
		}
		for _, r := range d.ReturningList {
			f(r)
		}
		if d.WithClause != nil {
			for _, cte := range d.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					f(c.CommonTableExpr.Ctequery)
				}
			}
		}
	case *pg_query.Node_InsertStmt:
		i := v.InsertStmt
		f(i.SelectStmt)
		for _, r := range i.ReturningList {
			f(r)
		}
		if i.WithClause != nil {
			for _, cte := range i.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					f(c.CommonTableExpr.Ctequery)
				}
			}
		}
	case *pg_query.Node_FuncCall:
		for _, a := range v.FuncCall.Args {
			f(a)
		}
	case *pg_query.Node_ResTarget:
		f(v.ResTarget.Val)
	case *pg_query.Node_AExpr:
		f(v.AExpr.Lexpr)
		f(v.AExpr.Rexpr)
	case *pg_query.Node_BoolExpr:
		for _, a := range v.BoolExpr.Args {
			f(a)
		}
	case *pg_query.Node_TypeCast:
		f(v.TypeCast.Arg)
	case *pg_query.Node_List:
		for _, a := range v.List.Items {
			f(a)
		}
	case *pg_query.Node_NullTest:
		f(v.NullTest.Arg)
	case *pg_query.Node_BooleanTest:
		f(v.BooleanTest.Arg)
	case *pg_query.Node_CaseExpr:
		f(v.CaseExpr.Arg)
		f(v.CaseExpr.Defresult)
		for _, a := range v.CaseExpr.Args {
			f(a)
		}
	case *pg_query.Node_CaseWhen:
		f(v.CaseWhen.Expr)
		f(v.CaseWhen.Result)
	case *pg_query.Node_SubLink:
		f(v.SubLink.Subselect)
		f(v.SubLink.Testexpr)
	case *pg_query.Node_JoinExpr:
		f(v.JoinExpr.Larg)
		f(v.JoinExpr.Rarg)
		f(v.JoinExpr.Quals)
	case *pg_query.Node_RangeSubselect:
		f(v.RangeSubselect.Subquery)
	case *pg_query.Node_SortBy:
		f(v.SortBy.Node)
	case *pg_query.Node_CoalesceExpr:
		for _, a := range v.CoalesceExpr.Args {
			f(a)
		}
	case *pg_query.Node_MinMaxExpr:
		for _, a := range v.MinMaxExpr.Args {
			f(a)
		}
	}
}

// walkFixups walks the whole tree applying AST-level deparse fixups that are
// independent of scope/alias rewriting
func walkFixups(n *pg_query.Node) {
	if n == nil {
		return
	}
	switch v := n.Node.(type) {
	case *pg_query.Node_FuncCall:
		fixExtractFieldIdent(v.FuncCall)
		for _, a := range v.FuncCall.Args {
			walkFixups(a)
		}
	case *pg_query.Node_SelectStmt:
		s := v.SelectStmt
		for _, t := range s.TargetList {
			walkFixups(t)
		}
		for _, f := range s.FromClause {
			walkFixups(f)
		}
		walkFixups(s.WhereClause)
		walkFixups(s.HavingClause)
		for _, g := range s.GroupClause {
			walkFixups(g)
		}
		for _, o := range s.SortClause {
			walkFixups(o)
		}
		walkFixups(s.LimitOffset)
		walkFixups(s.LimitCount)
		if s.WithClause != nil {
			for _, cte := range s.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					walkFixups(c.CommonTableExpr.Ctequery)
				}
			}
		}
		if s.Larg != nil {
			walkFixups(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Larg}})
		}
		if s.Rarg != nil {
			walkFixups(&pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s.Rarg}})
		}
	case *pg_query.Node_UpdateStmt:
		u := v.UpdateStmt
		for _, t := range u.TargetList {
			walkFixups(t)
		}
		walkFixups(u.WhereClause)
		for _, f := range u.FromClause {
			walkFixups(f)
		}
		for _, r := range u.ReturningList {
			walkFixups(r)
		}
		if u.WithClause != nil {
			for _, cte := range u.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					walkFixups(c.CommonTableExpr.Ctequery)
				}
			}
		}
	case *pg_query.Node_DeleteStmt:
		d := v.DeleteStmt
		walkFixups(d.WhereClause)
		for _, u := range d.UsingClause {
			walkFixups(u)
		}
		for _, r := range d.ReturningList {
			walkFixups(r)
		}
		if d.WithClause != nil {
			for _, cte := range d.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					walkFixups(c.CommonTableExpr.Ctequery)
				}
			}
		}
	case *pg_query.Node_InsertStmt:
		i := v.InsertStmt
		walkFixups(i.SelectStmt)
		for _, r := range i.ReturningList {
			walkFixups(r)
		}
		if i.WithClause != nil {
			for _, cte := range i.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					walkFixups(c.CommonTableExpr.Ctequery)
				}
			}
		}
	case *pg_query.Node_ResTarget:
		walkFixups(v.ResTarget.Val)
	case *pg_query.Node_AExpr:
		walkFixups(v.AExpr.Lexpr)
		walkFixups(v.AExpr.Rexpr)
	case *pg_query.Node_BoolExpr:
		for _, a := range v.BoolExpr.Args {
			walkFixups(a)
		}
	case *pg_query.Node_TypeCast:
		walkFixups(v.TypeCast.Arg)
	case *pg_query.Node_List:
		for _, a := range v.List.Items {
			walkFixups(a)
		}
	case *pg_query.Node_NullTest:
		walkFixups(v.NullTest.Arg)
	case *pg_query.Node_BooleanTest:
		walkFixups(v.BooleanTest.Arg)
	case *pg_query.Node_CoalesceExpr:
		for _, a := range v.CoalesceExpr.Args {
			walkFixups(a)
		}
	case *pg_query.Node_MinMaxExpr:
		for _, a := range v.MinMaxExpr.Args {
			walkFixups(a)
		}
	case *pg_query.Node_AArrayExpr:
		for _, e := range v.AArrayExpr.Elements {
			walkFixups(e)
		}
	case *pg_query.Node_RowExpr:
		for _, a := range v.RowExpr.Args {
			walkFixups(a)
		}
	case *pg_query.Node_SubLink:
		walkFixups(v.SubLink.Subselect)
		walkFixups(v.SubLink.Testexpr)
	case *pg_query.Node_JoinExpr:
		walkFixups(v.JoinExpr.Larg)
		walkFixups(v.JoinExpr.Rarg)
		walkFixups(v.JoinExpr.Quals)
	case *pg_query.Node_RangeSubselect:
		walkFixups(v.RangeSubselect.Subquery)
	case *pg_query.Node_RangeFunction:
		for _, fn := range v.RangeFunction.Functions {
			walkFixups(fn)
		}
	case *pg_query.Node_CaseExpr:
		walkFixups(v.CaseExpr.Arg)
		walkFixups(v.CaseExpr.Defresult)
		for _, a := range v.CaseExpr.Args {
			walkFixups(a)
		}
	case *pg_query.Node_CaseWhen:
		walkFixups(v.CaseWhen.Expr)
		walkFixups(v.CaseWhen.Result)
	case *pg_query.Node_SortBy:
		walkFixups(v.SortBy.Node)
	}
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

// isPgCatalogExtract reports whether the FuncCall is pg_catalog.extract,
// the internal form of EXTRACT(field FROM source)
func isPgCatalogExtract(fc *pg_query.FuncCall) bool {
	if fc == nil || len(fc.Funcname) != 2 {
		return false
	}
	s0, ok := fc.Funcname[0].Node.(*pg_query.Node_String_)
	if !ok || s0.String_.Sval != "pg_catalog" {
		return false
	}
	s1, ok := fc.Funcname[1].Node.(*pg_query.Node_String_)
	if !ok || s1.String_.Sval != "extract" {
		return false
	}
	return fc.Funcformat == pg_query.CoercionForm_COERCE_SQL_SYNTAX
}

// fixExtractFieldIdent rewrites the first arg of pg_catalog.extract from an
// A_Const(String) to a ColumnRef. The parser canonicalises `EXTRACT(epoch FROM x)`
// to the string-constant form, and the deparser renders that as `'epoch'`.
// Downstream tools that parameterise literals (pg_stat_statements-style)
// then rewrite it to `$N`, producing invalid SQL. A bare identifier survives
// both round-trips.
func fixExtractFieldIdent(fc *pg_query.FuncCall) {
	if !isPgCatalogExtract(fc) || len(fc.Args) == 0 {
		return
	}
	var ident string
	switch a := fc.Args[0].Node.(type) {
	case *pg_query.Node_AConst:
		sv, ok := a.AConst.Val.(*pg_query.A_Const_Sval)
		if !ok {
			return
		}
		ident = sv.Sval.Sval
	case *pg_query.Node_ParamRef:
		// Upstream parameterisers (pg_stat_statements) sometimes replace the
		// field name with $N; that form is not executable. Substitute a
		// stable identifier so EXPLAIN can parse the canonical.
		ident = "epoch"
	default:
		return
	}
	fc.Args[0] = &pg_query.Node{Node: &pg_query.Node_ColumnRef{ColumnRef: &pg_query.ColumnRef{
		Fields: []*pg_query.Node{
			{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: ident}}},
		},
	}}}
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
		rewriteRefs(v.FuncCall.AggFilter, dec)
		for _, a := range v.FuncCall.AggOrder {
			rewriteRefs(a, dec)
		}
	case *pg_query.Node_CoalesceExpr:
		for _, a := range v.CoalesceExpr.Args {
			rewriteRefs(a, dec)
		}
	case *pg_query.Node_MinMaxExpr:
		for _, a := range v.MinMaxExpr.Args {
			rewriteRefs(a, dec)
		}
	case *pg_query.Node_AArrayExpr:
		for _, e := range v.AArrayExpr.Elements {
			rewriteRefs(e, dec)
		}
	case *pg_query.Node_RowExpr:
		for _, a := range v.RowExpr.Args {
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
	case *pg_query.Node_RangeSubselect:
		// LATERAL subqueries may reference outer-scope aliases; rewrite
		// correlated refs inside. The subquery's own scope was handled by
		// its own reshapeSelect pass first, so any `u.x` still present
		// here is an outer-scope reference
		if sub, ok := v.RangeSubselect.Subquery.Node.(*pg_query.Node_SelectStmt); ok {
			rewriteInSelect(sub.SelectStmt, dec)
		}
	}
}

// rewriteInSelect rewrites correlated refs to outer-scope aliases inside
// a nested SelectStmt. FROM items are walked too: the inner scope's own
// reshape pass already handled its own aliases, so any remaining `u.x`
// is necessarily an outer-scope reference (e.g. in a JoinExpr.Quals or a
// derived-table subquery's WHERE)
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

	for _, n := range s.FromClause {
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
