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
		if v, ok := raw.Stmt.Node.(*pg_query.Node_SelectStmt); ok {
			reshapeSelect(v.SelectStmt)
		}
	}

	return nil
}

func reshapeSelect(s *pg_query.SelectStmt) {
	if s == nil {
		return
	}

	// Single-table FROM: strip a decorative table alias
	if len(s.FromClause) == 1 {
		if rv, ok := s.FromClause[0].Node.(*pg_query.Node_RangeVar); ok {
			if rv.RangeVar.Alias != nil && rv.RangeVar.Alias.Aliasname != "" &&
				len(rv.RangeVar.Alias.Colnames) == 0 {
				alias := rv.RangeVar.Alias.Aliasname
				rv.RangeVar.Alias = nil
				stripAliasInSelect(s, alias)
			}
		}
	}

	sortAndTree(s.WhereClause)
	sortAndTree(s.HavingClause)
}

func stripAliasInSelect(s *pg_query.SelectStmt, alias string) {
	for _, n := range s.TargetList {
		stripAliasInNode(n, alias)
	}

	stripAliasInNode(s.WhereClause, alias)
	stripAliasInNode(s.HavingClause, alias)

	for _, n := range s.GroupClause {
		stripAliasInNode(n, alias)
	}

	for _, n := range s.SortClause {
		stripAliasInNode(n, alias)
	}

	for _, n := range s.DistinctClause {
		stripAliasInNode(n, alias)
	}
}

func stripAliasInNode(n *pg_query.Node, alias string) {
	if n == nil {
		return
	}

	switch v := n.Node.(type) {
	case *pg_query.Node_ColumnRef:
		if len(v.ColumnRef.Fields) >= 2 {
			if s, ok := v.ColumnRef.Fields[0].Node.(*pg_query.Node_String_); ok && s.String_.Sval == alias {
				v.ColumnRef.Fields = v.ColumnRef.Fields[1:]
			}
		}
	case *pg_query.Node_AExpr:
		stripAliasInNode(v.AExpr.Lexpr, alias)
		stripAliasInNode(v.AExpr.Rexpr, alias)
	case *pg_query.Node_BoolExpr:
		for _, a := range v.BoolExpr.Args {
			stripAliasInNode(a, alias)
		}
	case *pg_query.Node_ResTarget:
		stripAliasInNode(v.ResTarget.Val, alias)
	case *pg_query.Node_FuncCall:
		for _, a := range v.FuncCall.Args {
			stripAliasInNode(a, alias)
		}
	case *pg_query.Node_List:
		for _, a := range v.List.Items {
			stripAliasInNode(a, alias)
		}
	case *pg_query.Node_SortBy:
		stripAliasInNode(v.SortBy.Node, alias)
	case *pg_query.Node_TypeCast:
		stripAliasInNode(v.TypeCast.Arg, alias)
	case *pg_query.Node_NullTest:
		stripAliasInNode(v.NullTest.Arg, alias)
	case *pg_query.Node_BooleanTest:
		stripAliasInNode(v.BooleanTest.Arg, alias)
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
