package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// typecastCache caches pg_proc lookups keyed by schema|name|arity. A nil
// value means "not resolvable" (unknown, ambiguous overload, or lookup
// error) — cached so we don't re-query per cluster.
type typecastCache struct {
	conn  *pgx.Conn
	cache map[string][]string
}

func newTypecastCache(conn *pgx.Conn) *typecastCache {
	return &typecastCache{conn: conn, cache: map[string][]string{}}
}

func (c *typecastCache) lookup(ctx context.Context, schema, name string, nargs int) []string {
	key := fmt.Sprintf("%s|%s|%d", schema, name, nargs)
	if v, ok := c.cache[key]; ok {
		return v
	}
	types := c.query(ctx, schema, name, nargs)
	c.cache[key] = types
	return types
}

// query returns the argument type names for the function if there's a
// single unambiguous match, otherwise nil. Type names are formatted as
// `typname` for pg_catalog types and `nspname.typname` otherwise.
func (c *typecastCache) query(ctx context.Context, schema, name string, nargs int) []string {
	if c.conn == nil {
		return nil
	}
	const sql = `
SELECT array_agg(
  CASE WHEN tn.nspname = 'pg_catalog' THEN t.typname
       ELSE tn.nspname || '.' || t.typname END
  ORDER BY a.ord)
FROM pg_proc p
JOIN pg_namespace pn ON pn.oid = p.pronamespace
JOIN LATERAL unnest(p.proargtypes) WITH ORDINALITY AS a(oid, ord) ON true
JOIN pg_type t ON t.oid = a.oid
JOIN pg_namespace tn ON tn.oid = t.typnamespace
WHERE p.proname = $2
  AND ($1 = '' OR pn.nspname = $1)
  AND p.pronargs = $3
GROUP BY p.oid`

	rows, err := c.conn.Query(ctx, sql, schema, name, nargs)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var matches [][]string
	for rows.Next() {
		var types []string
		if err := rows.Scan(&types); err != nil {
			return nil
		}
		matches = append(matches, types)
	}
	if err := rows.Err(); err != nil {
		return nil
	}
	if len(matches) != 1 {
		// zero: unknown function; >1: overloaded, can't pick
		return nil
	}
	return matches[0]
}

// castFuncParamRefs parses canonical, wraps every FuncCall ParamRef arg
// in a TypeCast resolved via pg_proc, and returns the modified SQL. Any
// FuncCall that can't be resolved (unknown, overloaded) is left alone.
// Returns the original canonical unchanged if parsing/deparsing fails or
// nothing needed casting.
func castFuncParamRefs(ctx context.Context, cache *typecastCache, canonical string) string {
	tree, err := pg_query.Parse(canonical)
	if err != nil {
		return canonical
	}
	changed := false
	for _, raw := range tree.Stmts {
		if raw == nil || raw.Stmt == nil {
			continue
		}
		if walkCast(ctx, cache, raw.Stmt) {
			changed = true
		}
	}
	if !changed {
		return canonical
	}
	out, err := pg_query.Deparse(tree)
	if err != nil {
		return canonical
	}
	return out
}

func walkCast(ctx context.Context, cache *typecastCache, n *pg_query.Node) bool {
	if n == nil {
		return false
	}
	changed := false
	switch v := n.Node.(type) {
	case *pg_query.Node_FuncCall:
		if castVariadicAny(v.FuncCall) {
			changed = true
		}
		if castFuncCall(ctx, cache, v.FuncCall) {
			changed = true
		}
		for _, a := range v.FuncCall.Args {
			if walkCast(ctx, cache, a) {
				changed = true
			}
		}
	case *pg_query.Node_BoolExpr:
		// AND / OR / NOT take boolean args. Bare ParamRefs here
		// default to unknown/text and break planning later.
		for i, a := range v.BoolExpr.Args {
			if _, isParam := a.Node.(*pg_query.Node_ParamRef); isParam {
				v.BoolExpr.Args[i] = castParam(a, "bool")
				changed = true
			}
		}
		for _, a := range v.BoolExpr.Args {
			if walkCast(ctx, cache, a) {
				changed = true
			}
		}
		return changed
	case *pg_query.Node_SelectStmt:
		s := v.SelectStmt
		for _, t := range s.TargetList {
			if walkCast(ctx, cache, t) {
				changed = true
			}
		}
		for _, f := range s.FromClause {
			if walkCast(ctx, cache, f) {
				changed = true
			}
		}
		if coerceBool(&s.WhereClause) {
			changed = true
		}
		if walkCast(ctx, cache, s.WhereClause) {
			changed = true
		}
		if coerceBool(&s.HavingClause) {
			changed = true
		}
		if walkCast(ctx, cache, s.HavingClause) {
			changed = true
		}
		for _, g := range s.GroupClause {
			if walkCast(ctx, cache, g) {
				changed = true
			}
		}
		if s.WithClause != nil {
			for _, cte := range s.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					if walkCast(ctx, cache, c.CommonTableExpr.Ctequery) {
						changed = true
					}
				}
			}
		}
	case *pg_query.Node_InsertStmt:
		i := v.InsertStmt
		if walkCast(ctx, cache, i.SelectStmt) {
			changed = true
		}
		for _, r := range i.ReturningList {
			if walkCast(ctx, cache, r) {
				changed = true
			}
		}
		if i.WithClause != nil {
			for _, cte := range i.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					if walkCast(ctx, cache, c.CommonTableExpr.Ctequery) {
						changed = true
					}
				}
			}
		}
	case *pg_query.Node_UpdateStmt:
		u := v.UpdateStmt
		for _, t := range u.TargetList {
			if walkCast(ctx, cache, t) {
				changed = true
			}
		}
		if coerceBool(&u.WhereClause) {
			changed = true
		}
		if walkCast(ctx, cache, u.WhereClause) {
			changed = true
		}
		for _, f := range u.FromClause {
			if walkCast(ctx, cache, f) {
				changed = true
			}
		}
		for _, r := range u.ReturningList {
			if walkCast(ctx, cache, r) {
				changed = true
			}
		}
		if u.WithClause != nil {
			for _, cte := range u.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					if walkCast(ctx, cache, c.CommonTableExpr.Ctequery) {
						changed = true
					}
				}
			}
		}
	case *pg_query.Node_DeleteStmt:
		d := v.DeleteStmt
		if coerceBool(&d.WhereClause) {
			changed = true
		}
		if walkCast(ctx, cache, d.WhereClause) {
			changed = true
		}
		for _, u := range d.UsingClause {
			if walkCast(ctx, cache, u) {
				changed = true
			}
		}
		for _, r := range d.ReturningList {
			if walkCast(ctx, cache, r) {
				changed = true
			}
		}
		if d.WithClause != nil {
			for _, cte := range d.WithClause.Ctes {
				if c, ok := cte.Node.(*pg_query.Node_CommonTableExpr); ok {
					if walkCast(ctx, cache, c.CommonTableExpr.Ctequery) {
						changed = true
					}
				}
			}
		}
	case *pg_query.Node_ResTarget:
		if walkCast(ctx, cache, v.ResTarget.Val) {
			changed = true
		}
	case *pg_query.Node_AExpr:
		if walkCast(ctx, cache, v.AExpr.Lexpr) {
			changed = true
		}
		if walkCast(ctx, cache, v.AExpr.Rexpr) {
			changed = true
		}
	case *pg_query.Node_TypeCast:
		if walkCast(ctx, cache, v.TypeCast.Arg) {
			changed = true
		}
	case *pg_query.Node_CoalesceExpr:
		for _, a := range v.CoalesceExpr.Args {
			if walkCast(ctx, cache, a) {
				changed = true
			}
		}
	case *pg_query.Node_MinMaxExpr:
		for _, a := range v.MinMaxExpr.Args {
			if walkCast(ctx, cache, a) {
				changed = true
			}
		}
	case *pg_query.Node_AArrayExpr:
		for _, e := range v.AArrayExpr.Elements {
			if walkCast(ctx, cache, e) {
				changed = true
			}
		}
	case *pg_query.Node_RowExpr:
		for _, a := range v.RowExpr.Args {
			if walkCast(ctx, cache, a) {
				changed = true
			}
		}
	case *pg_query.Node_NullTest:
		if walkCast(ctx, cache, v.NullTest.Arg) {
			changed = true
		}
	case *pg_query.Node_BooleanTest:
		if walkCast(ctx, cache, v.BooleanTest.Arg) {
			changed = true
		}
	case *pg_query.Node_List:
		for _, a := range v.List.Items {
			if walkCast(ctx, cache, a) {
				changed = true
			}
		}
	case *pg_query.Node_SubLink:
		// EXISTS (SELECT $N FROM ...) — the app parameterized the SELECT 1
		// literal. PG ignores the value but still needs to type $N. Cast
		// to int so the SubLink can be planned.
		if v.SubLink.SubLinkType == pg_query.SubLinkType_EXISTS_SUBLINK {
			if sub, ok := v.SubLink.Subselect.Node.(*pg_query.Node_SelectStmt); ok {
				for i, t := range sub.SelectStmt.TargetList {
					rt, isRT := t.Node.(*pg_query.Node_ResTarget)
					if !isRT {
						continue
					}
					if _, isParam := rt.ResTarget.Val.Node.(*pg_query.Node_ParamRef); !isParam {
						continue
					}
					rt.ResTarget.Val = castParam(rt.ResTarget.Val, "int4")
					sub.SelectStmt.TargetList[i] = t
					changed = true
				}
			}
		}
		if walkCast(ctx, cache, v.SubLink.Subselect) {
			changed = true
		}
		if walkCast(ctx, cache, v.SubLink.Testexpr) {
			changed = true
		}
	case *pg_query.Node_JoinExpr:
		if walkCast(ctx, cache, v.JoinExpr.Larg) {
			changed = true
		}
		if walkCast(ctx, cache, v.JoinExpr.Rarg) {
			changed = true
		}
		// JOIN ... ON <expr>: the qualifier must be boolean. Bare
		// ParamRef there (`LEFT JOIN t ON $N`) plans as unknown and
		// fails with "argument of JOIN/ON must be type boolean".
		if coerceBool(&v.JoinExpr.Quals) {
			changed = true
		}
		if walkCast(ctx, cache, v.JoinExpr.Quals) {
			changed = true
		}
	case *pg_query.Node_RangeSubselect:
		if walkCast(ctx, cache, v.RangeSubselect.Subquery) {
			changed = true
		}
	case *pg_query.Node_CaseExpr:
		// Searched CASE (no Arg): each WHEN is a boolean predicate.
		// Simple CASE (with Arg): each WHEN is a value compared to Arg.
		// Bare ParamRef in a searched WHEN must be cast to bool.
		searched := v.CaseExpr.Arg == nil
		if walkCast(ctx, cache, v.CaseExpr.Arg) {
			changed = true
		}
		if walkCast(ctx, cache, v.CaseExpr.Defresult) {
			changed = true
		}
		for _, a := range v.CaseExpr.Args {
			if searched {
				if cw, ok := a.Node.(*pg_query.Node_CaseWhen); ok {
					if _, isParam := cw.CaseWhen.Expr.Node.(*pg_query.Node_ParamRef); isParam {
						cw.CaseWhen.Expr = castParam(cw.CaseWhen.Expr, "bool")
						changed = true
					}
				}
			}
			if walkCast(ctx, cache, a) {
				changed = true
			}
		}
	case *pg_query.Node_CaseWhen:
		if walkCast(ctx, cache, v.CaseWhen.Expr) {
			changed = true
		}
		if walkCast(ctx, cache, v.CaseWhen.Result) {
			changed = true
		}
	}
	return changed
}

func castFuncCall(ctx context.Context, cache *typecastCache, fc *pg_query.FuncCall) bool {
	if fc == nil || len(fc.Args) == 0 {
		return false
	}
	// Only cast if at least one arg is a bare ParamRef — otherwise nothing
	// to disambiguate
	hasParamRef := false
	for _, a := range fc.Args {
		if _, ok := a.Node.(*pg_query.Node_ParamRef); ok {
			hasParamRef = true
			break
		}
	}
	if !hasParamRef {
		return false
	}

	schema, name, ok := funcSchemaName(fc.Funcname)
	if !ok {
		return false
	}
	// Skip pg_catalog synthetic names like pg_catalog.extract — their args
	// aren't real overload-resolvable params
	if schema == "pg_catalog" && fc.Funcformat == pg_query.CoercionForm_COERCE_SQL_SYNTAX {
		return false
	}

	types := cache.lookup(ctx, schema, name, len(fc.Args))
	if types == nil || len(types) != len(fc.Args) {
		return false
	}

	changed := false
	for i, a := range fc.Args {
		if _, isParam := a.Node.(*pg_query.Node_ParamRef); !isParam {
			continue
		}
		fc.Args[i] = &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: &pg_query.TypeCast{
			Arg:      a,
			TypeName: typeNameFromString(types[i]),
		}}}
		changed = true
	}
	return changed
}

// castVariadicAny handles a handful of well-known VARIADIC ANY functions
// whose signatures castFuncCall can't resolve through pg_proc (pronargs=0,
// no concrete param types). Returns true if any ParamRef got wrapped.
func castVariadicAny(fc *pg_query.FuncCall) bool {
	if fc == nil {
		return false
	}
	schema, name, ok := funcSchemaName(fc.Funcname)
	if !ok {
		return false
	}
	if schema != "" && schema != "pg_catalog" {
		return false
	}
	// json_build_object / jsonb_build_object: alternating (key, value,
	// key, value, ...). Keys (odd positions, 1-indexed) must be text.
	// Values (even positions) can be any — leave untouched.
	switch name {
	case "json_build_object", "jsonb_build_object":
		changed := false
		for i, a := range fc.Args {
			if i%2 != 0 {
				continue // even index = odd position = value
			}
			if _, isParam := a.Node.(*pg_query.Node_ParamRef); !isParam {
				continue
			}
			fc.Args[i] = castParam(a, "text")
			changed = true
		}
		return changed
	}
	return false
}

// coerceBool wraps a bare ParamRef in a ::bool cast when it sits in a
// boolean position (WHERE / HAVING / JOIN ON). Rewrites through the
// pointer so caller doesn't need to reassign.
func coerceBool(n **pg_query.Node) bool {
	if n == nil || *n == nil {
		return false
	}
	if _, isParam := (*n).Node.(*pg_query.Node_ParamRef); !isParam {
		return false
	}
	*n = castParam(*n, "bool")
	return true
}

func castParam(p *pg_query.Node, typeName string) *pg_query.Node {
	return &pg_query.Node{Node: &pg_query.Node_TypeCast{TypeCast: &pg_query.TypeCast{
		Arg:      p,
		TypeName: typeNameFromString(typeName),
	}}}
}

func funcSchemaName(funcname []*pg_query.Node) (schema, name string, ok bool) {
	parts := make([]string, 0, len(funcname))
	for _, f := range funcname {
		s, isStr := f.Node.(*pg_query.Node_String_)
		if !isStr {
			return "", "", false
		}
		parts = append(parts, s.String_.Sval)
	}
	switch len(parts) {
	case 1:
		return "", parts[0], true
	case 2:
		return parts[0], parts[1], true
	default:
		return "", "", false
	}
}

func typeNameFromString(s string) *pg_query.TypeName {
	parts := strings.Split(s, ".")
	names := make([]*pg_query.Node, 0, len(parts))
	for _, p := range parts {
		names = append(names, &pg_query.Node{Node: &pg_query.Node_String_{String_: &pg_query.String{Sval: p}}})
	}
	return &pg_query.TypeName{Names: names, Typemod: -1}
}
