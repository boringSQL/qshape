//! Wrap `$N` ParamRefs in TypeCasts so EXPLAIN(GENERIC_PLAN) can plan
//! a canonical SQL without manual annotation
//!
//! The lookup function (typically a DB-backed pg_proc cache) is decoupled
//! from the AST walk: callers pass an `FnMut(schema, name, arity) to
//! Option<Vec<String>>`, ithis way tests can use a closure with
//! prepopulated answers instead of touching a database

use std::string::String;

use pg_query::NodeEnum;
use pg_query::protobuf::*;

use crate::reshape::for_each_child;

/// Parse `canonical`, wrap every overload-resolvable `$N` in a TypeCast
/// using the types resolved by `lookup`, and deparse. Returns `canonical`
/// unchanged on parse/deparse failure or when nothing needed casting
pub fn cast_func_param_refs<F>(canonical: &str, lookup: &mut F) -> String
where
    F: FnMut(&str, &str, usize) -> Option<Vec<String>>,
{
    let Ok(mut tree) = pg_query::parse(canonical) else { return canonical.to_string() };
    let mut walker = Walker { lookup, changed: false };
    for raw in &mut tree.protobuf.stmts {
        if let Some(stmt) = raw.stmt.as_deref_mut() {
            walker.walk(stmt);
        }
    }
    if !walker.changed {
        return canonical.to_string();
    }
    tree.deparse().unwrap_or_else(|_| canonical.to_string())
}

/// Highest `$N` referenced in `sql`, ignoring `$N` inside single-quoted
/// string literals (with `''` escapes)
#[must_use]
pub fn max_param_number(sql: &str) -> i32 {
    let bytes = sql.as_bytes();
    let mut max = 0i32;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_string(bytes, i + 1),
            b'$' => {
                let mut j = i + 1;
                let mut n: i32 = 0;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    n = n * 10 + (bytes[j] - b'0') as i32;
                    j += 1;
                }
                if j > i + 1 {
                    max = max.max(n);
                    i = j;
                } else {
                    i += 1;
                }
            }
            _ => i += 1,
        }
    }
    max
}

fn skip_string(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // doubled '' is an escaped quote, not the end
            if bytes.get(i + 1) == Some(&b'\'') {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    i
}

struct Walker<'a, F> {
    lookup: &'a mut F,
    changed: bool,
}

impl<F> Walker<'_, F>
where
    F: FnMut(&str, &str, usize) -> Option<Vec<String>>,
{
    fn walk(&mut self, n: &mut Node) {
        match n.node.as_mut() {
            Some(NodeEnum::FuncCall(fc)) => {
                self.changed |= cast_variadic_any(fc);
                self.changed |= cast_func_call(fc, self.lookup);
            }
            Some(NodeEnum::BoolExpr(b)) => {
                for arg in &mut b.args {
                    self.changed |= cast_if_param(arg, "bool");
                }
            }
            Some(NodeEnum::SelectStmt(s)) => {
                self.changed |= coerce_bool(&mut s.where_clause);
                self.changed |= coerce_bool(&mut s.having_clause);
            }
            Some(NodeEnum::UpdateStmt(u)) => {
                self.changed |= coerce_bool(&mut u.where_clause);
            }
            Some(NodeEnum::DeleteStmt(d)) => {
                self.changed |= coerce_bool(&mut d.where_clause);
            }
            Some(NodeEnum::JoinExpr(j)) => {
                self.changed |= coerce_bool(&mut j.quals);
            }
            // searched CASE: each WHEN expr is a boolean predicate
            Some(NodeEnum::CaseExpr(c)) if c.arg.is_none() => {
                for arm in &mut c.args {
                    if let Some(NodeEnum::CaseWhen(cw)) = arm.node.as_mut()
                        && let Some(expr) = cw.expr.as_deref_mut()
                    {
                        self.changed |= cast_if_param(expr, "bool");
                    }
                }
            }
            // EXISTS (SELECT $N FROM ...): PG still needs to type $N
            // even though the value is ignored
            Some(NodeEnum::SubLink(sl))
                if sl.sub_link_type == SubLinkType::ExistsSublink as i32 =>
            {
                if let Some(sub) = sl.subselect.as_deref_mut()
                    && let Some(NodeEnum::SelectStmt(s)) = sub.node.as_mut()
                {
                    for t in &mut s.target_list {
                        if let Some(NodeEnum::ResTarget(rt)) = t.node.as_mut()
                            && let Some(val) = rt.val.as_deref_mut()
                        {
                            self.changed |= cast_if_param(val, "int4");
                        }
                    }
                }
            }
            _ => {}
        }
        for_each_child(n, &mut |c| self.walk(c));
    }
}

fn cast_func_call<F>(fc: &mut FuncCall, lookup: &mut F) -> bool
where
    F: FnMut(&str, &str, usize) -> Option<Vec<String>>,
{
    // skip the cache hit when there's nothing to disambiguate
    if !fc.args.iter().any(|a| matches!(a.node, Some(NodeEnum::ParamRef(_)))) {
        return false;
    }
    let Some((schema, name)) = func_schema_name(&fc.funcname) else { return false };
    // pg_catalog.extract & co. are COERCE_SQL_SYNTAX forms — args aren't
    // overload-resolvable params
    if schema == "pg_catalog" && fc.funcformat == CoercionForm::CoerceSqlSyntax as i32 {
        return false;
    }
    let nargs = fc.args.len();
    let Some(types) = lookup(&schema, &name, nargs) else { return false };
    if types.len() != nargs {
        return false;
    }
    let mut changed = false;
    for (arg, ty) in fc.args.iter_mut().zip(types.iter()) {
        changed |= cast_if_param(arg, ty);
    }
    changed
}

// json_build_object / jsonb_build_object are VARIADIC ANY (pronargs=0)
// so cast_func_call can't resolve them. Keys land at odd positions
// (1, 3, 5, etc) and must be text
fn cast_variadic_any(fc: &mut FuncCall) -> bool {
    let Some((schema, name)) = func_schema_name(&fc.funcname) else { return false };
    if !schema.is_empty() && schema != "pg_catalog" {
        return false;
    }
    if name != "json_build_object" && name != "jsonb_build_object" {
        return false;
    }
    let mut changed = false;
    for (i, arg) in fc.args.iter_mut().enumerate() {
        // even index = odd position (1-indexed) = key
        if i % 2 == 0 {
            changed |= cast_if_param(arg, "text");
        }
    }
    changed
}

fn coerce_bool(opt: &mut Option<Box<Node>>) -> bool {
    opt.as_deref_mut().is_some_and(|n| cast_if_param(n, "bool"))
}

fn cast_if_param(n: &mut Node, type_name: &str) -> bool {
    if !matches!(n.node, Some(NodeEnum::ParamRef(_))) {
        return false;
    }
    *n = cast_node(std::mem::take(n), type_name);
    true
}

fn cast_node(inner: Node, type_name: &str) -> Node {
    Node {
        node: Some(NodeEnum::TypeCast(Box::new(TypeCast {
            arg: Some(Box::new(inner)),
            type_name: Some(type_name_from_string(type_name)),
            location: -1,
        }))),
    }
}

fn func_schema_name(funcname: &[Node]) -> Option<(String, String)> {
    let parts: Option<Vec<&str>> = funcname
        .iter()
        .map(|f| match f.node.as_ref() {
            Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
            _ => None,
        })
        .collect();
    match parts?.as_slice() {
        [name] => Some((String::new(), (*name).to_string())),
        [schema, name] => Some(((*schema).to_string(), (*name).to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn lookup_from<'a>(
        map: &'a HashMap<(&'a str, &'a str, usize), Vec<&'a str>>,
    ) -> impl FnMut(&str, &str, usize) -> Option<Vec<String>> + 'a {
        |schema: &str, name: &str, n: usize| {
            map.get(&(schema, name, n))
                .map(|v| v.iter().map(|s| (*s).to_string()).collect())
        }
    }

    fn empty_lookup(_: &str, _: &str, _: usize) -> Option<Vec<String>> {
        None
    }

    #[test]
    fn wraps_unqualified_params() {
        let map = HashMap::from([(("toggl", "is_login_restricted_by_sso", 1), vec!["text"])]);
        let got = cast_func_param_refs(
            "SELECT toggl.is_login_restricted_by_sso($1)",
            &mut lookup_from(&map),
        );
        assert_eq!(got, "SELECT toggl.is_login_restricted_by_sso($1::text)");
    }

    #[test]
    fn multiple_args_mixed() {
        let map = HashMap::from([(("auth", "clean_up_sessions", 2), vec!["int8", "int4"])]);
        let got = cast_func_param_refs(
            "SELECT auth.clean_up_sessions($1, 100)",
            &mut lookup_from(&map),
        );
        assert_eq!(got, "SELECT auth.clean_up_sessions($1::int8, 100)");
    }

    #[test]
    fn qualified_type() {
        let map = HashMap::from([(("public", "do_thing", 1), vec!["public.my_type"])]);
        let got = cast_func_param_refs("SELECT public.do_thing($1)", &mut lookup_from(&map));
        assert_eq!(got, "SELECT public.do_thing($1::public.my_type)");
    }

    #[test]
    fn skips_unresolved() {
        let got = cast_func_param_refs("SELECT unknown.f($1)", &mut empty_lookup);
        assert_eq!(got, "SELECT unknown.f($1)");
    }

    // pg_catalog.extract is a COERCE_SQL_SYNTAX form — its first arg is
    // a field identifier, not an overload-resolvable param
    #[test]
    fn skips_extract() {
        let sql = "SELECT extract (epoch FROM $1::interval)";
        let got = cast_func_param_refs(sql, &mut empty_lookup);
        assert_eq!(got, sql);
    }

    // No FuncCall with ParamRef arg — $1 in WHERE is resolved by the
    // planner from the column type
    #[test]
    fn leaves_column_context_params() {
        let sql = "SELECT id FROM users WHERE id = $1";
        let got = cast_func_param_refs(sql, &mut empty_lookup);
        assert_eq!(got, sql);
    }

    // BoolExpr (AND/OR/NOT) args must be boolean — bare ParamRef there
    // defaults to unknown/text and breaks planning
    #[test]
    fn bool_expr_param_to_bool() {
        let got = cast_func_param_refs(
            "SELECT * FROM users WHERE $1 OR active",
            &mut empty_lookup,
        );
        assert_eq!(got, "SELECT * FROM users WHERE $1::bool OR active");
    }

    // VARIADIC ANY: pronargs=0 in pg_proc so cast_func_call can't resolve;
    // odd-position keys must be text
    #[test]
    fn json_build_object_keys_to_text() {
        let got = cast_func_param_refs(
            "SELECT json_build_object($1, user_id, $2, name)",
            &mut empty_lookup,
        );
        assert_eq!(got, "SELECT json_build_object($1::text, user_id, $2::text, name)");
    }

    // EXISTS (SELECT $N ...): PG ignores the value but still types $N
    #[test]
    fn exists_select_list_param_to_int() {
        let got = cast_func_param_refs(
            "SELECT EXISTS (SELECT $1 FROM users WHERE id = $2)",
            &mut empty_lookup,
        );
        assert_eq!(got, "SELECT EXISTS (SELECT $1::int4 FROM users WHERE id = $2)");
    }

    // JOIN ... ON <expr>: qualifier must be boolean
    #[test]
    fn join_on_param_to_bool() {
        let got = cast_func_param_refs(
            "SELECT * FROM a LEFT JOIN b ON $1",
            &mut empty_lookup,
        );
        assert_eq!(got, "SELECT * FROM a LEFT JOIN b ON $1::bool");
    }

    // Searched CASE: each WHEN is a boolean predicate
    #[test]
    fn searched_case_when_param_to_bool() {
        let got = cast_func_param_refs(
            "SELECT CASE WHEN $1 THEN 'a' ELSE 'b' END",
            &mut empty_lookup,
        );
        assert_eq!(got, "SELECT CASE WHEN $1::bool THEN 'a' ELSE 'b' END");
    }

    // Simple CASE: arg WHEN val — value compared to arg, must stay typed
    // to arg's type
    #[test]
    fn simple_case_when_param_uncast() {
        let sql = "SELECT CASE col WHEN $1 THEN 'a' ELSE 'b' END FROM t";
        let got = cast_func_param_refs(sql, &mut empty_lookup);
        assert_eq!(got, sql);
    }

    // CoalesceExpr is a SQL construct, not a FuncCall — walker needs an
    // explicit case for it. json_build_object buried inside COALESCE was
    // missed before walking CoalesceExpr children
    #[test]
    fn descends_into_coalesce_expr() {
        let got = cast_func_param_refs(
            "SELECT COALESCE(json_agg(json_build_object($1, col)), $2::pg_catalog.json) FROM t",
            &mut empty_lookup,
        );
        assert_eq!(
            got,
            "SELECT COALESCE(json_agg(json_build_object($1::text, col)), $2::pg_catalog.json) FROM t"
        );
    }

    #[test]
    fn max_param_number_cases() {
        for (sql, want) in [
            ("SELECT 1", 0),
            ("SELECT $1", 1),
            ("SELECT $1, $2, $3", 3),
            ("SELECT $10 + $2", 10),
            ("SELECT '$1 inside string'", 0),
            ("SELECT 'it''s $5' || $3", 3),
        ] {
            assert_eq!(max_param_number(sql), want, "input: {sql:?}");
        }
    }
}

fn type_name_from_string(s: &str) -> TypeName {
    let names = s
        .split('.')
        .map(|p| Node {
            node: Some(NodeEnum::String(pg_query::protobuf::String { sval: p.to_string() })),
        })
        .collect();
    TypeName { names, typemod: -1, ..TypeName::default() }
}
