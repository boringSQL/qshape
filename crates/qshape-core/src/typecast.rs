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

fn type_name_from_string(s: &str) -> TypeName {
    let names = s
        .split('.')
        .map(|p| Node {
            node: Some(NodeEnum::String(pg_query::protobuf::String { sval: p.to_string() })),
        })
        .collect();
    TypeName { names, typemod: -1, ..TypeName::default() }
}
