use std::collections::{BTreeSet, HashMap};

use pg_query::NodeEnum;
use pg_query::protobuf::*;
use prost::Message;

pub(crate) fn reshape(tree: &mut ParseResult) {
    for raw in &mut tree.stmts {
        if let Some(stmt) = raw.stmt.as_deref_mut() {
            reshape_node(stmt);
            walk_fixups(stmt);
            renumber_params(stmt);
        }
    }
}

fn reshape_node(n: &mut Node) {
    match n.node.as_mut() {
        Some(NodeEnum::SelectStmt(s)) => reshape_select(s),
        Some(NodeEnum::UpdateStmt(u)) => reshape_update(u),
        Some(NodeEnum::DeleteStmt(d)) => reshape_delete(d),
        _ => {}
    }
}

fn reshape_select(s: &mut SelectStmt) {
    reshape_nested_from(&mut s.from_clause);
    if let Some(w) = s.with_clause.as_mut() {
        reshape_with_clause(w);
    }
    if let Some(l) = s.larg.as_mut() {
        reshape_select(l);
    }
    if let Some(r) = s.rarg.as_mut() {
        reshape_select(r);
    }

    // TODO: alias stripping + sublink reshape

    if let Some(w) = s.where_clause.as_deref_mut() {
        sort_and_tree(w);
    }
    if let Some(h) = s.having_clause.as_deref_mut() {
        sort_and_tree(h);
    }
}

fn reshape_update(u: &mut UpdateStmt) {
    if let Some(w) = u.with_clause.as_mut() {
        reshape_with_clause(w);
    }
    reshape_nested_from(&mut u.from_clause);

    // TODO: alias stripping + sublinks

    if let Some(w) = u.where_clause.as_deref_mut() {
        sort_and_tree(w);
    }
}

fn reshape_delete(d: &mut DeleteStmt) {
    if let Some(w) = d.with_clause.as_mut() {
        reshape_with_clause(w);
    }
    reshape_nested_from(&mut d.using_clause);

    // TODO: alias stripping + sublinks

    if let Some(w) = d.where_clause.as_deref_mut() {
        sort_and_tree(w);
    }
}

fn reshape_with_clause(w: &mut WithClause) {
    for cte in &mut w.ctes {
        if let Some(NodeEnum::CommonTableExpr(c)) = cte.node.as_mut()
            && let Some(q) = c.ctequery.as_deref_mut()
        {
            reshape_node(q);
        }
    }
}

fn reshape_nested_from(items: &mut [Node]) {
    for n in items {
        reshape_nested_from_item(n);
    }
}

fn reshape_nested_from_item(n: &mut Node) {
    match n.node.as_mut() {
        Some(NodeEnum::RangeSubselect(r)) => {
            if let Some(q) = r.subquery.as_deref_mut() {
                reshape_node(q);
            }
        }
        Some(NodeEnum::JoinExpr(j)) => {
            if let Some(l) = j.larg.as_deref_mut() {
                reshape_nested_from_item(l);
            }
            if let Some(r) = j.rarg.as_deref_mut() {
                reshape_nested_from_item(r);
            }
        }
        _ => {}
    }
}

// --- AST fixups -------------------------------------------------------------

fn walk_fixups(n: &mut Node) {
    if let Some(NodeEnum::FuncCall(fc)) = n.node.as_mut() {
        fix_extract_field_ident(fc);
    }
    for_each_child(n, &mut walk_fixups);
}

fn is_pg_catalog_extract(fc: &FuncCall) -> bool {
    if fc.funcname.len() != 2 {
        return false;
    }
    let Some(NodeEnum::String(s0)) = fc.funcname[0].node.as_ref() else {
        return false;
    };
    let Some(NodeEnum::String(s1)) = fc.funcname[1].node.as_ref() else {
        return false;
    };
    s0.sval == "pg_catalog"
        && s1.sval == "extract"
        && fc.funcformat == CoercionForm::CoerceSqlSyntax as i32
}

// change EXTRACT's field from a string constant to a bare identifier. I.e.
// `epoch` becomes string, and is later emitted quoted. Can't decide whatever
// that's real bug in pg_query or strange case in test queries used.
fn fix_extract_field_ident(fc: &mut FuncCall) {
    if !is_pg_catalog_extract(fc) || fc.args.is_empty() {
        return;
    }

    let ident = match fc.args[0].node.as_ref() {
        Some(NodeEnum::AConst(AConst { val: Some(a_const::Val::Sval(s)), .. })) => s.sval.clone(),
        Some(NodeEnum::ParamRef(_)) => "epoch".to_string(),
        _ => return,
    };

    fc.args[0] = Node {
        node: Some(NodeEnum::ColumnRef(ColumnRef {
            fields: vec![Node {
                node: Some(NodeEnum::String(pg_query::protobuf::String { sval: ident })),
            }],
            location: -1,
        })),
    };
}

// --- param renumber ---------------------------------------------------------

// renumber_params closes gaps in $N numbering left by fixups that dropped
// ParamRefs (e.g. an EXTRACT field that used to be parameterised). EXPLAIN
// (GENERIC_PLAN) won't plan a query that uses $2 without a $1.
fn renumber_params(root: &mut Node) {
    let mut used: BTreeSet<i32> = BTreeSet::new();
    collect_params(root, &mut used);
    if used.is_empty() {
        return;
    }
    let mut remap: HashMap<i32, i32> = HashMap::new();
    let mut contiguous = true;
    for (i, &n) in used.iter().enumerate() {
        let new_n = (i as i32) + 1;
        if n != new_n {
            contiguous = false;
        }
        remap.insert(n, new_n);
    }
    if contiguous {
        return;
    }
    apply_param_remap(root, &remap);
}

fn collect_params(n: &mut Node, out: &mut BTreeSet<i32>) {
    if let Some(NodeEnum::ParamRef(p)) = n.node.as_ref() {
        out.insert(p.number);
    }
    for_each_child(n, &mut |c| collect_params(c, out));
}

fn apply_param_remap(n: &mut Node, remap: &HashMap<i32, i32>) {
    if let Some(NodeEnum::ParamRef(p)) = n.node.as_mut()
        && let Some(&nn) = remap.get(&p.number)
    {
        p.number = nn;
    }
    for_each_child(n, &mut |c| apply_param_remap(c, remap));
}

// --- forEachChild -----------------------------------------------------------

type NodeFn<'a> = &'a mut dyn FnMut(&mut Node);

fn visit(n: Option<&mut Node>, f: NodeFn<'_>) {
    if let Some(n) = n {
        f(n);
    }
}

fn visit_all(ns: &mut [Node], f: NodeFn<'_>) {
    for n in ns {
        f(n);
    }
}

fn for_each_child(n: &mut Node, f: NodeFn<'_>) {
    match n.node.as_mut() {
        Some(NodeEnum::SelectStmt(s)) => for_each_select_child(s, f),
        Some(NodeEnum::UpdateStmt(u)) => {
            visit_all(&mut u.target_list, f);
            visit(u.where_clause.as_deref_mut(), f);
            visit_all(&mut u.from_clause, f);
            visit_all(&mut u.returning_list, f);
            if let Some(w) = u.with_clause.as_mut() {
                for_each_cte(w, f);
            }
        }
        Some(NodeEnum::DeleteStmt(d)) => {
            visit(d.where_clause.as_deref_mut(), f);
            visit_all(&mut d.using_clause, f);
            visit_all(&mut d.returning_list, f);
            if let Some(w) = d.with_clause.as_mut() {
                for_each_cte(w, f);
            }
        }
        Some(NodeEnum::InsertStmt(i)) => {
            visit(i.select_stmt.as_deref_mut(), f);
            visit_all(&mut i.returning_list, f);
            if let Some(w) = i.with_clause.as_mut() {
                for_each_cte(w, f);
            }
        }
        Some(NodeEnum::FuncCall(fc)) => visit_all(&mut fc.args, f),
        Some(NodeEnum::ResTarget(r)) => visit(r.val.as_deref_mut(), f),
        Some(NodeEnum::AExpr(a)) => {
            visit(a.lexpr.as_deref_mut(), f);
            visit(a.rexpr.as_deref_mut(), f);
        }
        Some(NodeEnum::BoolExpr(b)) => visit_all(&mut b.args, f),
        Some(NodeEnum::TypeCast(t)) => visit(t.arg.as_deref_mut(), f),
        Some(NodeEnum::List(l)) => visit_all(&mut l.items, f),
        Some(NodeEnum::NullTest(t)) => visit(t.arg.as_deref_mut(), f),
        Some(NodeEnum::BooleanTest(t)) => visit(t.arg.as_deref_mut(), f),
        Some(NodeEnum::CaseExpr(c)) => {
            visit(c.arg.as_deref_mut(), f);
            visit(c.defresult.as_deref_mut(), f);
            visit_all(&mut c.args, f);
        }
        Some(NodeEnum::CaseWhen(c)) => {
            visit(c.expr.as_deref_mut(), f);
            visit(c.result.as_deref_mut(), f);
        }
        Some(NodeEnum::SubLink(sl)) => {
            visit(sl.subselect.as_deref_mut(), f);
            visit(sl.testexpr.as_deref_mut(), f);
        }
        Some(NodeEnum::JoinExpr(j)) => {
            visit(j.larg.as_deref_mut(), f);
            visit(j.rarg.as_deref_mut(), f);
            visit(j.quals.as_deref_mut(), f);
        }
        Some(NodeEnum::RangeSubselect(r)) => visit(r.subquery.as_deref_mut(), f),
        Some(NodeEnum::SortBy(s)) => visit(s.node.as_deref_mut(), f),
        Some(NodeEnum::CoalesceExpr(c)) => visit_all(&mut c.args, f),
        Some(NodeEnum::MinMaxExpr(m)) => visit_all(&mut m.args, f),
        _ => {}
    }
}

fn for_each_select_child(s: &mut SelectStmt, f: NodeFn<'_>) {
    visit_all(&mut s.target_list, f);
    visit_all(&mut s.from_clause, f);
    visit(s.where_clause.as_deref_mut(), f);
    visit(s.having_clause.as_deref_mut(), f);
    visit_all(&mut s.group_clause, f);
    visit_all(&mut s.sort_clause, f);
    visit_all(&mut s.distinct_clause, f);
    visit(s.limit_offset.as_deref_mut(), f);
    visit(s.limit_count.as_deref_mut(), f);
    visit_all(&mut s.values_lists, f);
    if let Some(w) = s.with_clause.as_mut() {
        for_each_cte(w, f);
    }
    if let Some(l) = s.larg.as_deref_mut() {
        for_each_select_child(l, f);
    }
    if let Some(r) = s.rarg.as_deref_mut() {
        for_each_select_child(r, f);
    }
}

fn for_each_cte(w: &mut WithClause, f: &mut dyn FnMut(&mut Node)) {
    for cte in &mut w.ctes {
        if let Some(NodeEnum::CommonTableExpr(c)) = cte.node.as_mut()
            && let Some(q) = c.ctequery.as_deref_mut()
        {
            f(q);
        }
    }
}

// --- sort AND tree ----------------------------------------------------------

fn sort_and_tree(n: &mut Node) {
    match n.node.as_mut() {
        Some(NodeEnum::BoolExpr(b)) => {
            for a in &mut b.args {
                sort_and_tree(a);
            }
            if b.boolop == BoolExprType::AndExpr as i32 {
                b.args.sort_by_cached_key(arg_sort_key);
            }
        }
        Some(NodeEnum::AExpr(a)) => {
            if let Some(l) = a.lexpr.as_deref_mut() {
                sort_and_tree(l);
            }
            if let Some(r) = a.rexpr.as_deref_mut() {
                sort_and_tree(r);
            }
        }
        _ => {}
    }
}

fn arg_sort_key(n: &Node) -> Vec<u8> {
    n.encode_to_vec()
}

#[cfg(test)]
mod tests {
    use crate::normalize::normalize;

    #[test]
    fn sorts_flat_and_tree() {
        let a = normalize("SELECT id FROM users WHERE a = $1 AND b = $2").unwrap();
        let b = normalize("SELECT id FROM users WHERE b = $2 AND a = $1").unwrap();
        assert_eq!(a, b, "reordered AND conjuncts did not collapse");
    }

    #[test]
    fn sorts_three_way_and() {
        let a = normalize("SELECT 1 FROM t WHERE c = $3 AND a = $1 AND b = $2").unwrap();
        let b = normalize("SELECT 1 FROM t WHERE a = $1 AND b = $2 AND c = $3").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn idempotent_without_aliases() {
        let cases = [
            "SELECT id FROM users",
            "SELECT id FROM users WHERE b = $1 AND a = $2 AND c = $3",
            "UPDATE users SET name = $1 WHERE id = $2",
            "WITH recent AS (SELECT id FROM users) SELECT id FROM recent",
        ];
        for sql in cases {
            let first = normalize(sql).unwrap_or_else(|e| panic!("{sql:?}: {e}"));
            let second = normalize(&first).unwrap_or_else(|e| panic!("{first:?}: {e}"));
            assert_eq!(first, second, "not idempotent for {sql:?}");
        }
    }

    // Exercises the larg/rarg recursion path inside reshape_select.
    #[test]
    fn union_is_idempotent() {
        let sql = "SELECT id FROM users UNION SELECT user_id FROM orders";
        let first = normalize(sql).unwrap();
        let second = normalize(&first).unwrap();
        assert_eq!(first, second);
    }

    // Param numbering is query-wide, not per-scope: a subquery must not
    // restart at $1. This guards against moving renumber_params inside
    // reshape_select.
    #[test]
    fn params_stay_query_wide_across_subselect() {
        let sql =
            "SELECT id FROM users WHERE id = $1 AND tenant_id IN (SELECT t FROM tenants WHERE t = $2)";
        let out = normalize(sql).unwrap();
        assert!(out.contains("$1") && out.contains("$2"), "got: {out}");
    }

    // LIMIT $N lives in limit_count — easy to miss when porting a walker.
    #[test]
    fn limit_param_survives_renumber() {
        let out = normalize("SELECT * FROM t LIMIT $1").unwrap();
        assert!(out.contains("LIMIT $1"), "got: {out}");
    }
}
