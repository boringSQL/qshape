use std::collections::{BTreeSet, HashMap};
use std::string::String;

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
    // first nested scoped
    reshape_nested_from(&mut s.from_clause);
    if let Some(w) = s.with_clause.as_mut() {
        reshape_with_clause(w);
    }
    if let Some(l) = s.larg.as_deref_mut() {
        reshape_select(l);
    }
    if let Some(r) = s.rarg.as_deref_mut() {
        reshape_select(r);
    }
    reshape_sublinks_in_select(s);

    // current scope
    let entries = collect_scope(&s.from_clause);
    let dec = decorative_aliases(&entries);
    if !dec.is_empty() {
        strip_aliases_in_from(&mut s.from_clause, &dec);
        for n in &mut s.target_list {
            rewrite_refs(n, &dec);
        }
        if let Some(w) = s.where_clause.as_deref_mut() {
            rewrite_refs(w, &dec);
        }
        if let Some(h) = s.having_clause.as_deref_mut() {
            rewrite_refs(h, &dec);
        }
        for n in &mut s.group_clause {
            rewrite_refs(n, &dec);
        }
        for n in &mut s.sort_clause {
            rewrite_refs(n, &dec);
        }
        for n in &mut s.distinct_clause {
            rewrite_refs(n, &dec);
        }
        if let Some(n) = s.limit_offset.as_deref_mut() {
            rewrite_refs(n, &dec);
        }
        if let Some(n) = s.limit_count.as_deref_mut() {
            rewrite_refs(n, &dec);
        }
        // JoinExpr.Quals live inside FROM clause nodes
        for n in &mut s.from_clause {
            rewrite_refs(n, &dec);
        }
    }

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
    for n in &mut u.target_list {
        reshape_sublinks(n);
    }
    if let Some(w) = u.where_clause.as_deref_mut() {
        reshape_sublinks(w);
    }
    for n in &mut u.returning_list {
        reshape_sublinks(n);
    }

    let mut entries: Vec<ScopeEntry> = Vec::new();
    if let Some(rv) = u.relation.as_ref() {
        entries.push(range_var_entry(rv));
    }
    for n in &u.from_clause {
        collect_from_item(n, &mut entries);
    }
    let dec = decorative_aliases(&entries);
    if !dec.is_empty() {
        if let Some(rv) = u.relation.as_mut()
            && let Some(a) = rv.alias.as_ref()
            && dec.contains_key(&a.aliasname)
        {
            rv.alias = None;
        }
        strip_aliases_in_from(&mut u.from_clause, &dec);
        for n in &mut u.target_list {
            rewrite_refs(n, &dec);
        }
        if let Some(w) = u.where_clause.as_deref_mut() {
            rewrite_refs(w, &dec);
        }
        for n in &mut u.returning_list {
            rewrite_refs(n, &dec);
        }
        for n in &mut u.from_clause {
            rewrite_refs(n, &dec);
        }
    }

    if let Some(w) = u.where_clause.as_deref_mut() {
        sort_and_tree(w);
    }
}

fn reshape_delete(d: &mut DeleteStmt) {
    if let Some(w) = d.with_clause.as_mut() {
        reshape_with_clause(w);
    }
    reshape_nested_from(&mut d.using_clause);
    if let Some(w) = d.where_clause.as_deref_mut() {
        reshape_sublinks(w);
    }
    for n in &mut d.returning_list {
        reshape_sublinks(n);
    }

    let mut entries: Vec<ScopeEntry> = Vec::new();
    if let Some(rv) = d.relation.as_ref() {
        entries.push(range_var_entry(rv));
    }
    for n in &d.using_clause {
        collect_from_item(n, &mut entries);
    }
    let dec = decorative_aliases(&entries);
    if !dec.is_empty() {
        if let Some(rv) = d.relation.as_mut()
            && let Some(a) = rv.alias.as_ref()
            && dec.contains_key(&a.aliasname)
        {
            rv.alias = None;
        }
        strip_aliases_in_from(&mut d.using_clause, &dec);
        if let Some(w) = d.where_clause.as_deref_mut() {
            rewrite_refs(w, &dec);
        }
        for n in &mut d.returning_list {
            rewrite_refs(n, &dec);
        }
        for n in &mut d.using_clause {
            rewrite_refs(n, &dec);
        }
    }

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

// --- sublink reshape -------------------------------------------------------

fn reshape_sublinks_in_select(s: &mut SelectStmt) {
    if let Some(w) = s.where_clause.as_deref_mut() {
        reshape_sublinks(w);
    }
    if let Some(h) = s.having_clause.as_deref_mut() {
        reshape_sublinks(h);
    }
    for n in &mut s.target_list {
        reshape_sublinks(n);
    }
    for n in &mut s.from_clause {
        reshape_join_quals_sublinks(n);
    }
}

fn reshape_join_quals_sublinks(n: &mut Node) {
    if let Some(NodeEnum::JoinExpr(j)) = n.node.as_mut() {
        if let Some(q) = j.quals.as_deref_mut() {
            reshape_sublinks(q);
        }
        if let Some(l) = j.larg.as_deref_mut() {
            reshape_join_quals_sublinks(l);
        }
        if let Some(r) = j.rarg.as_deref_mut() {
            reshape_join_quals_sublinks(r);
        }
    }
}

// Reshape all sublinks and recures rest. Go source actually
// missed sublinks in functions like COALESCE, ARRAY, ROW and
// aggregate FILTER/ORDER BY
fn reshape_sublinks(n: &mut Node) {
    match n.node.as_mut() {
        Some(NodeEnum::SubLink(sl)) => {
            if let Some(sub) = sl.subselect.as_deref_mut() {
                reshape_node(sub);
            }
            if let Some(t) = sl.testexpr.as_deref_mut() {
                reshape_sublinks(t);
            }
        }
        _ => for_each_child(n, &mut reshape_sublinks),
    }
}

// --- scope collection + alias stripping -------------------------------------

struct ScopeEntry {
    relname: String,
    alias_name: String,
    required: bool,
}

// Dec for alias replacement nodes.
// - empty means drop alias
// - non-empty means replace aliases with those nodes
type Dec = HashMap<String, Vec<Node>>;

fn collect_scope(items: &[Node]) -> Vec<ScopeEntry> {
    let mut out = Vec::new();
    for n in items {
        collect_from_item(n, &mut out);
    }
    out
}

fn collect_from_item(n: &Node, out: &mut Vec<ScopeEntry>) {
    match n.node.as_ref() {
        Some(NodeEnum::RangeVar(rv)) => out.push(range_var_entry(rv)),
        Some(NodeEnum::JoinExpr(j)) => {
            if let Some(l) = j.larg.as_deref() {
                collect_from_item(l, out);
            }
            if let Some(r) = j.rarg.as_deref() {
                collect_from_item(r, out);
            }
        }
        Some(NodeEnum::RangeSubselect(rs)) => {
            let alias = rs
                .alias
                .as_ref()
                .map(|a| a.aliasname.clone())
                .unwrap_or_default();
            out.push(ScopeEntry {
                relname: String::new(),
                alias_name: alias,
                required: true,
            });
        }
        Some(NodeEnum::RangeFunction(rf)) => {
            let alias = rf
                .alias
                .as_ref()
                .map(|a| a.aliasname.clone())
                .unwrap_or_default();
            out.push(ScopeEntry {
                relname: String::new(),
                alias_name: alias,
                required: true,
            });
        }
        _ => {}
    }
}

fn range_var_entry(rv: &RangeVar) -> ScopeEntry {
    let alias_name = match rv.alias.as_ref() {
        Some(a) if a.colnames.is_empty() => a.aliasname.clone(),
        _ => String::new(),
    };
    ScopeEntry {
        relname: rv.relname.clone(),
        alias_name,
        required: false,
    }
}

fn decorative_aliases(entries: &[ScopeEntry]) -> Dec {
    let mut rel_count: HashMap<&str, usize> = HashMap::new();
    for e in entries {
        if !e.relname.is_empty() {
            *rel_count.entry(&e.relname).or_insert(0) += 1;
        }
    }

    let single_relation = entries.len() == 1;
    let mut out: Dec = HashMap::new();

    for e in entries {
        if e.alias_name.is_empty() || e.required {
            continue;
        }
        if !e.relname.is_empty() && rel_count.get(e.relname.as_str()).copied().unwrap_or(0) > 1 {
            continue;
        }
        if single_relation {
            out.insert(e.alias_name.clone(), Vec::new());
        } else if !e.relname.is_empty() {
            out.insert(e.alias_name.clone(), vec![string_node(&e.relname)]);
        }
    }
    out
}

fn string_node(s: &str) -> Node {
    Node {
        node: Some(NodeEnum::String(pg_query::protobuf::String {
            sval: s.to_string(),
        })),
    }
}

fn strip_aliases_in_from(items: &mut [Node], dec: &Dec) {
    for n in items {
        strip_alias_in_from_item(n, dec);
    }
}

fn strip_alias_in_from_item(n: &mut Node, dec: &Dec) {
    match n.node.as_mut() {
        Some(NodeEnum::RangeVar(rv)) => {
            if let Some(a) = rv.alias.as_ref()
                && dec.contains_key(&a.aliasname)
            {
                rv.alias = None;
            }
        }
        Some(NodeEnum::JoinExpr(j)) => {
            if let Some(l) = j.larg.as_deref_mut() {
                strip_alias_in_from_item(l, dec);
            }
            if let Some(r) = j.rarg.as_deref_mut() {
                strip_alias_in_from_item(r, dec);
            }
        }
        _ => {}
    }
}

fn rewrite_refs(n: &mut Node, dec: &Dec) {
    match n.node.as_mut() {
        Some(NodeEnum::ColumnRef(cr)) => rewrite_column_ref(cr, dec),
        Some(NodeEnum::SubLink(sl)) => {
            if let Some(sub) = sl.subselect.as_deref_mut()
                && let Some(NodeEnum::SelectStmt(s)) = sub.node.as_mut()
            {
                rewrite_in_select(s, dec);
            }
            if let Some(t) = sl.testexpr.as_deref_mut() {
                rewrite_refs(t, dec);
            }
        }
        Some(NodeEnum::RangeSubselect(rs)) => {
            // LATERAL / derived-table subqueries may reference outer-scope
            // aliases; their own scope was reshaped first, so any `u.x`
            // left here is an outer-scope reference
            if let Some(sub) = rs.subquery.as_deref_mut()
                && let Some(NodeEnum::SelectStmt(s)) = sub.node.as_mut()
            {
                rewrite_in_select(s, dec);
            }
        }
        _ => for_each_child(n, &mut |c| rewrite_refs(c, dec)),
    }
}

fn rewrite_column_ref(cr: &mut ColumnRef, dec: &Dec) {
    if cr.fields.len() < 2 {
        return;
    }
    let Some(NodeEnum::String(s)) = cr.fields[0].node.as_ref() else {
        return;
    };
    let Some(repl) = dec.get(&s.sval) else {
        return;
    };
    let rest: Vec<Node> = cr.fields.drain(1..).collect();
    cr.fields.clear();
    cr.fields.extend(repl.iter().cloned());
    cr.fields.extend(rest);
}

fn rewrite_in_select(s: &mut SelectStmt, dec: &Dec) {
    let mut f: Box<dyn FnMut(&mut Node)> = Box::new(|n| rewrite_refs(n, dec));
    visit_all(&mut s.target_list, &mut f);
    visit(s.where_clause.as_deref_mut(), &mut f);
    visit(s.having_clause.as_deref_mut(), &mut f);
    visit_all(&mut s.group_clause, &mut f);
    visit_all(&mut s.sort_clause, &mut f);
    visit_all(&mut s.distinct_clause, &mut f);
    visit_all(&mut s.from_clause, &mut f);
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
        Some(NodeEnum::AConst(AConst {
            val: Some(a_const::Val::Sval(s)),
            ..
        })) => s.sval.clone(),
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
        Some(NodeEnum::FuncCall(fc)) => {
            visit_all(&mut fc.args, f);
            visit(fc.agg_filter.as_deref_mut(), f);
            visit_all(&mut fc.agg_order, f);
        }
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
        Some(NodeEnum::AArrayExpr(a)) => visit_all(&mut a.elements, f),
        Some(NodeEnum::RowExpr(r)) => visit_all(&mut r.args, f),
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
        let sql = "SELECT id FROM users WHERE id = $1 AND tenant_id IN (SELECT t FROM tenants WHERE t = $2)";
        let out = normalize(sql).unwrap();
        assert!(out.contains("$1") && out.contains("$2"), "got: {out}");
    }

    // LIMIT $N lives in limit_count — easy to miss when porting a walker.
    #[test]
    fn limit_param_survives_renumber() {
        let out = normalize("SELECT * FROM t LIMIT $1").unwrap();
        assert!(out.contains("LIMIT $1"), "got: {out}");
    }

    // EXTRACT's field must deparse as a bare identifier, not a quoted
    // string. Otherwise downstream literal-parameterisers break the SQL.
    #[test]
    fn extract_field_stays_identifier() {
        let out = normalize("SELECT auth.f(extract(epoch FROM '1 hour'::interval)::bigint, 100)")
            .unwrap();
        assert!(out.contains("extract (epoch FROM"), "got: {out}");
        assert!(
            !out.contains("'epoch'"),
            "field leaked as string literal: {out}"
        );
    }

    // If the field arrived already parameterised, substitute a stable
    // ident and let renumber_params close the resulting gap.
    #[test]
    fn extract_param_field_recovered() {
        let out =
            normalize("SELECT auth.clean_up_sessions(extract($1 FROM $2::interval)::bigint, $3)")
                .unwrap();
        let want = "SELECT auth.clean_up_sessions(extract (epoch FROM $1::interval)::bigint, $2)";
        assert_eq!(out, want);
    }

    #[test]
    fn extract_recovery_is_idempotent() {
        let sql = "SELECT extract($1 FROM $2::interval), $3 FROM t";
        let first = normalize(sql).unwrap();
        let second = normalize(&first).unwrap();
        assert_eq!(first, second);
    }

    // --- SELECT alias stripping (ported from reshape_test.go) ---------------

    use crate::fingerprint::fingerprint;

    #[test]
    fn strips_single_table_alias() {
        let got = normalize("SELECT u.id, u.name FROM users u WHERE u.id = $1").unwrap();
        assert_eq!(got, "SELECT id, name FROM users WHERE id = $1");
    }

    #[test]
    fn leaves_unaliased_unchanged() {
        let got = normalize("SELECT id FROM users WHERE id = $1").unwrap();
        assert_eq!(got, "SELECT id FROM users WHERE id = $1");
    }

    #[test]
    fn orm_variants_collapse() {
        let a = fingerprint("SELECT id, name FROM users WHERE id = $1").unwrap();
        let b = fingerprint("SELECT u.id, u.name FROM users u WHERE u.id = $1").unwrap();
        assert_eq!(a, b, "aliased and unaliased should fingerprint the same");
    }

    #[test]
    fn join_canonicalises_aliases_to_relname() {
        // Multi-relation scope: can't strip to bare (ambiguous) but can
        // canonicalise `u.col` / `users.col` to the same form.
        let got = normalize(
            "SELECT u.id, o.total FROM users u INNER JOIN orders o ON o.user_id = u.id WHERE u.tenant = $1",
        )
        .unwrap();
        let want = "SELECT users.id, orders.total FROM users JOIN orders ON orders.user_id = users.id WHERE users.tenant = $1";
        assert_eq!(got, want);
    }

    #[test]
    fn join_aliased_and_unaliased_collapse() {
        let a = fingerprint("SELECT u.id, o.total FROM users u JOIN orders o ON o.user_id = u.id")
            .unwrap();
        let b = fingerprint(
            "SELECT users.id, orders.total FROM users JOIN orders ON orders.user_id = users.id",
        )
        .unwrap();
        assert_eq!(a, b, "alias and relname variants should collapse");
    }

    #[test]
    fn self_join_preserves_aliases() {
        // Both aliases must stay — otherwise the join is ambiguous.
        let got =
            normalize("SELECT a.id FROM users a INNER JOIN users b ON a.id = b.parent_id").unwrap();
        assert_eq!(
            got,
            "SELECT a.id FROM users a JOIN users b ON a.id = b.parent_id"
        );
    }

    #[test]
    fn range_subselect_alias_required() {
        // SQL syntactically requires the alias on a subselect in FROM.
        let got = normalize("SELECT s.id FROM (SELECT id FROM users) s").unwrap();
        assert_eq!(got, "SELECT s.id FROM (SELECT id FROM users) s");
    }

    #[test]
    fn correlated_subquery_rewrites_outer_ref() {
        // Outer `u` stripped; correlated `u.id` inside subquery must also
        // be rewritten, or the deparsed SQL references a missing alias.
        let got = normalize(
            "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
        )
        .unwrap();
        assert_eq!(
            got,
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)"
        );
    }

    #[test]
    fn cte_gets_own_scope() {
        let got = normalize(
            "WITH recent AS (SELECT u.id FROM users u WHERE u.created_at > $1) SELECT r.id FROM recent r",
        )
        .unwrap();
        assert_eq!(
            got,
            "WITH recent AS (SELECT id FROM users WHERE created_at > $1) SELECT id FROM recent"
        );
    }

    #[test]
    fn cte_used_in_outer_join_keeps_aliases() {
        // Outer scope has the CTE ref + another table; both expose user_id.
        // Stripping the `s` alias would leave an ambiguous bare `user_id`.
        let in_sql = "WITH ur AS (SELECT ua.user_id FROM auth.user_account ua WHERE ua.user_id = $1) \
                      SELECT ur.user_id FROM ur JOIN sessions s ON s.user_id = ur.user_id";
        let got = normalize(in_sql).unwrap();
        let want = "WITH ur AS (SELECT user_id FROM auth.user_account WHERE user_id = $1) \
                    SELECT ur.user_id FROM ur JOIN sessions ON sessions.user_id = ur.user_id";
        assert_eq!(got, want);
    }

    #[test]
    fn comma_join_canonicalises_aliases() {
        let got = normalize("SELECT a.id FROM users a, orders b WHERE a.id = b.user_id").unwrap();
        assert_eq!(
            got,
            "SELECT users.id FROM users, orders WHERE users.id = orders.user_id"
        );
    }

    #[test]
    fn union_both_arms() {
        let a = normalize("SELECT u.id FROM users u UNION SELECT o.user_id FROM orders o").unwrap();
        let b = normalize("SELECT id FROM users UNION SELECT user_id FROM orders").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn correlated_ref_in_nested_subquery_from() {
        // Outer alias `u` (decorative, stripped in outer scope) is referenced
        // from a derived-table WHERE that lives inside a correlated subquery's
        // FROM. rewrite_in_select must walk the subquery's FROM, not just its
        // target list / WHERE, or the outer ref is left dangling.
        let got = normalize(
            "SELECT u.id, (SELECT x FROM (SELECT a.x FROM t a WHERE a.uid = u.id) sub) FROM updated u JOIN t2 p ON p.uid = u.id",
        )
        .unwrap();
        assert!(!got.contains("u."), "outer alias `u` leaked: {got}");
    }

    #[test]
    fn coalesce_args_get_rewritten() {
        // COALESCE is parsed as CoalesceExpr, not FuncCall. rewrite_refs
        // must walk its args or aliases inside COALESCE survive FROM
        // stripping and deparse references a missing table.
        let got = normalize(
            "SELECT COALESCE(CAST(o.options -> $1 AS boolean), $2) FROM org.organization o LEFT JOIN org.workspace ON organization.id = workspace.org_id",
        )
        .unwrap();
        assert!(!got.contains("o.options"), "o.options not rewritten: {got}");
    }

    // --- UPDATE/DELETE alias stripping --------------------------------------

    #[test]
    fn update_strips_alias() {
        let got = normalize("UPDATE users u SET name = $1 WHERE u.id = $2").unwrap();
        assert_eq!(got, "UPDATE users SET name = $1 WHERE id = $2");
    }

    #[test]
    fn delete_strips_alias() {
        let got = normalize("DELETE FROM users u WHERE u.id = $1").unwrap();
        assert_eq!(got, "DELETE FROM users WHERE id = $1");
    }

    #[test]
    fn update_with_from_canonicalises_aliases() {
        let got = normalize("UPDATE users u SET name = $1 FROM orders o WHERE o.user_id = u.id")
            .unwrap();
        assert_eq!(
            got,
            "UPDATE users SET name = $1 FROM orders WHERE orders.user_id = users.id"
        );
    }

    // INSERT statements have their own WithClause — fixup walker must descend.
    #[test]
    fn insert_with_clause_gets_fixups() {
        let sql = "WITH c AS (SELECT r FROM t gs WHERE extract($1 FROM gs) NOT IN ($2, $3)) INSERT INTO x SELECT * FROM c";
        let got = normalize(sql).unwrap();
        assert!(!got.contains("extract($") && !got.contains("extract ($"), "got: {got}");
    }

    // extract() nested inside COALESCE/round() — fixup must descend into
    // CoalesceExpr to reach the inner FuncCall.
    #[test]
    fn extract_inside_coalesce_gets_fixed() {
        let got = normalize("SELECT COALESCE(round(extract($1 FROM col)), 0) FROM t").unwrap();
        assert!(!got.contains("extract($") && !got.contains("extract ($"), "got: {got}");
    }

    // CoalesceExpr → FuncCall → FuncCall → ColumnRef. Before fix, outer
    // CoalesceExpr was skipped so c.oid was never reached.
    #[test]
    fn coalesce_with_nested_func_call_ref() {
        let got = normalize(
            "SELECT COALESCE(sum(pg_stat_get_live_tuples(c.oid)), $1) FROM pg_class c LEFT JOIN pg_namespace ON pg_namespace.oid = c.relnamespace WHERE c.relkind = $2",
        )
        .unwrap();
        assert!(!got.contains("c.oid"), "c.oid not rewritten: {got}");
    }

    #[test]
    fn agg_filter_and_case_expr_refs_rewritten() {
        // FILTER lands in FuncCall.agg_filter, not args. Also exercises
        // alias refs inside a CASE inside array_agg.
        let got = normalize(
            "WITH r AS (SELECT COALESCE(array_agg(DISTINCT CASE wu.role WHEN $1 THEN 'a' END) FILTER (WHERE wu.role IS NOT NULL), ARRAY[]::text[]) AS roles FROM org.workspace_user wu JOIN org.organization_user ON wu.org_user_id = organization_user.id GROUP BY wu.org_user_id) SELECT * FROM r",
        )
        .unwrap();
        assert!(!got.contains("wu."), "wu. refs not rewritten: {got}");
    }
}
