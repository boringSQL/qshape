//! Attribute $N placeholders to table.column by running EXPLAIN(GENERIC_PLAN)
//! on each cluster's canonical and walking the plan tree

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Write};
use std::sync::OnceLock;

use anyhow::{Context, Result, anyhow};
use postgres::{Client, NoTls, SimpleQueryMessage};
use qshape_core::{
    CURRENT_SCHEMA_VERSION, ParamAttribution, cast_func_param_refs, load_clusters_doc,
    max_param_number, normalize,
};
use regex::Regex;
use serde::Deserialize;

use crate::typecast::TypecastCache;

pub fn run(in_path: Option<&str>, conn_str: &str, top: usize) -> Result<()> {
    let bytes = match in_path {
        Some(p) => fs::read(p).with_context(|| format!("read {p}"))?,
        None => {
            let mut buf = Vec::new();
            io::stdin().read_to_end(&mut buf).context("read stdin")?;
            buf
        }
    };
    let mut doc = load_clusters_doc(&bytes).context("decode clusters.json")?;

    let mut client = Client::connect(conn_str, NoTls).context("connect")?;
    let mut cache = TypecastCache::new(&mut client);
    let mut attributed = 0usize;
    let mut skipped = 0usize;

    let limit = if top == 0 { doc.clusters.len() } else { top.min(doc.clusters.len()) };
    for c in doc.clusters.iter_mut().take(limit) {
        if c.fingerprint.is_empty() || c.canonical.is_empty() {
            continue;
        }
        match attribute_cluster(&mut cache, &c.canonical) {
            Ok(params) if params.is_empty() => skipped += 1,
            Ok(params) => {
                c.params = params;
                attributed += 1;
            }
            Err(e) => {
                skipped += 1;
                c.params = vec![ParamAttribution {
                    confidence: "none".to_string(),
                    note: e.to_string(),
                    ..ParamAttribution::default()
                }];
            }
        }
    }

    eprintln!("attributed {attributed} clusters, {skipped} skipped");
    doc.schema_version = CURRENT_SCHEMA_VERSION.to_string();
    let stdout = io::stdout();
    let mut h = stdout.lock();
    serde_json::to_writer_pretty(&mut h, &doc)?;
    h.write_all(b"\n")?;
    Ok(())
}

fn attribute_cluster(
    cache: &mut TypecastCache<'_>,
    canonical: &str,
) -> Result<Vec<ParamAttribution>> {
    // re-normalize so older clusters.json picks up current reshape fixes
    let canonical = normalize(canonical).unwrap_or_else(|_| canonical.to_string());
    let explain_sql = cast_func_param_refs(&canonical, &mut cache.lookup());
    let nparams = max_param_number(&explain_sql);

    // PREPARE + EXPLAIN EXECUTE gives PG param context for $N.
    // force_generic_plan keeps $N in plan output instead of inlining
    // NULL args. SET LOCAL needs transaction, hence BEGIN/COMMIT
    let mut script = String::from(
        "BEGIN;\nSET LOCAL plan_cache_mode = force_generic_plan;\nPREPARE _qshape_tmp AS ",
    );
    script.push_str(&explain_sql);
    script.push_str(";\n");
    if nparams > 0 {
        script.push_str("EXPLAIN (FORMAT JSON) EXECUTE _qshape_tmp(");
        for i in 0..nparams {
            if i > 0 {
                script.push_str(", ");
            }
            script.push_str("NULL");
        }
        script.push_str(");\n");
    } else {
        script.push_str("EXPLAIN (FORMAT JSON) EXECUTE _qshape_tmp;\n");
    }
    script.push_str("DEALLOCATE _qshape_tmp;\nCOMMIT;");

    let plan_json = match read_plan_json(cache.client_mut(), &script) {
        Ok(v) => v,
        Err(e) => {
            // mid-batch error aborts BEGIN, reset connection
            let _ = cache.client_mut().simple_query("ROLLBACK");
            let _ = cache.client_mut().simple_query("DEALLOCATE IF EXISTS _qshape_tmp");
            return Err(e);
        }
    };

    #[derive(Deserialize)]
    struct PlanWrapper {
        #[serde(rename = "Plan")]
        plan: serde_json::Value,
    }
    let plans: Vec<PlanWrapper> =
        serde_json::from_str(&plan_json).context("parse EXPLAIN JSON")?;
    let Some(first) = plans.into_iter().next() else { return Ok(Vec::new()) };

    let mut ctx = AttrCtx { by_position: HashMap::new() };
    walk_plan(&first.plan, "", "", &mut ctx);

    let mut out: Vec<ParamAttribution> = ctx.by_position.into_values().collect();
    out.sort_by_key(|p| p.position);
    Ok(out)
}

fn read_plan_json(client: &mut Client, script: &str) -> Result<String> {
    let messages = client.simple_query(script).context("EXPLAIN script")?;
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg
            && let Some(text) = row.get(0)
        {
            return Ok(text.to_string());
        }
    }
    Err(anyhow!("EXPLAIN returned no rows"))
}

struct AttrCtx {
    by_position: HashMap<i32, ParamAttribution>,
}

#[derive(Default)]
struct TableRef {
    schema: String,
    table: String,
}

#[derive(Deserialize, Default)]
struct PlanNode {
    #[serde(rename = "Schema", default)]
    schema: String,
    #[serde(rename = "Relation Name", default)]
    relation_name: String,
    #[serde(rename = "Alias", default)]
    alias: String,
    #[serde(rename = "Filter", default)]
    filter: String,
    #[serde(rename = "Index Cond", default)]
    index_cond: String,
    #[serde(rename = "Hash Cond", default)]
    hash_cond: String,
    #[serde(rename = "Recheck Cond", default)]
    recheck_cond: String,
    #[serde(rename = "Join Filter", default)]
    join_filter: String,
    #[serde(rename = "Merge Cond", default)]
    merge_cond: String,
    #[serde(rename = "Plans", default)]
    plans: Vec<serde_json::Value>,
}

fn walk_plan(raw: &serde_json::Value, _parent_schema: &str, _parent_table: &str, ctx: &mut AttrCtx) {
    let Ok(n) = serde_json::from_value::<PlanNode>(raw.clone()) else { return };

    // alias to table so u.id = $1 resolves to users.id. Function Scan
    // on system view leaves Relation Name empty but sets Alias — use
    // alias as table so `(name = $1)` attributes correctly
    let mut aliases: HashMap<String, TableRef> = HashMap::new();
    let mut fallback_table = n.relation_name.clone();
    let fallback_schema = n.schema.clone();
    if !n.relation_name.is_empty() {
        aliases.insert(
            n.relation_name.clone(),
            TableRef { schema: n.schema.clone(), table: n.relation_name.clone() },
        );
        if !n.alias.is_empty() && n.alias != n.relation_name {
            aliases.insert(
                n.alias.clone(),
                TableRef { schema: n.schema.clone(), table: n.relation_name.clone() },
            );
        }
    } else if !n.alias.is_empty() {
        aliases.insert(
            n.alias.clone(),
            TableRef { schema: n.schema.clone(), table: n.alias.clone() },
        );
        fallback_table = n.alias.clone();
    }

    for cond in [
        &n.index_cond,
        &n.hash_cond,
        &n.filter,
        &n.recheck_cond,
        &n.join_filter,
        &n.merge_cond,
    ] {
        if cond.is_empty() {
            continue;
        }
        attribute_cond(cond, &aliases, &fallback_schema, &fallback_table, ctx);
    }

    for child in &n.plans {
        walk_plan(child, &n.schema, &n.relation_name, ctx);
    }
}

// column op $N or $N op column, alias.column optional
const PARAM_COND_PATTERN: &str = r"(?:\(?(\w+)\.)?(\w+)\s*(?:=|<|>|<=|>=|<>|!=)\s*\$(\d+)|\$(\d+)\s*(?:=|<|>|<=|>=|<>|!=)\s*(?:\(?(\w+)\.)?(\w+)";
// column IN ($N, ...), capture only first param
const PARAM_IN_PATTERN: &str = r"(?:\(?(\w+)\.)?(\w+)\s+(?:=\s*ANY\s*\()?IN\s*\(\s*\$(\d+)";

fn param_cond_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(PARAM_COND_PATTERN).expect("static regex"))
}

fn param_in_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(PARAM_IN_PATTERN).expect("static regex"))
}

fn attribute_cond(
    cond: &str,
    aliases: &HashMap<String, TableRef>,
    fallback_schema: &str,
    fallback_table: &str,
    ctx: &mut AttrCtx,
) {
    for caps in param_cond_re().captures_iter(cond) {
        // first alt: [1]=alias [2]=col [3]=pos
        // second:    [4]=pos   [5]=alias [6]=col
        let (alias_or_table, col, pos) = match caps.get(3) {
            Some(p) => (
                caps.get(1).map_or("", |m| m.as_str()),
                caps.get(2).map_or("", |m| m.as_str()),
                p.as_str(),
            ),
            None => match caps.get(4) {
                Some(p) => (
                    caps.get(5).map_or("", |m| m.as_str()),
                    caps.get(6).map_or("", |m| m.as_str()),
                    p.as_str(),
                ),
                None => continue,
            },
        };
        record_param(alias_or_table, col, pos, aliases, fallback_schema, fallback_table, ctx);
    }
    for caps in param_in_re().captures_iter(cond) {
        let alias_or_table = caps.get(1).map_or("", |m| m.as_str());
        let col = caps.get(2).map_or("", |m| m.as_str());
        let pos = caps.get(3).map_or("", |m| m.as_str());
        record_param(alias_or_table, col, pos, aliases, fallback_schema, fallback_table, ctx);
    }
}

fn record_param(
    alias_or_table: &str,
    col: &str,
    pos_str: &str,
    aliases: &HashMap<String, TableRef>,
    fallback_schema: &str,
    fallback_table: &str,
    ctx: &mut AttrCtx,
) {
    let Ok(pos) = pos_str.parse::<i32>() else { return };
    if let Some(existing) = ctx.by_position.get(&pos)
        && existing.confidence == "exact"
    {
        return;
    }

    let (schema, table, confidence) = if let Some(t) = aliases.get(alias_or_table) {
        (t.schema.clone(), t.table.clone(), "exact")
    } else if !fallback_table.is_empty() {
        // unqualified column on scan node = exact (PG telling us
        // which scan column belongs to). Downgrade only when
        // qualifier failed to resolve
        let conf = if alias_or_table.is_empty() { "exact" } else { "heuristic" };
        (fallback_schema.to_string(), fallback_table.to_string(), conf)
    } else {
        (String::new(), String::new(), "none")
    };

    ctx.by_position.insert(
        pos,
        ParamAttribution {
            position: pos,
            schema,
            table,
            column: col.to_string(),
            confidence: confidence.to_string(),
            note: String::new(),
        },
    );
}
