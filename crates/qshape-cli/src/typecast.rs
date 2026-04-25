//! pg_proc-backed cache for `qshape_core::cast_func_param_refs`
#![allow(dead_code)]

use std::collections::HashMap;

use postgres::Client;

const PG_PROC_SQL: &str = include_str!("../sql/pg_proc.sql");

pub struct TypecastCache<'a> {
    client: &'a mut Client,
    cache: HashMap<(String, String, i32), Option<Vec<String>>>,
}

impl<'a> TypecastCache<'a> {
    pub fn new(client: &'a mut Client) -> Self {
        Self { client, cache: HashMap::new() }
    }

    // closure adapter for `qshape_core::cast_func_param_refs`
    pub fn lookup(&mut self) -> impl FnMut(&str, &str, usize) -> Option<Vec<String>> + '_ {
        |schema: &str, name: &str, nargs: usize| {
            let key = (schema.to_string(), name.to_string(), nargs as i32);
            if let Some(hit) = self.cache.get(&key) {
                return hit.clone();
            }
            let resolved = query_pg_proc(self.client, schema, name, nargs as i32);
            self.cache.insert(key, resolved.clone());
            resolved
        }
    }
}

fn query_pg_proc(
    client: &mut Client,
    schema: &str,
    name: &str,
    nargs: i32,
) -> Option<Vec<String>> {
    let rows = client.query(PG_PROC_SQL, &[&schema, &name, &nargs]).ok()?;
    if rows.len() != 1 {
        // 0 = unknown, >1 = ambiguous overload; in either case give up.
        return None;
    }
    rows[0].try_get::<_, Vec<String>>(0).ok()
}
