//! Shared CLI helpers

use std::fs;
use std::io::{self, Read};

use anyhow::{Context, Result};
use qshape_core::{ClustersDoc, load_clusters_doc};

/// Load and validate clusters.json from `--in <path>` or stdin
pub fn load_clusters(in_path: Option<&str>) -> Result<ClustersDoc> {
    let bytes = match in_path {
        Some(p) => fs::read(p).with_context(|| format!("read {p}"))?,
        None => {
            let mut buf = Vec::new();
            io::stdin().read_to_end(&mut buf).context("read stdin")?;
            buf
        }
    };
    load_clusters_doc(&bytes).context("decode clusters.json")
}
