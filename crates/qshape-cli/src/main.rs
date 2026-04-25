use std::io::{self, Read};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

mod attribute;
mod capture;
mod loader;
mod regresql_stub;
mod typecast;

#[derive(Debug, Parser)]
#[command(
    name = "qshape",
    version,
    about = "Normalize, fingerprint, and cluster PostgreSQL queries"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the qshape version.
    Version,

    /// Fingerprint a SQL statement (pass `-` or omit to read stdin).
    Fingerprint {
        /// SQL to fingerprint; `-` or absent reads stdin.
        sql: Option<String>,
    },

    /// Normalize a SQL statement (pass `-` or omit to read stdin).
    Normalize {
        /// SQL to normalize; `-` or absent reads stdin.
        sql: Option<String>,
    },

    /// Attribute $N placeholders in a clusters.json to table.column
    ///
    /// Reads clusters.json (from --in or stdin), runs EXPLAIN(GENERIC_PLAN)
    /// on each cluster's canonical SQL via PREPARE+EXPLAIN EXECUTE, walks
    /// the plan tree to map $N to (schema, table, column), and writes the
    /// input back to stdout with a `params` array per cluster
    Attribute {
        /// PostgreSQL connection string
        #[arg(long)]
        conn: String,
        /// Input clusters.json (default: stdin)
        #[arg(long)]
        r#in: Option<String>,
        /// Only attribute the top N clusters (0 = all)
        #[arg(long, default_value_t = 0)]
        top: usize,
    },

    /// Generate regresql sql/ skeletons from clusters.json
    ///
    /// For each of the top N clusters, writes <out>/sql/<slug>.sql with
    /// the canonical SQL ($N rewritten to :paramN) and a header carrying
    /// fingerprint + call count
    RegresqlStub {
        /// Input clusters.json (default: stdin)
        #[arg(long)]
        r#in: Option<String>,
        /// Output directory
        #[arg(long, default_value = "regresql-stubs")]
        out: String,
        /// Number of top clusters to emit
        #[arg(long, default_value_t = 10)]
        top: usize,
        /// Skip clusters with total_calls <= this
        #[arg(long, default_value_t = 0)]
        min_calls: i64,
    },

    /// Fetch pg_stat_statements (with timing) from a live PG and cluster it
    Capture {
        /// libpq connection string.
        conn: String,
        /// Exclude queries with calls <= this value.
        #[arg(long, default_value_t = 0)]
        min_calls: i64,
        /// Limit to top N by total_exec_time (0 = no limit).
        #[arg(long, default_value_t = 0)]
        limit: i32,
        /// Filter to a specific database name (default: all).
        #[arg(long)]
        database: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
        }
        Command::Fingerprint { sql } => {
            let input = match sql.as_deref() {
                Some("-") | None => read_stdin()?,
                Some(s) => s.to_string(),
            };
            let fp = qshape_core::fingerprint(input.trim())?;
            println!("{fp}");
        }
        Command::Normalize { sql } => {
            let input = match sql.as_deref() {
                Some("-") | None => read_stdin()?,
                Some(s) => s.to_string(),
            };
            let out = qshape_core::normalize(input.trim())?;
            println!("{out}");
        }
        Command::Capture { conn, min_calls, limit, database } => {
            capture::run(&conn, min_calls, limit, database.as_deref())?;
        }
        Command::Attribute { conn, r#in, top } => {
            attribute::run(r#in.as_deref(), &conn, top)?;
        }
        Command::RegresqlStub { r#in, out, top, min_calls } => {
            regresql_stub::run(r#in.as_deref(), &out, top, min_calls)?;
        }
    }

    Ok(())
}

fn read_stdin() -> Result<String> {
    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .context("reading SQL from stdin")?;
    Ok(buf)
}
