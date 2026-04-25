use std::io::{self, Read};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

mod capture;

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
