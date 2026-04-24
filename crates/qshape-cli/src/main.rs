use std::io::{self, Read};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "qshape", version, about = "Normalize, fingerprint, and cluster PostgreSQL queries")]
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
