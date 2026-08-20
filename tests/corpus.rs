//! Discovery against the real SQLite files under `$DBCON_CORPUS`.
//!
//! This target runs **without libtest** (`harness = false` in `Cargo.toml`) for one
//! reason: when `DBCON_CORPUS` is unset it must be obvious that nothing ran. libtest
//! captures output and reports a suite that executed nothing as a pass, which is exactly
//! the failure mode this test exists to avoid.
//!
//! | Variable | Effect |
//! |---|---|
//! | `DBCON_CORPUS` | root of the corpus; unset means "skipped", loudly |
//! | `DBCON_CORPUS_REQUIRED=1` | absence of `DBCON_CORPUS` becomes a failure |
//! | `DBCON_CORPUS_FULL=1` | scan every row of every table, ignoring the budget |
//! | `DBCON_CORPUS_MAX_ROWS` | per-file row-scan budget (default 2,000,000) |
//! | `DBCON_SNAPSHOT_UPDATE=1` | rewrite the committed snapshots |
//!
//! Per file it asserts: the tables discovered, each table's columns and mapped
//! `NormalizedType`s, primary keys and foreign keys (all via a committed snapshot), that
//! `scan` yields exactly the row count the file holds, and - for the OCEL 2.0 files - that
//! the schema matches what rust4pm's OCEL reader requires.

#![cfg(feature = "sqlite")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use dbcon::DataSource;

#[path = "support/mod.rs"]
mod support;

const DEFAULT_MAX_SCAN_ROWS: u64 = 2_000_000;

/// Files named in the plan, relative to `$DBCON_CORPUS`. The `ocel/` half is globbed
/// because its file names vary; both halves must be non-empty.
const DATASET_FILES: &[&str] = &["datasets/Chinook_Sqlite.sqlite", "datasets/northwind.db"];

fn main() {
    let Ok(root) = std::env::var("DBCON_CORPUS") else {
        let required = std::env::var("DBCON_CORPUS_REQUIRED").as_deref() == Ok("1");
        println!("\n{}", "=".repeat(78));
        println!("CORPUS TESTS DID NOT RUN: DBCON_CORPUS is not set.");
        println!("Nothing in this target was executed. Zero files were checked.");
        println!("Expected, relative to $DBCON_CORPUS:");
        for f in DATASET_FILES {
            println!("  - {f}");
        }
        println!("  - ocel/*.sqlite");
        println!("Set DBCON_CORPUS_REQUIRED=1 to make this absence a hard failure.");
        println!("{}\n", "=".repeat(78));
        if required {
            eprintln!("FAIL: DBCON_CORPUS_REQUIRED=1 but DBCON_CORPUS is unset.");
            std::process::exit(1);
        }
        println!("corpus: SKIPPED (0 files, 0 assertions)");
        return;
    };

    let root = PathBuf::from(root);
    let mut files: Vec<(PathBuf, bool)> = Vec::new();
    let mut failures: Vec<String> = Vec::new();
    for name in DATASET_FILES {
        let path = root.join(name);
        if path.is_file() {
            files.push((path, false));
        } else {
            failures.push(format!("missing corpus file {}", path.display()));
        }
    }
    match ocel_files(&root.join("ocel")) {
        Ok(found) if found.is_empty() => {
            failures.push(format!("no ocel/*.sqlite under {}", root.display()))
        }
        Ok(found) => files.extend(found.into_iter().map(|p| (p, true))),
        Err(e) => failures.push(format!("reading {}/ocel: {e}", root.display())),
    }

    let budget = if std::env::var("DBCON_CORPUS_FULL").as_deref() == Ok("1") {
        u64::MAX
    } else {
        std::env::var("DBCON_CORPUS_MAX_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_SCAN_ROWS)
    };

    println!("\ncorpus root: {}", root.display());
    println!("row-scan budget per file: {}\n", budget_label(budget));

    let mut checked = 0usize;
    for (path, is_ocel) in &files {
        let started = Instant::now();
        match check_file(path, *is_ocel, budget) {
            Ok(summary) => {
                checked += 1;
                println!(
                    "PASS {:<52} {summary} ({:.1?})",
                    name_of(path),
                    started.elapsed()
                );
            }
            Err(e) => {
                println!("FAIL {:<52} {e}", name_of(path));
                failures.push(format!("{}: {e}", name_of(path)));
            }
        }
    }

    println!(
        "\ncorpus: {checked} file(s) passed, {} failed",
        failures.len()
    );
    if !failures.is_empty() {
        for f in &failures {
            eprintln!("  FAIL {f}");
        }
        std::process::exit(1);
    }
    if checked == 0 {
        eprintln!("FAIL: DBCON_CORPUS is set but no corpus file was checked.");
        std::process::exit(1);
    }
}

fn budget_label(budget: u64) -> String {
    if budget == u64::MAX {
        "unlimited (DBCON_CORPUS_FULL=1)".to_string()
    } else {
        format!("{budget} rows")
    }
}

fn name_of(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}

fn snapshot_name(path: &Path) -> String {
    let sanitised: String = name_of(path)
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("corpus_{sanitised}")
}

fn ocel_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "sqlite"))
        .collect();
    out.sort();
    Ok(out)
}

fn check_file(path: &Path, is_ocel: bool, budget: u64) -> Result<String, String> {
    // Read-only: the corpus lives outside the repo and several files are ~700 MB, so
    // discovery must not create a -wal sidecar next to them.
    let url = format!("sqlite://{}?mode=ro", path.display());
    let ds =
        DataSource::new_any(name_of(path), url).map_err(|e| format!("connect/discover: {e}"))?;

    // `count(*)` is cheap and always recorded, so the snapshot pins row counts even for
    // files too large to iterate row by row.
    let mut counts: BTreeMap<String, String> = BTreeMap::new();
    let mut declared_total = 0u64;
    for table in ds.tables.keys() {
        let n =
            support::count_rows_sql(&ds, table).map_err(|e| format!("count(*) on {table}: {e}"))?;
        declared_total += n;
        counts.insert(table.clone(), n.to_string());
    }

    support::check_snapshot(&snapshot_name(path), &support::render_schema(&ds, &counts))?;

    let scanned = if declared_total <= budget {
        let mut tables: Vec<&String> = ds.tables.keys().collect();
        tables.sort();
        for table in tables {
            let expected: u64 = counts[table].parse().unwrap_or(0);
            let got = support::count_rows_scan(&ds, table)
                .map_err(|e| format!("scan on {table}: {e}"))?;
            if got != expected {
                return Err(format!(
                    "scan yielded {got} rows for {table}, file holds {expected}"
                ));
            }
        }
        format!("{declared_total} rows verified")
    } else {
        format!("{declared_total} rows counted, row scan over budget")
    };

    let ocel_note = if is_ocel {
        let deviations = support::ocel_oracle(&ds).map_err(|e| format!("OCEL 2.0 layout: {e}"))?;
        if deviations.is_empty() {
            ", OCEL layout ok".to_string()
        } else {
            // The full list is in the snapshot; keep the summary line readable.
            let shown = deviations.len().min(2);
            format!(
                ", OCEL layout ok with {} reader-compat deviation(s) [{}{}]",
                deviations.len(),
                deviations[..shown].join("; "),
                if deviations.len() > shown {
                    ", ..."
                } else {
                    ""
                }
            )
        }
    } else {
        String::new()
    };

    let unknown = ds.unknown_column_types();
    let unknown_note = if unknown.is_empty() {
        String::new()
    } else {
        let kinds: std::collections::BTreeSet<&str> = unknown.iter().map(|(_, _, t)| *t).collect();
        format!(
            ", {} column(s) unmapped {:?}",
            unknown.len(),
            kinds.into_iter().collect::<Vec<_>>()
        )
    };

    Ok(format!(
        "{} tables, {scanned}{unknown_note}{ocel_note}",
        ds.tables.len()
    ))
}
