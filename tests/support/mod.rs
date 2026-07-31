//! Shared helpers for the schema-discovery tests.
//!
//! Discovery results are rendered to a deterministic text form and compared against a
//! committed snapshot, so a change in what dbcon reports about a database shows up as a
//! diff instead of as a judgement call. Set `DBCON_SNAPSHOT_UPDATE=1` to rewrite them.

#![allow(dead_code)] // each test target uses a different subset

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use dbcon::{DataSource, NormalizedType};

pub fn snapshot_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/snapshots")
}

pub fn fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    format!("sqlite://{}?mode=ro", path.display())
}

pub fn render_type(t: &NormalizedType) -> String {
    match t {
        NormalizedType::Unknown(raw) => format!("Unknown({raw:?})"),
        other => format!("{other:?}"),
    }
}

/// Deterministic text rendering of everything discovery reports about a database.
///
/// `row_counts` is keyed by table name; a table missing from the map renders as `?`,
/// which is how a corpus file too large to scan is recorded.
pub fn render_schema(ds: &DataSource, row_counts: &BTreeMap<String, String>) -> String {
    let mut tables: Vec<_> = ds.tables.values().collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));

    let mut out = String::new();
    out.push_str(&format!("tables: {}\n", tables.len()));
    for table in tables {
        let rows = row_counts
            .get(&table.name)
            .map(String::as_str)
            .unwrap_or("?");
        out.push_str(&format!("\ntable {:?} rows={}\n", table.name, rows));

        let mut columns: Vec<_> = table.columns.values().collect();
        columns.sort_by(|a, b| a.name.cmp(&b.name));
        for col in columns {
            out.push_str(&format!(
                "  col {:?}: {} {}\n",
                col.name,
                render_type(&col.col_type),
                if col.is_nullable { "NULL" } else { "NOT NULL" }
            ));
        }

        let mut pks: Vec<String> = table
            .primary_keys
            .iter()
            .map(|pk| format!("  key {:?}: ({})\n", pk.name, pk.columns.join(", ")))
            .collect();
        // The first entry is the real primary key by construction; the rest are unique
        // constraints whose relative order the catalog does not fix.
        if pks.len() > 1 {
            pks[1..].sort();
        }
        out.extend(pks);

        let mut fks: Vec<String> = table
            .foreign_keys
            .iter()
            .map(|fk| {
                format!(
                    "  fk {:?}: ({}) -> {:?}({})\n",
                    fk.name,
                    fk.from_columns.join(", "),
                    fk.to_table,
                    fk.to_columns.join(", ")
                )
            })
            .collect();
        fks.sort();
        out.extend(fks);
    }
    out
}

/// Compare `actual` with the committed snapshot `name`, or write it when
/// `DBCON_SNAPSHOT_UPDATE=1`. Returns an error describing the first difference.
pub fn check_snapshot(name: &str, actual: &str) -> Result<(), String> {
    let path = snapshot_dir().join(format!("{name}.txt"));
    if std::env::var("DBCON_SNAPSHOT_UPDATE").as_deref() == Ok("1") {
        std::fs::create_dir_all(snapshot_dir()).map_err(|e| e.to_string())?;
        std::fs::write(&path, actual).map_err(|e| e.to_string())?;
        return Ok(());
    }
    let expected = std::fs::read_to_string(&path).map_err(|e| {
        format!(
            "no snapshot at {}: {e}. Re-run with DBCON_SNAPSHOT_UPDATE=1 to create it.",
            path.display()
        )
    })?;
    if expected == actual {
        return Ok(());
    }
    let mut diff = String::new();
    for (i, (e, a)) in expected.lines().zip(actual.lines()).enumerate() {
        if e != a {
            diff = format!(
                "first difference at line {}:\n  expected: {e}\n  actual:   {a}",
                i + 1
            );
            break;
        }
    }
    if diff.is_empty() {
        diff = format!(
            "line counts differ: expected {}, actual {}",
            expected.lines().count(),
            actual.lines().count()
        );
    }
    Err(format!("snapshot {name} mismatch. {diff}"))
}

/// `SELECT count(*)` per table: the oracle that `scan` is checked against.
pub fn count_rows_sql(ds: &DataSource, table: &str) -> anyhow::Result<u64> {
    let mut count = 0u64;
    ds.for_each_row_sql(
        &format!("SELECT count(*) AS n FROM \"{table}\""),
        &mut |row| {
            if let Some((_, value)) = row.first() {
                count = value.to_string().parse().unwrap_or(0);
            }
        },
    )?;
    Ok(count)
}

/// Rows actually yielded by `scan` over every column of `table`.
pub fn count_rows_scan(ds: &DataSource, table: &str) -> anyhow::Result<u64> {
    let info = ds
        .tables
        .get(table)
        .ok_or_else(|| anyhow::anyhow!("table {table} not discovered"))?;
    let mut names: Vec<&str> = info.columns.values().map(|c| c.name.as_str()).collect();
    names.sort_unstable();
    let mut count = 0u64;
    ds.scan(table, &names, None, &mut |_| {
        count += 1;
        std::ops::ControlFlow::Continue(())
    })?;
    Ok(count)
}

/// Check discovery output against the OCEL 2.0 SQLite layout that rust4pm's
/// `import_ocel_sqlite_from_con` reads.
///
/// This is the corpus's own oracle: the `ocel/*.sqlite` files are not checked only
/// against themselves but against the schema an independent reader requires. See
/// `process_mining/src/core/event_data/object_centric/ocel_sql/` in rust4pm.
///
/// `Err` means the file is not an OCEL 2.0 SQLite log at all, or discovery reported it
/// wrongly. `Ok(deviations)` lists places where the file is a valid OCEL log that
/// rust4pm's reader would nevertheless choke on - a fact about the file, not about
/// discovery, so it is reported rather than failed. The exact columns are pinned by the
/// snapshot either way.
pub fn ocel_oracle(ds: &DataSource) -> Result<Vec<String>, String> {
    let table = |name: &str| {
        ds.tables
            .get(name)
            .ok_or_else(|| format!("OCEL table `{name}` missing"))
    };

    for (name, want) in [
        ("event", vec!["ocel_id", "ocel_type"]),
        ("object", vec!["ocel_id", "ocel_type"]),
        ("event_map_type", vec!["ocel_type", "ocel_type_map"]),
        ("object_map_type", vec!["ocel_type", "ocel_type_map"]),
        (
            "event_object",
            vec!["ocel_event_id", "ocel_object_id", "ocel_qualifier"],
        ),
        (
            "object_object",
            vec!["ocel_qualifier", "ocel_source_id", "ocel_target_id"],
        ),
    ] {
        let info = table(name)?;
        let mut got: Vec<&str> = info.columns.keys().map(String::as_str).collect();
        got.sort_unstable();
        if got != want {
            return Err(format!("table `{name}` columns {got:?}, expected {want:?}"));
        }
        for col in got {
            let t = &info.columns[col].col_type;
            if t != &NormalizedType::Text {
                return Err(format!("{name}.{col} mapped to {t:?}, expected Text"));
            }
        }
    }

    // Every declared event/object type must have its own attribute table, named by the
    // *mapped* name. `ocel_id` is required of both; rust4pm additionally reads
    // `ocel_time` from event tables and `ocel_changed_field` from object tables.
    let mut deviations = Vec::new();
    for (map_table, prefix, required, expected) in [
        (
            "event_map_type",
            "event_",
            &["ocel_id", "ocel_time"][..],
            &[][..],
        ),
        (
            "object_map_type",
            "object_",
            &["ocel_id"][..],
            &["ocel_time", "ocel_changed_field"][..],
        ),
    ] {
        let type_maps = ds
            .get_distinct_values(map_table, "ocel_type_map")
            .map_err(|e| format!("reading `{map_table}`: {e}"))?;
        for mapped in type_maps {
            let name = format!("{prefix}{mapped}");
            let info = table(&name)?;
            for col in required {
                let Some(c) = info.columns.get(*col) else {
                    return Err(format!("`{name}` has no `{col}` column"));
                };
                if !readable_as(&c.col_type, col) {
                    return Err(format!("`{name}`.`{col}` mapped to {:?}", c.col_type));
                }
            }
            for col in expected {
                match info.columns.get(*col) {
                    None => deviations.push(format!("`{name}` has no `{col}`")),
                    Some(c) if !readable_as(&c.col_type, col) => {
                        deviations.push(format!("`{name}`.`{col}` mapped to {:?}", c.col_type))
                    }
                    Some(_) => {}
                }
            }
        }
    }
    Ok(deviations)
}

/// Whether a mapped type is usable for a given OCEL column. Exporters disagree on
/// whether `ocel_time` is declared TIMESTAMP, TEXT or VARCHAR; all three are readable,
/// an `Unknown` mapping would not be.
fn readable_as(t: &NormalizedType, column: &str) -> bool {
    match column {
        "ocel_time" => matches!(t, NormalizedType::Timestamp | NormalizedType::Text),
        _ => t == &NormalizedType::Text,
    }
}
