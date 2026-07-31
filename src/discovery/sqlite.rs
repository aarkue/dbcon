//! SQLite schema discovery from `sqlite_master` and the `PRAGMA` catalog.
//!
//! | What | Source |
//! |---|---|
//! | tables | `sqlite_master WHERE type = 'table'` |
//! | columns, nullability | `PRAGMA table_info` |
//! | primary key | `PRAGMA table_info.pk` (1-based position within the key) |
//! | unique constraints | `PRAGMA index_list` + `PRAGMA index_info` |
//! | foreign keys | `PRAGMA foreign_key_list` |
//!
//! The pragmas are called through their table-valued function forms
//! (`pragma_table_info(?)`), which - unlike `PRAGMA table_info(x)` - accept a bound
//! parameter, so table names containing spaces or quotes (northwind's `Order Details`)
//! need no escaping.

use std::collections::HashMap;

use crate::sqlite::SqliteSource;
use crate::{DataColumnInfo, DataTableInfo, ForeignKey, NormalizedType, PrimaryKey};

/// One row of `PRAGMA table_info`.
struct ColumnRow {
    name: String,
    declared_type: String,
    not_null: bool,
    /// 0 when the column is not part of the primary key, otherwise its 1-based
    /// position within it.
    pk_position: i64,
}

pub(crate) fn discover(source: &SqliteSource) -> anyhow::Result<HashMap<String, DataTableInfo>> {
    // `_` is a LIKE wildcard, so the internal-table filter has to escape it or it would
    // also drop a user table called e.g. `sqliteXfoo`.
    let table_names = source.query_strings(
        r#"SELECT name FROM sqlite_master
           WHERE type = 'table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\'
           ORDER BY name"#,
        &[],
    )?;

    let mut column_rows: HashMap<String, Vec<ColumnRow>> =
        HashMap::with_capacity(table_names.len());
    for table in &table_names {
        column_rows.insert(table.clone(), table_info(source, table)?);
    }

    // A foreign key may omit the referenced columns, which means "the primary key of the
    // target table"; resolving that needs every table's primary key up front.
    let primary_key_columns: HashMap<&str, Vec<String>> = column_rows
        .iter()
        .map(|(table, cols)| (table.as_str(), pk_columns(cols)))
        .collect();

    let mut tables = HashMap::with_capacity(table_names.len());
    for table in &table_names {
        let cols = &column_rows[table];

        let columns = cols
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    DataColumnInfo {
                        name: c.name.clone(),
                        col_type: NormalizedType::from_sqlite_declared(&c.declared_type),
                        is_nullable: !c.not_null,
                    },
                )
            })
            .collect();

        let mut primary_keys = Vec::new();
        let pk = pk_columns(cols);
        if !pk.is_empty() {
            primary_keys.push(PrimaryKey {
                name: format!("{table}_pk"),
                columns: pk,
            });
        }
        primary_keys.extend(unique_constraints(source, table)?);

        tables.insert(
            table.clone(),
            DataTableInfo {
                name: table.clone(),
                columns,
                primary_keys,
                foreign_keys: foreign_keys(source, table, &primary_key_columns)?,
            },
        );
    }

    Ok(tables)
}

fn table_info(source: &SqliteSource, table: &str) -> anyhow::Result<Vec<ColumnRow>> {
    source.query_rows(
        r#"SELECT name, type, "notnull", pk FROM pragma_table_info(?) ORDER BY cid"#,
        &[table],
        |row| {
            Ok(ColumnRow {
                name: row.get(0)?,
                // A column may be declared with no type at all, which SQLite reports as "".
                declared_type: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                not_null: row.get::<_, i64>(2)? != 0,
                pk_position: row.get(3)?,
            })
        },
    )
}

/// Primary-key columns in key order. `pk` is the 1-based position within the key, so a
/// composite key is recovered by sorting on it.
fn pk_columns(cols: &[ColumnRow]) -> Vec<String> {
    let mut in_key: Vec<&ColumnRow> = cols.iter().filter(|c| c.pk_position > 0).collect();
    in_key.sort_by_key(|c| c.pk_position);
    in_key.into_iter().map(|c| c.name.clone()).collect()
}

/// Unique constraints and unique indexes, reported alongside the primary key because
/// [`PrimaryKey`] covers both kinds of uniqueness guarantee.
///
/// `origin = 'pk'` is skipped (already reported as the primary key) and partial indexes
/// are skipped because they only guarantee uniqueness over the rows they cover.
fn unique_constraints(source: &SqliteSource, table: &str) -> anyhow::Result<Vec<PrimaryKey>> {
    let index_names = source.query_strings(
        r#"SELECT name FROM pragma_index_list(?)
           WHERE "unique" = 1 AND origin <> 'pk' AND partial = 0
           ORDER BY name"#,
        &[table],
    )?;

    let mut out = Vec::with_capacity(index_names.len());
    for index_name in index_names {
        let cols: Vec<Option<String>> = source.query_rows(
            r#"SELECT name FROM pragma_index_info(?) ORDER BY seqno"#,
            &[index_name.as_str()],
            |row| row.get(0),
        )?;
        // A NULL column name means the index is over an expression, which is not a
        // uniqueness guarantee over any column set we could name.
        if cols.is_empty() || cols.iter().any(Option::is_none) {
            continue;
        }
        out.push(PrimaryKey {
            name: index_name,
            columns: cols.into_iter().flatten().collect(),
        });
    }
    Ok(out)
}

fn foreign_keys(
    source: &SqliteSource,
    table: &str,
    primary_key_columns: &HashMap<&str, Vec<String>>,
) -> anyhow::Result<Vec<ForeignKey>> {
    let rows: Vec<(i64, String, String, Option<String>)> = source.query_rows(
        r#"SELECT id, seq, "table" AS target_table, "from" AS from_column, "to" AS to_column
           FROM pragma_foreign_key_list(?)
           ORDER BY id, seq"#,
        &[table],
        |row| Ok((row.get(0)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )?;

    // Rows are one column pair each, grouped by `id`; `seq` orders a composite key.
    /// `(constraint id, target table, referencing columns, referenced columns)`.
    /// The referenced columns are optional: SQLite allows them to be omitted.
    type Grouped = (i64, String, Vec<String>, Vec<Option<String>>);
    let mut grouped: Vec<Grouped> = Vec::new();
    for (id, target, from, to) in rows {
        match grouped.last_mut() {
            Some((last_id, _, froms, tos)) if *last_id == id => {
                froms.push(from);
                tos.push(to);
            }
            _ => grouped.push((id, target, vec![from], vec![to])),
        }
    }

    Ok(grouped
        .into_iter()
        .map(|(id, target, from_columns, to_columns)| {
            // SQLite allows `REFERENCES other(...)` with the column list omitted, meaning
            // the target's primary key. Resolve it rather than reporting an empty target.
            let to_columns = if to_columns.iter().all(Option::is_some) {
                to_columns.into_iter().flatten().collect()
            } else {
                primary_key_columns
                    .get(target.as_str())
                    .cloned()
                    .unwrap_or_default()
            };
            ForeignKey {
                // SQLite does not record foreign-key constraint names, only an index.
                name: format!("{table}_fk_{id}"),
                from_columns,
                to_table: target,
                to_columns,
            }
        })
        .collect())
}
