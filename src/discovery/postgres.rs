//! PostgreSQL schema discovery from `information_schema`.
//!
//! | What | Source |
//! |---|---|
//! | tables | `information_schema.tables` (`BASE TABLE` only) |
//! | columns, nullability | `information_schema.columns` |
//! | primary key / unique | `table_constraints` joined to `key_column_usage` |
//! | foreign keys | `table_constraints` joined to `key_column_usage` twice, via `referential_constraints` |
//!
//! The foreign-key query joins `key_column_usage` a second time through
//! `referential_constraints.unique_constraint_name` instead of using
//! `constraint_column_usage`, because only `key_column_usage` carries the ordinal
//! positions needed to pair up the columns of a *composite* foreign key correctly.
//!
//! Discovery is scoped to one schema (`public` by default): `information_schema` also
//! lists `pg_catalog` and every other schema the role can see, which is never what a
//! caller asking for "the tables" means.

use std::collections::HashMap;

use sqlx::{PgPool, Row};

use crate::{DataColumnInfo, DataTableInfo, ForeignKey, NormalizedType, PrimaryKey};

/// The schema dbcon discovers when none is given.
pub(crate) const DEFAULT_SCHEMA: &str = "public";

pub(crate) async fn discover(
    pool: &PgPool,
    schema: &str,
) -> anyhow::Result<HashMap<String, DataTableInfo>> {
    let table_names: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = $1 AND table_type = 'BASE TABLE'
         ORDER BY table_name",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    let mut tables: HashMap<String, DataTableInfo> = table_names
        .into_iter()
        .map(|name| {
            (
                name.clone(),
                DataTableInfo {
                    name,
                    columns: HashMap::new(),
                    primary_keys: Vec::new(),
                    foreign_keys: Vec::new(),
                },
            )
        })
        .collect();

    // `data_type` is the readable form (`character varying`, `timestamp with time zone`)
    // but degrades to `ARRAY` / `USER-DEFINED`; `udt_name` is the underlying type name
    // (`int4`, `_text`, an enum's name), so it is the fallback when `data_type` is not
    // something dbcon recognises.
    let column_rows = sqlx::query(
        "SELECT table_name, column_name, data_type, udt_name, is_nullable
         FROM information_schema.columns
         WHERE table_schema = $1
         ORDER BY table_name, ordinal_position",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    for row in column_rows {
        let table: String = row.get("table_name");
        let Some(info) = tables.get_mut(&table) else {
            continue; // a view or other non-BASE TABLE relation
        };
        let name: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        let udt_name: String = row.get("udt_name");
        let col_type = match NormalizedType::from_raw(&data_type) {
            NormalizedType::Unknown(_) => NormalizedType::from_raw(&udt_name),
            known => known,
        };
        info.columns.insert(
            name.clone(),
            DataColumnInfo {
                name,
                col_type,
                is_nullable: row
                    .get::<String, _>("is_nullable")
                    .eq_ignore_ascii_case("YES"),
            },
        );
    }

    let key_rows = sqlx::query(
        "SELECT tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON kcu.constraint_schema = tc.constraint_schema
          AND kcu.constraint_name = tc.constraint_name
         WHERE tc.table_schema = $1
           AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
         ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name, kcu.ordinal_position",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    // Ordered by constraint_type, so 'PRIMARY KEY' sorts before 'UNIQUE' and the real
    // primary key is always the first entry of `primary_keys`.
    for row in key_rows {
        let table: String = row.get("table_name");
        let Some(info) = tables.get_mut(&table) else {
            continue;
        };
        let constraint: String = row.get("constraint_name");
        let column: String = row.get("column_name");
        match info.primary_keys.last_mut() {
            Some(key) if key.name == constraint => key.columns.push(column),
            _ => info.primary_keys.push(PrimaryKey {
                name: constraint,
                columns: vec![column],
            }),
        }
    }

    let fk_rows = sqlx::query(
        "SELECT tc.table_name,
                tc.constraint_name,
                kcu.column_name       AS from_column,
                target.table_name     AS to_table,
                target.column_name    AS to_column
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu
           ON kcu.constraint_schema = tc.constraint_schema
          AND kcu.constraint_name = tc.constraint_name
         JOIN information_schema.referential_constraints rc
           ON rc.constraint_schema = tc.constraint_schema
          AND rc.constraint_name = tc.constraint_name
         JOIN information_schema.key_column_usage target
           ON target.constraint_schema = rc.unique_constraint_schema
          AND target.constraint_name = rc.unique_constraint_name
          AND target.ordinal_position = kcu.position_in_unique_constraint
         WHERE tc.table_schema = $1 AND tc.constraint_type = 'FOREIGN KEY'
         ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    for row in fk_rows {
        let table: String = row.get("table_name");
        let Some(info) = tables.get_mut(&table) else {
            continue;
        };
        let constraint: String = row.get("constraint_name");
        let from_column: String = row.get("from_column");
        let to_table: String = row.get("to_table");
        let to_column: String = row.get("to_column");
        match info.foreign_keys.last_mut() {
            Some(fk) if fk.name == constraint => {
                fk.from_columns.push(from_column);
                fk.to_columns.push(to_column);
            }
            _ => info.foreign_keys.push(ForeignKey {
                name: constraint,
                from_columns: vec![from_column],
                to_table,
                to_columns: vec![to_column],
            }),
        }
    }

    Ok(tables)
}
