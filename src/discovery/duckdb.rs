//! `DuckDB` schema discovery, from `information_schema` and the `duckdb_constraints()` catalog.
//!
//! | What | Source |
//! |---|---|
//! | tables | `information_schema.tables WHERE table_type = 'BASE TABLE'` |
//! | columns, nullability, type | `information_schema.columns` |
//! | primary key, unique | `duckdb_constraints()` |
//! | foreign keys | `duckdb_constraints()` |
//!
//! `DuckDB` ships an `information_schema`, but it has no `key_column_usage` /
//! `referential_constraints` pair to read keys from, so constraints come from the
//! `duckdb_constraints()` table function instead. That function reports each constraint's columns
//! as a `VARCHAR[]`, which is why every query here joins the list with a separator no identifier
//! can hold and this module splits it back into names rather than joining against another catalog
//! table.
//!
//! Discovery is scoped to a single schema (`main` by default), as the PostgreSQL backend is.
//! Everything here is keyed by bare table name, and `DuckDB` admits any number of schemas, so
//! `main.orders` and `s2.orders` would otherwise be the same table.

use std::collections::HashMap;

use crate::duckdb::{normalize_type, DuckDbSource};
use crate::{DataColumnInfo, DataTableInfo, ForeignKey, PrimaryKey};

/// The schema dbcon discovers when none is given. `DuckDB` puts a `CREATE TABLE` with no schema
/// qualifier here.
pub(crate) const DEFAULT_SCHEMA: &str = "main";

/// `DuckDB` exposes its catalogs as databases, and a temporary table lives in `temp.main`. Neither
/// holds a table a caller asked about.
const USER_CATALOG: &str = "table_catalog NOT IN ('system', 'temp')";

pub(crate) fn discover(
    source: &DuckDbSource,
    schema: &str,
) -> anyhow::Result<HashMap<String, DataTableInfo>> {
    let table_names = source.query_strings(
        &format!(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_type = 'BASE TABLE' AND table_schema = ? AND {USER_CATALOG} \
             ORDER BY table_name"
        ),
        &[schema],
    )?;

    // One pass over the whole column catalog rather than a query per table: `DuckDB` answers this
    // from memory, and a per-table round trip over a wide schema is the slower shape.
    let column_rows = source.query_string_rows(
        &format!(
            "SELECT table_name, column_name, data_type, is_nullable \
             FROM information_schema.columns WHERE table_schema = ? AND {USER_CATALOG} \
             ORDER BY table_name, ordinal_position"
        ),
        &[schema],
    )?;

    let mut columns_by_table: HashMap<String, HashMap<String, DataColumnInfo>> = HashMap::new();
    for row in &column_rows {
        let [table, name, data_type, nullable] = &row[..] else {
            continue;
        };
        columns_by_table
            .entry(table.clone())
            .or_default()
            .insert(
                name.clone(),
                DataColumnInfo {
                    name: name.clone(),
                    col_type: normalize_type(data_type),
                    // `information_schema` spells this "YES"/"NO".
                    is_nullable: nullable.eq_ignore_ascii_case("YES"),
                },
            );
    }

    let constraints = constraint_rows(source, schema)?;

    // A foreign key may name the target table without its columns, which means the target's
    // primary key. Collected up front because the key belongs to a table other than the one
    // whose constraints are being read.
    let primary_key_columns: HashMap<&str, &[String]> = constraints
        .iter()
        .filter(|c| c.kind == "PRIMARY KEY")
        .map(|c| (c.table.as_str(), c.columns.as_slice()))
        .collect();

    let mut tables = HashMap::with_capacity(table_names.len());
    for table in &table_names {
        let mut primary_keys = Vec::new();
        let mut foreign_keys = Vec::new();
        for c in constraints.iter().filter(|c| &c.table == table) {
            match c.kind.as_str() {
                "PRIMARY KEY" => primary_keys.push(PrimaryKey {
                    name: c.name.clone().unwrap_or_else(|| format!("{table}_pk")),
                    columns: c.columns.clone(),
                }),
                // Reported alongside the primary key, matching the SQLite backend: a unique
                // constraint identifies a row just as well, and a blueprint join may use either.
                "UNIQUE" => primary_keys.push(PrimaryKey {
                    name: c
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("{table}_unique_{}", c.columns.join("_"))),
                    columns: c.columns.clone(),
                }),
                // A foreign key with no resolvable target is dropped rather than emitted pointing
                // at nothing: a blueprint would render it as a join to a table that is not in the
                // catalog, or to columns the target does not have.
                "FOREIGN KEY" => {
                    let Some(to_table) = c.referenced_table.as_deref() else {
                        continue;
                    };
                    let to_columns = if c.referenced_columns.is_empty() {
                        match primary_key_columns.get(to_table) {
                            Some(columns) => columns.to_vec(),
                            None => continue,
                        }
                    } else {
                        c.referenced_columns.clone()
                    };
                    foreign_keys.push(ForeignKey {
                        name: c
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{table}_fk_{}", c.columns.join("_"))),
                        from_columns: c.columns.clone(),
                        to_columns,
                        to_table: to_table.to_string(),
                    });
                }
                _ => {}
            }
        }

        tables.insert(
            table.clone(),
            DataTableInfo {
                name: table.clone(),
                columns: columns_by_table.remove(table).unwrap_or_default(),
                primary_keys,
                foreign_keys,
            },
        );
    }

    Ok(tables)
}

struct ConstraintRow {
    table: String,
    kind: String,
    name: Option<String>,
    columns: Vec<String>,
    referenced_table: Option<String>,
    referenced_columns: Vec<String>,
}

/// The unit separator, joining each `VARCHAR[]` into one text field. No SQL identifier can hold
/// it, so splitting on it round-trips names that `DuckDB`'s own `[a, b]` rendering would not.
const SEPARATOR: char = '\x1f';

fn constraint_rows(source: &DuckDbSource, schema: &str) -> anyhow::Result<Vec<ConstraintRow>> {
    // `referenced_table`/`referenced_column_names` are empty for a non-FK row, and
    // `constraint_name` is the name the database gave the constraint.
    let rows = source.query_string_rows(
        "SELECT table_name, constraint_type, COALESCE(constraint_name, ''), \
                array_to_string(constraint_column_names, chr(31)), \
                COALESCE(referenced_table, ''), \
                array_to_string(referenced_column_names, chr(31)) \
         FROM duckdb_constraints() \
         WHERE schema_name = ? AND database_name NOT IN ('system', 'temp') \
         ORDER BY table_name",
        &[schema],
    )?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            let [table, kind, name, columns, referenced_table, referenced_columns] = &row[..]
            else {
                return None;
            };
            Some(ConstraintRow {
                table: table.clone(),
                kind: kind.clone(),
                name: (!name.is_empty()).then(|| name.clone()),
                columns: parse_name_list(columns),
                referenced_table: (!referenced_table.is_empty()).then(|| referenced_table.clone()),
                referenced_columns: parse_name_list(referenced_columns),
            })
        })
        .collect())
}

/// A separator-joined `VARCHAR[]` as the names it holds. An empty field yields no names.
fn parse_name_list(joined: &str) -> Vec<String> {
    joined
        .split(SEPARATOR)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NormalizedType;

    fn source_with(setup: &[&str]) -> DuckDbSource {
        let s = DuckDbSource::open("duckdb://:memory:").expect("in-memory database opens");
        for sql in setup {
            s.rows(sql, None).expect("setup statement runs");
        }
        s
    }

    #[test]
    fn name_lists_parse_from_the_joined_field() {
        assert_eq!(parse_name_list("a\x1fb"), vec!["a", "b"]);
        assert_eq!(parse_name_list("id"), vec!["id"]);
        assert_eq!(parse_name_list("a, b\x1fc"), vec!["a, b", "c"]);
        assert!(parse_name_list("").is_empty());
    }

    #[test]
    fn columns_carry_their_type_and_nullability() {
        let s = source_with(&[
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, total DECIMAL(18,2), note VARCHAR)",
        ]);
        let tables = discover(&s, DEFAULT_SCHEMA).expect("discovery runs");
        let orders = tables.get("orders").expect("orders is discovered");
        assert_eq!(orders.columns["id"].col_type, NormalizedType::Integer);
        assert_eq!(orders.columns["total"].col_type, NormalizedType::Float);
        assert_eq!(orders.columns["note"].col_type, NormalizedType::Text);
        assert!(!orders.columns["id"].is_nullable, "a primary key is NOT NULL");
        assert!(orders.columns["note"].is_nullable);
    }

    #[test]
    fn the_primary_key_is_reported_with_its_columns() {
        let s = source_with(&["CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))"]);
        let tables = discover(&s, DEFAULT_SCHEMA).expect("discovery runs");
        let pk = &tables["t"].primary_keys;
        assert_eq!(pk.len(), 1, "{pk:?}");
        assert_eq!(pk[0].columns, vec!["a", "b"]);
    }

    #[test]
    fn a_foreign_key_names_the_table_it_points_at() {
        let s = source_with(&[
            "CREATE TABLE customers (id BIGINT PRIMARY KEY)",
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT REFERENCES customers(id))",
        ]);
        let tables = discover(&s, DEFAULT_SCHEMA).expect("discovery runs");
        let fks = &tables["orders"].foreign_keys;
        assert_eq!(fks.len(), 1, "{fks:?}");
        assert_eq!(fks[0].from_columns, vec!["customer_id"]);
        assert_eq!(fks[0].to_table, "customers");
        assert_eq!(fks[0].to_columns, vec!["id"]);
    }

    /// Two schemas can hold the same table name. Unscoped, the second one's columns merged into
    /// the first's entry and then overwrote it with nothing.
    #[test]
    fn only_the_requested_schema_is_discovered() {
        let s = source_with(&[
            "CREATE TABLE orders (id BIGINT, note VARCHAR)",
            "CREATE SCHEMA s2",
            "CREATE TABLE s2.orders (other DOUBLE)",
        ]);
        let tables = discover(&s, DEFAULT_SCHEMA).expect("discovery runs");
        let orders = tables.get("orders").expect("orders is discovered");
        assert_eq!(
            orders.columns.keys().collect::<Vec<_>>().len(),
            2,
            "{:?}",
            orders.columns.keys().collect::<Vec<_>>()
        );
        assert!(orders.columns.contains_key("note"));
        assert!(!orders.columns.contains_key("other"));

        let other = discover(&s, "s2").expect("discovery runs");
        assert!(other["orders"].columns.contains_key("other"));
    }

    /// `DuckDB` names its own constraints, so the reported name is the database's, not one made
    /// up here.
    #[test]
    fn a_constraint_keeps_the_name_the_database_gave_it() {
        let s = source_with(&["CREATE TABLE t (a INTEGER PRIMARY KEY)"]);
        let tables = discover(&s, DEFAULT_SCHEMA).expect("discovery runs");
        let pk = &tables["t"].primary_keys[0];
        assert!(!pk.name.is_empty());
        assert_ne!(pk.name, "t_pk", "the synthesised name is only a fallback");
    }

    /// The internal catalogs are schemas too, so an unfiltered query would report them as tables.
    #[test]
    fn internal_catalog_tables_are_not_reported() {
        let s = source_with(&["CREATE TABLE mine (a INTEGER)"]);
        let tables = discover(&s, DEFAULT_SCHEMA).expect("discovery runs");
        assert!(tables.contains_key("mine"));
        assert!(
            !tables.keys().any(|t| t.starts_with("duckdb_")),
            "catalog tables leaked in: {:?}",
            tables.keys().collect::<Vec<_>>()
        );
    }
}
