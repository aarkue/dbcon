//! Always-on discovery coverage against the fixtures committed in `tests/fixtures`.
//!
//! These run with no environment set up at all. The larger, real-world checks live in
//! `tests/corpus.rs`, which needs `DBCON_CORPUS`.

#![cfg(feature = "sqlite")]

use std::collections::BTreeMap;

use dbcon::{DataSource, NormalizedType};

#[path = "support/mod.rs"]
mod support;

async fn open(fixture: &str) -> DataSource {
    DataSource::new_any(fixture.to_string(), support::fixture(fixture))
        .await
        .unwrap_or_else(|e| panic!("opening fixture {fixture}: {e}"))
}

fn row_counts(ds: &DataSource) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for table in ds.tables.keys() {
        let expected = support::count_rows_sql(ds, table).unwrap();
        let scanned = support::count_rows_scan(ds, table).unwrap();
        assert_eq!(
            expected, scanned,
            "scan yielded {scanned} rows for table {table}, but it holds {expected}"
        );
        out.insert(table.clone(), scanned.to_string());
    }
    out
}

#[tokio::test]
async fn typezoo_snapshot() {
    let ds = open("typezoo.sqlite").await;
    let counts = row_counts(&ds);
    support::check_snapshot("fixture_typezoo", &support::render_schema(&ds, &counts)).unwrap();
}

/// The mapping a consumer's literal coercion depends on, asserted column by column
/// rather than only via the snapshot.
#[tokio::test]
async fn typezoo_maps_every_declared_spelling() {
    use NormalizedType::*;
    let ds = open("typezoo.sqlite").await;
    let table = ds.tables.get("type_zoo").expect("type_zoo discovered");
    let expect = [
        ("c_integer", Integer),
        ("c_bigint", Integer),
        ("c_int8", Integer),
        ("c_tinyint", Integer),
        ("c_text", Text),
        ("c_varchar", Text),
        ("c_nvarchar", Text),
        ("c_clob", Text),
        ("c_real", Float),
        ("c_double", Float),
        ("c_numeric", Float),
        ("c_float", Float),
        ("c_bool", Boolean),
        ("c_date", Timestamp),
        ("c_datetime", Timestamp),
        ("c_timestamp", Timestamp),
        ("c_json", Json),
        // SQLite affinity rules, not guesses.
        ("c_aff_int", Integer),
        ("c_aff_text", Text),
        ("c_aff_text2", Text),
        ("c_aff_real", Float),
    ];
    for (col, want) in expect {
        let got = &table
            .columns
            .get(col)
            .unwrap_or_else(|| panic!("{col}"))
            .col_type;
        assert_eq!(got, &want, "column {col}");
    }

    // The deliberate fallbacks: never silently Text.
    assert_eq!(
        table.columns["c_blob"].col_type,
        Unknown("blob".to_string()),
        "a BLOB column must stay visible as Unknown"
    );
    assert_eq!(
        table.columns["c_untyped"].col_type,
        Unknown(String::new()),
        "a column declared with no type has BLOB affinity and nothing to report"
    );
    assert_eq!(
        table.columns["c_weird"].col_type,
        Unknown("blah".to_string()),
        "NUMERIC affinity could be integer, real or text, so it stays Unknown"
    );

    // ... and those three are exactly what `unknown_column_types` surfaces.
    assert_eq!(
        ds.unknown_column_types(),
        vec![
            ("type_zoo", "c_blob", "blob"),
            ("type_zoo", "c_untyped", ""),
            ("type_zoo", "c_weird", "blah"),
        ]
    );
}

#[tokio::test]
async fn keys_snapshot() {
    let ds = open("keys.sqlite").await;
    let counts = row_counts(&ds);
    support::check_snapshot("fixture_keys", &support::render_schema(&ds, &counts)).unwrap();
}

#[tokio::test]
async fn keys_composite_self_referencing_and_implicit_targets() {
    let ds = open("keys.sqlite").await;

    let parent = &ds.tables["parent"];
    assert_eq!(parent.primary_keys.len(), 1);
    assert_eq!(parent.primary_keys[0].columns, vec!["a", "b"]);

    let child = &ds.tables["child"];
    assert_eq!(child.primary_keys[0].columns, vec!["id"]);
    // A UNIQUE index is a uniqueness guarantee and is reported; a plain index is not.
    let unique: Vec<&str> = child.primary_keys[1..]
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    assert_eq!(unique, vec!["child_code_unique"]);

    let composite = child
        .foreign_keys
        .iter()
        .find(|fk| fk.to_table == "parent")
        .expect("composite fk to parent");
    assert_eq!(composite.from_columns, vec!["p_a", "p_b"]);
    assert_eq!(composite.to_columns, vec!["a", "b"]);

    // `manager_id INTEGER REFERENCES child` names no target column: SQLite means the
    // target's primary key, and discovery has to resolve that rather than report nothing.
    let self_ref = child
        .foreign_keys
        .iter()
        .find(|fk| fk.from_columns == vec!["manager_id"])
        .expect("self-referencing fk");
    assert_eq!(self_ref.to_table, "child");
    assert_eq!(self_ref.to_columns, vec!["id"]);

    // A table name with a space must survive both discovery and row iteration.
    let details = &ds.tables["Order Details"];
    assert_eq!(details.primary_keys[0].columns, vec!["order_id", "line"]);
    assert_eq!(support::count_rows_scan(&ds, "Order Details").unwrap(), 2);
}

#[tokio::test]
async fn ocel_mini_snapshot() {
    let ds = open("ocel_mini.sqlite").await;
    let counts = row_counts(&ds);
    support::check_snapshot("fixture_ocel_mini", &support::render_schema(&ds, &counts)).unwrap();
}

/// The OCEL 2.0 SQLite layout rust4pm's reader expects, checked against a fixture built
/// to that layout. `tests/corpus.rs` applies the same oracle to the real files.
#[tokio::test]
async fn ocel_mini_matches_the_ocel2_sqlite_layout() {
    let ds = open("ocel_mini.sqlite").await;
    let deviations = support::ocel_oracle(&ds).expect("fixture matches the OCEL 2.0 SQLite layout");
    assert!(deviations.is_empty(), "{deviations:?}");
}

/// Regression: SQLite's decoder reads any non-zero integer as `true`, so trying `bool`
/// first in the dynamic-type fallback made `count(*)` come back as `Boolean(true)`.
#[tokio::test]
async fn dynamic_typed_sql_expressions_decode_as_numbers_not_booleans() {
    let ds = open("keys.sqlite").await;
    let mut row = Vec::new();
    ds.for_each_row_sql(
        "SELECT count(*) AS n, 7 AS i, 1.5 AS f, 'x' AS s FROM parent",
        &mut |r| row = r,
    )
    .unwrap();
    let values: Vec<_> = row.into_iter().map(|(_, v)| v).collect();
    assert_eq!(
        values,
        vec![
            dbcon::NormalizedValue::Integer(2),
            dbcon::NormalizedValue::Integer(7),
            dbcon::NormalizedValue::Float(1.5),
            dbcon::NormalizedValue::Text("x".into()),
        ]
    );
}
