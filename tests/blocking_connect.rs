//! `DataSource::new_any` / `new_any_without_discovery` are blocking: every backend but
//! PostgreSQL connects and discovers with plain blocking I/O, and PostgreSQL's futures are
//! driven on a short-lived runtime inside the call. Safe to call from any thread, including one
//! already inside a Tokio runtime, where a bare `Runtime::block_on` would panic.

#![cfg(feature = "csv")]

use dbcon::DataSource;
use std::io::Write;

fn write_temp_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn new_any_opens_a_csv_and_discovers_its_schema() {
    let file = write_temp_csv("id,name\n1,alice\n2,bob\n");
    let ds = DataSource::new_any("csv".into(), file.path().to_str().unwrap().into()).expect("open");

    let table = ds.tables.get(dbcon::CSV_TABLE_NAME).expect("the one table");
    assert!(table.columns.contains_key("id"));
    assert!(table.columns.contains_key("name"));
}

#[test]
fn new_any_without_discovery_skips_discovery() {
    let file = write_temp_csv("id\n1\n2\n");
    let ds =
        DataSource::new_any_without_discovery("csv".into(), file.path().to_str().unwrap().into())
            .expect("open");

    assert!(
        ds.tables.is_empty(),
        "without-discovery must not populate the schema"
    );
    let rows = ds
        .get_first_rows(dbcon::CSV_TABLE_NAME, 10)
        .expect("scan still works without discovery");
    assert_eq!(rows.len(), 2);
}

/// A blocking API called from inside a runtime must not panic: `on_a_runtime` hands the work to
/// its own thread rather than calling `Runtime::block_on` on the caller's.
#[tokio::test]
async fn new_any_works_from_inside_an_existing_tokio_runtime() {
    let file = write_temp_csv("id\n1\n");
    let ds = DataSource::new_any("csv".into(), file.path().to_str().unwrap().into())
        .expect("open from inside a Tokio runtime must not panic or error");
    assert!(ds.tables.contains_key(dbcon::CSV_TABLE_NAME));
}
