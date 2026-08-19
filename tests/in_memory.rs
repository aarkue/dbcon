//! Sources read from memory rather than from a path.
//!
//! The path a browser `File`, a `wasm32` build, or bytes off a network must take: they have
//! contents but no name to open.

#![cfg(any(feature = "csv", feature = "parquet"))]

use dbcon::{DataSource, SourceData};

#[cfg(feature = "csv")]
#[tokio::test]
async fn a_csv_held_in_memory_reports_its_schema_and_rows() {
    let bytes: &[u8] = b"order_id,placed_at\no1,2024-01-02\no2,2024-01-03\n";
    let source = DataSource::new_csv_bytes("orders".to_string(), bytes.to_vec())
        .await
        .expect("open from bytes");

    let table = source
        .tables
        .get(dbcon::CSV_TABLE_NAME)
        .expect("the one table");
    assert!(
        table.columns.contains_key("order_id"),
        "{:?}",
        table.columns
    );
    assert!(table.columns.contains_key("placed_at"));

    // Read twice: a source is scanned once per consumer, so the bytes must be re-readable.
    for _ in 0..2 {
        let rows = source
            .get_first_rows(dbcon::CSV_TABLE_NAME, 10)
            .expect("rows");
        assert_eq!(rows.len(), 2, "both data rows, header excluded");
    }
}

/// The delimiter is detected from the contents, not assumed to be a comma.
#[cfg(feature = "csv")]
#[tokio::test]
async fn a_semicolon_csv_in_memory_is_detected() {
    let bytes: &[u8] = b"a;b;c\n1;2;3\n4;5;6\n";
    let csv = dbcon::CSVSource::from_bytes_autodetect(bytes.to_vec());
    assert_eq!(csv.delimiter, b';');
    let source = DataSource::new("s".to_string(), csv).await.expect("open");
    assert_eq!(source.tables[dbcon::CSV_TABLE_NAME].columns.len(), 3);
}

/// CSV has no native DISTINCT, so `unique` in `get_all_records` is applied in memory --
/// same as XLSX. This is the CSV half of that behaviour.
#[cfg(feature = "csv")]
#[tokio::test]
async fn unique_dedupes_csv_rows_in_memory() {
    let bytes: &[u8] = b"a,b\n1,x\n1,x\n2,y\n1,x\n";
    let source = DataSource::new_csv_bytes("dupes".to_string(), bytes.to_vec())
        .await
        .expect("open from bytes");

    let all = source
        .get_all_records(dbcon::CSV_TABLE_NAME, &["a", "b"], false)
        .expect("all rows");
    assert_eq!(all.len(), 4);

    let deduped = source
        .get_all_records(dbcon::CSV_TABLE_NAME, &["a", "b"], true)
        .expect("unique rows");
    assert_eq!(deduped.len(), 2);
}

/// `SourceData` reports a path only when there is one; in-memory bytes describe themselves.
#[test]
fn source_data_describes_itself() {
    let path = SourceData::Path("/tmp/x.csv".to_string());
    assert_eq!(path.path(), Some("/tmp/x.csv"));
    assert_eq!(path.describe(), "/tmp/x.csv");

    let mem = SourceData::Memory(vec![1u8, 2, 3].into());
    assert_eq!(mem.path(), None, "in-memory bytes have no path to report");
    assert!(mem.describe().contains("3 bytes"), "{}", mem.describe());
}

/// Parquet from memory, round-tripped through the crate's own writer so the test needs no fixture
/// file. This is the case that could not work at all before: `SerializedFileReader` was built from
/// a `File`, so bytes had to be spilled to disk first.
#[cfg(feature = "parquet")]
#[tokio::test]
async fn a_parquet_file_held_in_memory_reports_its_schema_and_rows() {
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    let schema = Arc::new(
        parse_message_type("message row { REQUIRED INT64 id; REQUIRED INT64 amount; }")
            .expect("schema"),
    );
    let mut buf = Vec::new();
    {
        let mut writer =
            SerializedFileWriter::new(&mut buf, schema, Arc::new(WriterProperties::new()))
                .expect("writer");
        let mut group = writer.next_row_group().expect("row group");
        for values in [[1i64, 2i64], [10, 20]] {
            let mut col = group.next_column().expect("column").expect("some column");
            col.typed::<parquet::data_type::Int64Type>()
                .write_batch(&values, None, None)
                .expect("write");
            col.close().expect("close column");
        }
        group.close().expect("close group");
        writer.close().expect("close writer");
    }

    let source = DataSource::new_parquet_bytes("nums".to_string(), buf)
        .await
        .expect("open parquet from bytes");

    let table = source
        .tables
        .get(dbcon::PARQUET_TABLE_NAME)
        .expect("the one table");
    assert!(table.columns.contains_key("id"), "{:?}", table.columns);
    assert!(table.columns.contains_key("amount"));

    let rows = source
        .get_first_rows(dbcon::PARQUET_TABLE_NAME, 10)
        .expect("rows");
    assert_eq!(rows.len(), 2);
}

/// Parquet has no native DISTINCT either, so `unique` in `get_all_records` is applied in
/// memory the same way as CSV and XLSX -- the Parquet half of that behaviour.
#[cfg(feature = "parquet")]
#[tokio::test]
async fn unique_dedupes_parquet_rows_in_memory() {
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    let schema =
        Arc::new(parse_message_type("message row { REQUIRED INT64 id; }").expect("schema"));
    let mut buf = Vec::new();
    {
        let mut writer =
            SerializedFileWriter::new(&mut buf, schema, Arc::new(WriterProperties::new()))
                .expect("writer");
        let mut group = writer.next_row_group().expect("row group");
        let mut col = group.next_column().expect("column").expect("some column");
        col.typed::<parquet::data_type::Int64Type>()
            .write_batch(&[1i64, 1, 2, 1], None, None)
            .expect("write");
        col.close().expect("close column");
        group.close().expect("close group");
        writer.close().expect("close writer");
    }

    let source = DataSource::new_parquet_bytes("dupes".to_string(), buf)
        .await
        .expect("open parquet from bytes");

    let all = source
        .get_all_records(dbcon::PARQUET_TABLE_NAME, &["id"], false)
        .expect("all rows");
    assert_eq!(all.len(), 4);

    let deduped = source
        .get_all_records(dbcon::PARQUET_TABLE_NAME, &["id"], true)
        .expect("unique rows");
    assert_eq!(deduped.len(), 2);
}
