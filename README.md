# dbcon

Universal Rust connector for tabular data sources.

`dbcon` offers a single async API over SQLite, PostgreSQL, and CSV files. It provides:

- Automatic schema discovery (tables, columns, primary and foreign keys)
- Row iteration with eager (`get_all_records`) and streaming (`for_each_record`) modes
- Distinct-value queries on single columns
- Backend-agnostic `NormalizedValue` and `NormalizedType` so callers don't have to
  branch on source type

## Supported sources

| Source     | Schema discovery | Reading rows |
| ---------- | ---------------- | ------------ |
| PostgreSQL | Yes              | Yes          |
| SQLite     | Yes              | Yes          |
| CSV        | Headers only     | Yes          |

Connection strings:

```text
postgres://user:password@host/db
postgresql://user:password@host/db
sqlite:path/to/file.db
csv://path/to/file.csv
path/to/file.csv              # Bare .csv path is also accepted
```

## Usage

```rust,no_run
use dbcon::DataSource;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Auto-detect source type from the connection string
    let ds = DataSource::new_any(
        "example".into(),
        "sqlite:orders.db".into(),
    ).await?;

    // Inspect schema
    for table in ds.get_all_tables() {
        println!("Table: {table}");
    }

    // Read all rows of a table for a subset of columns
    let rows = ds.get_all_records("orders", &["id", "customer"], false).await?;
    println!("{} rows", rows.len());

    // Stream large tables without materialising in memory
    ds.for_each_record("orders", &["id", "customer"], None, |row| {
        println!("{row:?}");
    }).await?;

    Ok(())
}
```

To skip the (potentially slow) schema-discovery step when you only need to run queries,
use `DataSource::new_any_without_discovery`.

## Running the tests

The tests require a running PostgreSQL instance, an SQLite database, and a CSV file.
Paths/URLs are read from a `.env` file (see `.env.example`):

```text
POSTGRES_URL=postgres://user:password@localhost/dbname
SQLITE_PATH=sqlite:path/to/database.sqlite
CSV_PATH=path/to/file.csv
```

Then run `cargo test`.

## Dependency notes

`dbcon` currently depends on a patched fork of `sqlx`
([aarkue/sqlx-fix](https://github.com/aarkue/sqlx-fix), branch `sqlite3-fix`) to work
around an upstream SQLite parsing issue, and on a release-candidate of `sea-schema`.
Both are git dependencies, so `dbcon` cannot be published to crates.io as-is;
downstream consumers must mirror the `[patch.crates-io]` entry in their workspace
`Cargo.toml`.

## License

MIT. See [`LICENSE`](LICENSE).
