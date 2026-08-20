# dbcon

Universal Rust connector for tabular data sources.

`dbcon` offers a single API over SQLite, PostgreSQL, DuckDB, CSV, Parquet, and XLSX files. It provides:

- Automatic schema discovery (tables, columns, primary and foreign keys)
- Row iteration with eager (`get_all_records`) and streaming (`scan`) modes
- Distinct-value queries on single columns
- Backend-agnostic `NormalizedValue` and `NormalizedType` so callers don't have to
  branch on source type

**Every public method is blocking, including connecting.** SQLite, DuckDB, CSV, Parquet and
XLSX are blocking code all the way down, so an `async` signature over them would suspend at no
point while forcing every synchronous consumer to own a runtime. Only PostgreSQL genuinely
awaits: `sqlx` is async-only, so its futures are driven on a short-lived runtime inside the
call, and `tokio` is pulled in by the `sql` feature rather than by every build.

## Supported sources

| Source     | Schema discovery | Reading rows |
| ---------- | ---------------- | ------------ |
| PostgreSQL | Yes              | Yes          |
| SQLite     | Yes              | Yes          |
| DuckDB     | Yes              | Yes          |
| CSV        | Headers only     | Yes          |
| Parquet    | Yes              | Yes          |
| XLSX       | Headers only     | Yes          |

## Cargo features

Every backend is a feature and **nothing is on by default** - pick what you need:

```toml
dbcon = { version = "0.3", features = ["sqlite", "csv"] }
```

| Feature | Backend | Pulls in |
| --- | --- | --- |
| `sqlite` | `sqlite:` | `rusqlite` |
| `duckdb` | `duckdb:` | `duckdb` (bundled, built from source) |
| `postgres` | `postgres://`, `postgresql://` | `sqlx` + its PostgreSQL driver |
| `csv` | `csv://`, `*.csv` | `csv` |
| `parquet` | `parquet://`, `*.parquet` | `parquet` |
| `xlsx` | `xlsx://`, `*.xlsx` (one table per sheet) | `calamine` |

`sqlx` is optional and only `postgres` needs it. SQLite goes through `rusqlite`: sqlx
gives every SQLite connection a dedicated OS thread and ships one channel message per row,
which measured at ~12x the cost of stepping the statement directly on a full-table scan.
Measured with `cargo tree -e normal`, unique crates in the dependency graph:

| Features | Crates |
| --- | --- |
| none | 16 |
| `csv` | 19 |
| `sqlite` | 25 |
| `xlsx` | 38 |
| `parquet` | 59 |
| `duckdb` | 76 |
| `postgres` | 128 |
| `sqlite,postgres,csv,parquet` | 169 |

Connection strings:

```text
postgres://user:password@host/db
postgresql://user:password@host/db
sqlite:path/to/file.db
duckdb:path/to/file.duckdb
csv://path/to/file.csv
path/to/file.csv              # Bare .csv path is also accepted
parquet://path/to/file.parquet
path/to/file.parquet          # Bare .parquet path is also accepted
xlsx://path/to/file.xlsx
path/to/file.xlsx             # Bare .xlsx path is also accepted
```

## Usage

```rust,no_run
use dbcon::DataSource;
use std::ops::ControlFlow;

fn main() -> anyhow::Result<()> {
    // Auto-detect source type from the connection string.
    let ds = DataSource::new_any(
        "example".into(),
        "sqlite:orders.db".into(),
    )?;

    // Inspect schema
    for table in ds.get_all_tables() {
        println!("Table: {table}");
    }

    let rows = ds.get_all_records("orders", &["id", "customer"], false)?;
    println!("{} rows", rows.len());

    // Stream large tables without materialising in memory. `row` borrows a buffer reused
    // for every row, so copy anything you keep; `Break` abandons the scan.
    ds.scan("orders", &["id", "customer"], None, &mut |row| {
        println!("{row:?}");
        ControlFlow::Continue(())
    })?;

    Ok(())
}
```

To skip the (potentially slow) schema-discovery step when you only need to run queries,
use `DataSource::new_any_without_discovery`.

## Running the tests

```sh
cargo test --features sqlite,postgres,csv,parquet,duckdb,xlsx
```

Everything except the two targets below runs with no setup: SQLite discovery is covered
by the fixtures in `tests/fixtures`, compared against committed snapshots in
`tests/snapshots`.

**`tests/corpus.rs`** runs discovery over real database files under `$DBCON_CORPUS`
(`datasets/Chinook_Sqlite.sqlite`, `datasets/northwind.db`, `ocel/*.sqlite`). It has no
libtest harness so that an unset `DBCON_CORPUS` prints a visible "did not run" banner
instead of quietly passing:

```sh
DBCON_CORPUS=~/dow  cargo test --release --features sqlite --test corpus
DBCON_CORPUS_REQUIRED=1 cargo test --features sqlite --test corpus  # absence is a failure
DBCON_CORPUS_FULL=1 ...    # scan every row rather than stopping at the 2M-row budget
DBCON_SNAPSHOT_UPDATE=1 ...  # rewrite the committed snapshots
```

**`tests/postgres.rs`** needs `POSTGRES_URL` (read from `.env`, see `.env.example`).
There is no PostgreSQL corpus file - a Postgres schema lives in a server, not a file -
so without a reachable server that half of discovery is not exercised. The test says so
rather than passing silently; `DBCON_POSTGRES_REQUIRED=1` makes it a failure.

## Dependency notes

Schema discovery is dbcon's own: `src/discovery/` queries `sqlite_master`/`PRAGMA` and
`information_schema` directly, with no external schema-introspection dependency.

`dbcon` depends on plain `sqlx` from crates.io, used only for the `postgres` backend; a
`sqlite`-only build does not compile it at all.

## License

MIT. See [`LICENSE`](LICENSE).
