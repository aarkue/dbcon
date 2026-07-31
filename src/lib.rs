//! # dbcon
//!
//! A small universal connector for tabular data sources.
//!
//! dbcon offers a uniform API over SQLite, PostgreSQL, CSV, and Parquet files: schema
//! discovery (tables, columns, primary and foreign keys), row iteration, streaming, and
//! distinct-value queries. Values are exposed via a normalised [`NormalizedValue`] enum
//! so callers can handle data from any backend without knowing the source-specific type
//! system.
//!
//! ## Quick start
//!
//! ```no_run
//! use dbcon::DataSource;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let ds = DataSource::new_any("example".into(), "sqlite:path/to/db.sqlite".into()).await?;
//! for table in ds.get_all_tables() {
//!     println!("{table}");
//! }
//! # Ok(()) }
//! ```
//!
//! ## Async where it earns its keep, synchronous where it does not
//!
//! **Connecting and discovering a schema are `async`. Reading rows is not.**
//!
//! Connection setup and schema discovery involve network round trips and happen once per
//! source, so they belong in async. Reading rows is a tight loop over a callback --
//! [`DataSource::scan`] takes `&mut dyn FnMut(&[NormalizedValue]) -> ControlFlow<()>`, and
//! there is no await point between two rows for a caller to interleave anything with. Three
//! of the four backends (SQLite via `rusqlite`, CSV, Parquet) are blocking code all the way
//! down; only PostgreSQL is genuinely async, and it drives its own runtime once per scan.
//!
//! An async caller is therefore responsible for the bridge, because only it knows its own
//! runtime: wrap a scan in `tokio::task::spawn_blocking`. Calling a row-reading method from
//! inside a runtime with a PostgreSQL source is refused with an error naming that, rather
//! than panicking inside Tokio.
//!
//! ## Feature flags
//!
//! Nothing is enabled by default; pick the backends you need.
//!
//! | Feature | Enables | Pulls in |
//! |---|---|---|
//! | `sqlite` | `sqlite:` connection strings | `rusqlite` |
//! | `postgres` | `postgres://` / `postgresql://` | `sqlx` with its PostgreSQL driver |
//! | `csv` | `csv://` and `*.csv` paths | `csv` |
//! | `parquet` | `parquet://` and `*.parquet` paths | `parquet` |
//!
//! `sqlx` is optional and, notably, **not** what `sqlite` uses: see [`mod@sqlite`] for why
//! stepping SQLite's own statement handle beats routing every row through a worker thread
//! and a channel. `sql` is an internal aggregate feature enabled by `postgres`; it is not
//! usable on its own.

// With no backend feature enabled `DataSourceInner` is uninhabited, so every dispatch
// body below is unreachable and its bindings are never read. That is the point of the
// build, not an oversight.
#![cfg_attr(
    not(any(feature = "sql", feature = "csv", feature = "parquet")),
    allow(unused_variables, unused_mut, unreachable_code)
)]

#[cfg(all(feature = "sql", not(feature = "postgres")))]
compile_error!("the `sql` feature is internal plumbing; enable the `postgres` feature instead");

mod discovery;
#[cfg(feature = "sqlite")]
pub mod sqlite;
mod types;

#[cfg(feature = "sqlite")]
pub use sqlite::SqliteSource;
pub use types::{NormalizedType, NormalizedValue, SqliteAffinity, sqlite_affinity};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::ControlFlow;

#[cfg(feature = "sql")]
use chrono::{FixedOffset, NaiveDateTime};
#[cfg(feature = "sql")]
use sqlx::{Column, ColumnIndex, Decode, Row, TypeInfo};

#[cfg(feature = "postgres")]
use sqlx::PgPool;

#[cfg(feature = "parquet")]
use parquet::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
#[cfg(feature = "parquet")]
use parquet::file::reader::{FileReader, SerializedFileReader};
#[cfg(feature = "parquet")]
use parquet::record::Field;
#[cfg(feature = "parquet")]
use parquet::schema::types::Type as ParquetType;
#[cfg(feature = "parquet")]
use std::fs::File;

/// The connection-string forms *this build* accepts, which depends on which backend
/// features are enabled. Used to make the "unsupported data source" error name the
/// feature set rather than a fixed list the binary may not actually support.
fn supported_connection_strings() -> String {
    let forms: &[&str] = &[
        #[cfg(feature = "postgres")]
        "`postgres://`, `postgresql://`",
        #[cfg(feature = "sqlite")]
        "`sqlite:`",
        #[cfg(feature = "csv")]
        "`csv://` or a path ending in `.csv`",
        #[cfg(feature = "parquet")]
        "`parquet://` or a path ending in `.parquet`",
    ];
    if forms.is_empty() {
        "nothing (this build enables no backend feature)".to_string()
    } else {
        forms.join(", ")
    }
}

/// A connected data source (PostgreSQL, SQLite, or CSV) with discovered schema.
///
/// Construct via one of the `new_*` methods. `new` and its variants perform full schema
/// discovery on connection; `new_*_without_discovery` skips it for query-only workloads
/// where table/column introspection isn't needed.
#[derive(Debug, Serialize)]
pub struct DataSource {
    pub name: String,
    pub tables: HashMap<String, DataTableInfo>,
    #[serde(skip)]
    inner: DataSourceInner,
}

impl DataSource {
    pub async fn new(name: String, from: impl Into<DataSourceInner>) -> anyhow::Result<Self> {
        let inner = from.into();
        Ok(Self {
            name,
            tables: inner.get_tables().await?,
            inner,
        })
    }

    /// Create a connection without schema discovery (for query-only use cases like extraction)
    pub async fn new_without_discovery(
        name: String,
        from: impl Into<DataSourceInner>,
    ) -> anyhow::Result<Self> {
        let inner = from.into();
        Ok(Self {
            name,
            tables: HashMap::new(),
            inner,
        })
    }

    #[cfg(feature = "postgres")]
    pub async fn new_postgres(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(name, PgPool::connect(&connection_string).await?).await
    }

    #[cfg(feature = "sqlite")]
    pub async fn new_sqlite(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(name, SqliteSource::open(&connection_string)?).await
    }

    #[cfg(feature = "csv")]
    pub async fn new_csv(name: String, path: String) -> anyhow::Result<Self> {
        let delimiter = detect_csv_delimiter(&path).unwrap_or(b',');
        Self::new(
            name,
            CSVSource {
                data: SourceData::Path(path),
                delimiter,
                has_headers: true,
            },
        )
        .await
    }

    #[cfg(feature = "parquet")]
    pub async fn new_parquet(name: String, path: String) -> anyhow::Result<Self> {
        Self::new(
            name,
            ParquetSource {
                data: SourceData::Path(path),
            },
        )
        .await
    }

    /// A CSV source over bytes already in memory, with its delimiter detected from the contents.
    ///
    /// The counterpart to [`DataSource::new_csv`] for contents with no path: a browser upload, a
    /// `wasm32` build with no filesystem, or a file fetched over the network.
    #[cfg(feature = "csv")]
    pub async fn new_csv_bytes(
        name: String,
        bytes: impl Into<std::sync::Arc<[u8]>>,
    ) -> anyhow::Result<Self> {
        Self::new(name, CSVSource::from_bytes_autodetect(bytes)).await
    }

    /// A Parquet source over bytes already in memory.
    #[cfg(feature = "parquet")]
    pub async fn new_parquet_bytes(
        name: String,
        bytes: impl Into<std::sync::Arc<[u8]>>,
    ) -> anyhow::Result<Self> {
        Self::new(
            name,
            ParquetSource {
                data: SourceData::Memory(bytes.into()),
            },
        )
        .await
    }

    /// A multi-table source assembled from one file per table -- a directory of CSV or Parquet
    /// files, or the members of an archive. Build the [`TableSetSource`] with
    /// [`TableSetSource::insert_csv`]/[`insert_parquet`](TableSetSource::insert_parquet).
    #[cfg(any(
        feature = "sql",
        feature = "sqlite",
        feature = "csv",
        feature = "parquet"
    ))]
    pub async fn new_table_set(name: String, tables: TableSetSource) -> anyhow::Result<Self> {
        Self::new(name, tables).await
    }

    pub async fn new_any(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(
            name,
            Self::inner_from_connection_string(&connection_string).await?,
        )
        .await
    }

    /// Connect without schema discovery; only establishes the connection for querying.
    /// Much faster than `new_any` for databases with many tables.
    pub async fn new_any_without_discovery(
        name: String,
        connection_string: String,
    ) -> anyhow::Result<Self> {
        Self::new_without_discovery(
            name,
            Self::inner_from_connection_string(&connection_string).await?,
        )
        .await
    }

    async fn inner_from_connection_string(
        connection_string: &str,
    ) -> anyhow::Result<DataSourceInner> {
        #[cfg(feature = "postgres")]
        if connection_string.starts_with("postgres://")
            || connection_string.starts_with("postgresql://")
        {
            return Ok(PgPool::connect(connection_string).await?.into());
        }
        #[cfg(feature = "sqlite")]
        if connection_string.starts_with("sqlite:") {
            return Ok(SqliteSource::open(connection_string)?.into());
        }
        #[cfg(feature = "csv")]
        if let Some(rest) = connection_string.strip_prefix("csv://") {
            return Ok(CSVSource::from_csv_spec(rest).into());
        }
        #[cfg(feature = "parquet")]
        if let Some(rest) = connection_string.strip_prefix("parquet://") {
            return Ok(ParquetSource {
                data: SourceData::Path(rest.to_string()),
            }
            .into());
        }
        #[cfg(feature = "csv")]
        if connection_string.ends_with(".csv") {
            return Ok(CSVSource::from_path_autodetect(connection_string.to_string()).into());
        }
        #[cfg(feature = "parquet")]
        if connection_string.ends_with(".parquet") {
            return Ok(ParquetSource {
                data: SourceData::Path(connection_string.to_string()),
            }
            .into());
        }
        anyhow::bail!(
            "Unsupported data source `{}`. This build of dbcon accepts: {}. \
             Rebuild with the matching cargo feature to add a backend.",
            connection_string,
            supported_connection_strings(),
        )
    }

    /// Every column whose declared type dbcon could not map, as
    /// `(table, column, declared type)`.
    ///
    /// Empty is the expected result. A non-empty result is the signal that a caller's
    /// type-dependent behaviour (literal coercion, join-key comparison) will fall back to
    /// dynamic decoding for those columns - see [`NormalizedType`]'s `Unknown` contract.
    pub fn unknown_column_types(&self) -> Vec<(&str, &str, &str)> {
        let mut out: Vec<(&str, &str, &str)> = self
            .tables
            .values()
            .flat_map(|t| {
                t.columns.values().filter_map(move |c| {
                    c.col_type
                        .unknown_type()
                        .map(|raw| (t.name.as_str(), c.name.as_str(), raw))
                })
            })
            .collect();
        out.sort_unstable();
        out
    }

    pub fn get_first_rows_of_all_tables(
        &self,
        n: usize,
    ) -> anyhow::Result<HashMap<String, Vec<HashMap<String, String>>>> {
        let mut result = HashMap::new();
        for table in self.get_all_tables() {
            let rows = self.get_first_rows(table, n)?;
            result.insert(table.clone(), rows);
        }
        Ok(result)
    }

    pub fn get_all_tables(&self) -> impl Iterator<Item = &String> {
        self.tables.keys()
    }

    pub fn get_all_records(
        &self,
        table: &str,
        columns: &[&str],
        unique: bool,
    ) -> anyhow::Result<Vec<Vec<NormalizedValue>>> {
        self.inner.get_first_records(table, columns, None, unique)
    }

    pub fn get_all_rows(&self, table: &str) -> anyhow::Result<Vec<HashMap<String, String>>> {
        self.inner.get_first_rows(table, None)
    }

    pub fn get_first_rows(
        &self,
        table: &str,
        n: usize,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        self.inner.get_first_rows(table, Some(n))
    }

    /// Read `columns` from `table`, calling `handler` once per row. Rows are streamed and
    /// never fully materialised. Optional `order_by` appends ORDER BY columns to the query.
    ///
    /// # Synchronous on purpose
    ///
    /// There is no await point between two rows, so `async` here would buy a caller nothing
    /// while forcing every synchronous consumer to own a runtime and a bridge. See the crate
    /// docs for the full boundary, and for what an async caller owes in return
    /// (`spawn_blocking`).
    ///
    /// # The handler
    ///
    /// `handler` receives a slice **borrowed from a buffer the backend reuses for every row**,
    /// so it must copy anything it wants to keep. Returning [`ControlFlow::Break`] abandons
    /// the scan then and there -- the query stops, it is not merely ignored -- which is what
    /// makes a `LIMIT`-shaped consumer, or one that has hit a fatal error, cost what it
    /// should.
    ///
    /// An **empty** `columns` is a legitimate request for "one callback per row, no values",
    /// not a request for every column: the handler gets a zero-length slice once per row.
    pub fn scan(
        &self,
        table: &str,
        columns: &[&str],
        order_by: Option<&[&str]>,
        handler: &mut dyn FnMut(&[NormalizedValue]) -> ControlFlow<()>,
    ) -> anyhow::Result<()> {
        self.inner.scan(table, columns, order_by, handler)
    }

    /// Run an arbitrary SQL query against the underlying SQL connection, streaming each row to
    /// `handler` as a `Vec<(column_name, NormalizedValue)>`. Errors out if this `DataSource`
    /// is backed by CSV or Parquet, which do not accept arbitrary SQL.
    ///
    /// Synchronous, for the reasons on [`DataSource::scan`]. Unlike `scan` this hands over an
    /// owned `Vec` per row, since the column names make a shared buffer pointless.
    pub fn for_each_row_sql(
        &self,
        sql: &str,
        handler: &mut dyn FnMut(Vec<(String, NormalizedValue)>),
    ) -> anyhow::Result<()> {
        self.inner.for_each_row_sql(sql, handler)
    }

    /// Run SELECT DISTINCT on a single column. Useful for discovering attribute names.
    pub fn get_distinct_values(&self, table: &str, column: &str) -> anyhow::Result<Vec<String>> {
        self.inner.get_distinct_values(table, column)
    }
}

/// Schema information for a single table: its columns and key constraints.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataTableInfo {
    pub name: String,
    pub columns: HashMap<String, DataColumnInfo>,
    pub primary_keys: Vec<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
}

/// A primary-key or unique constraint over one or more columns.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryKey {
    pub name: String,
    pub columns: Vec<String>,
}

/// A foreign-key reference from this table's `from_columns` to `to_table.to_columns`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub from_columns: Vec<String>,
    pub to_table: String,
    pub to_columns: Vec<String>,
}

/// Metadata for a single column.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataColumnInfo {
    pub name: String,
    pub col_type: NormalizedType,
    pub is_nullable: bool,
}

#[derive(Debug)]
pub enum DataSourceInner {
    #[cfg(feature = "sql")]
    SQL(SQLPool),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteSource),
    #[cfg(feature = "csv")]
    CSV(CSVSource),
    #[cfg(feature = "parquet")]
    Parquet(ParquetSource),
    /// Several single-table sources presented as one multi-table source.
    #[cfg(any(
        feature = "sql",
        feature = "sqlite",
        feature = "csv",
        feature = "parquet"
    ))]
    TableSet(TableSetSource),
}
/// Several single-table sources presented as one multi-table source: one file per table.
///
/// Exists because a schema spread over a directory of CSV or Parquet files is a real source
/// shape that neither [`CSVSource`] nor [`ParquetSource`] can describe on its own -- each is
/// one table under the fixed name `main`.
///
/// Table names are the caller's and are never inferred from a filename. A format whose
/// manifest declares which file holds which table (the OCEL 2.0 bundle, say) would otherwise
/// have its naming rules re-derived here, wrongly, from a path.
#[cfg(any(
    feature = "sql",
    feature = "sqlite",
    feature = "csv",
    feature = "parquet"
))]
#[derive(Debug, Default)]
pub struct TableSetSource {
    members: HashMap<String, TableSetMember>,
}

/// One table of a [`TableSetSource`]: a source, and the name that source knows the table by.
#[cfg(any(
    feature = "sql",
    feature = "sqlite",
    feature = "csv",
    feature = "parquet"
))]
#[derive(Debug)]
struct TableSetMember {
    inner: DataSourceInner,
    /// What `inner` calls it, which is `main` for both single-file sources.
    inner_table: String,
}

#[cfg(any(
    feature = "sql",
    feature = "sqlite",
    feature = "csv",
    feature = "parquet"
))]
impl TableSetSource {
    /// Add `table`, backed by `inner`'s table `inner_table`. Replaces any table of that name.
    pub fn insert(
        &mut self,
        table: impl Into<String>,
        inner: impl Into<DataSourceInner>,
        inner_table: impl Into<String>,
    ) {
        self.members.insert(
            table.into(),
            TableSetMember {
                inner: inner.into(),
                inner_table: inner_table.into(),
            },
        );
    }

    /// A CSV file as one table. Its delimiter is detected from the contents.
    #[cfg(feature = "csv")]
    pub fn insert_csv(&mut self, table: impl Into<String>, data: SourceData) {
        let delimiter = detect_csv_delimiter_in(&data).unwrap_or(b',');
        self.insert(
            table,
            CSVSource {
                data,
                delimiter,
                has_headers: true,
            },
            CSV_TABLE_NAME,
        );
    }

    /// A Parquet file as one table.
    #[cfg(feature = "parquet")]
    pub fn insert_parquet(&mut self, table: impl Into<String>, data: SourceData) {
        self.insert(table, ParquetSource { data }, PARQUET_TABLE_NAME);
    }

    /// The table names, in no particular order.
    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.members.keys().map(String::as_str)
    }

    fn member(&self, table: &str) -> anyhow::Result<&TableSetMember> {
        self.members
            .get(table)
            .ok_or_else(|| anyhow::anyhow!("no table '{table}' in this source"))
    }
}

#[cfg(any(
    feature = "sql",
    feature = "sqlite",
    feature = "csv",
    feature = "parquet"
))]
impl From<TableSetSource> for DataSourceInner {
    fn from(set: TableSetSource) -> Self {
        Self::TableSet(set)
    }
}

#[cfg(feature = "postgres")]
impl From<PgPool> for DataSourceInner {
    fn from(pool: PgPool) -> Self {
        Self::SQL(pool.into())
    }
}
#[cfg(feature = "sqlite")]
impl From<SqliteSource> for DataSourceInner {
    fn from(source: SqliteSource) -> Self {
        Self::Sqlite(source)
    }
}
#[cfg(feature = "csv")]
impl From<CSVSource> for DataSourceInner {
    fn from(csv: CSVSource) -> Self {
        Self::CSV(csv)
    }
}
#[cfg(feature = "parquet")]
impl From<ParquetSource> for DataSourceInner {
    fn from(parquet: ParquetSource) -> Self {
        Self::Parquet(parquet)
    }
}

/// Build a SELECT query string from columns, table, and optional ORDER BY.
///
/// An **empty** `columns` selects the constant `1`, because `SELECT  FROM "t"` is not SQL in
/// any dialect. That gives the right row count and needs no backend-specific syntax, at the
/// cost of one column the caller did not ask for; every caller therefore truncates each row
/// to `columns.len()` before handing it on. Asking for no columns is a real request -- a
/// mapping whose targets are all constants still needs one callback per row.
#[cfg(any(feature = "sql", feature = "sqlite"))]
fn build_select_query(
    columns: &[&str],
    table: &str,
    order_by: Option<&[&str]>,
    limit: Option<usize>,
    unique: bool,
) -> String {
    let mut col_str = String::new();
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            col_str.push_str(", ");
        }
        col_str.push('"');
        col_str.push_str(col);
        col_str.push('"');
    }
    if columns.is_empty() {
        col_str.push('1');
    }
    let distinct = if unique { "DISTINCT " } else { "" };
    let mut query = format!("SELECT {}{} FROM \"{}\"", distinct, col_str, table);
    if let Some(order_cols) = order_by
        && !order_cols.is_empty()
    {
        query.push_str(" ORDER BY ");
        for (i, col) in order_cols.iter().enumerate() {
            if i > 0 {
                query.push_str(", ");
            }
            query.push('"');
            query.push_str(col);
            query.push('"');
        }
    }
    if let Some(limit) = limit {
        query.push_str(&format!(" LIMIT {}", limit));
    }
    query
}

/// Drive `future` to completion from synchronous code, for the one backend that is really
/// async.
///
/// A fresh current-thread runtime per call rather than a shared one: this runs once per
/// row-reading call, never per row, and a shared current-thread runtime would serialise
/// concurrent scans against each other for no gain.
///
/// # Being inside a runtime is an error, not a panic
///
/// `Runtime::block_on` panics when the *calling thread* is already inside any runtime's
/// context. Detecting that and returning an error instead means an async caller who forgot
/// `spawn_blocking` gets a message naming the fix, rather than a Tokio panic from inside a
/// library they did not know was blocking.
/// Drop the placeholder column [`build_select_query`] adds for an empty `columns` request.
/// A no-op for every other request, since `len` is then the row's own width.
#[cfg(feature = "sql")]
fn truncated(mut row: Vec<NormalizedValue>, len: usize) -> Vec<NormalizedValue> {
    row.truncate(len);
    row
}

#[cfg(feature = "sql")]
fn block_on<F: std::future::Future>(future: F) -> anyhow::Result<F::Output> {
    if tokio::runtime::Handle::try_current().is_ok() {
        anyhow::bail!(
            "dbcon's row-reading API is synchronous, but this PostgreSQL source was read from \
             a thread that is already inside a Tokio runtime, where blocking would panic. \
             Wrap the call in `tokio::task::spawn_blocking`."
        );
    }
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(future))
}

#[cfg(feature = "sql")]
fn extract_row_column_value<'d, C, R, D: sqlx::Database>(
    row: &'d R,
    col: &'d C,
) -> Result<NormalizedValue, sqlx::Error>
where
    C: Column<Database = D>,
    R: Row<Database = D>,
    &'d str: ColumnIndex<R>,
    String: Decode<'d, D> + sqlx::Type<D>,
    i64: Decode<'d, D> + sqlx::Type<D>,
    i32: Decode<'d, D> + sqlx::Type<D>,
    f64: Decode<'d, D> + sqlx::Type<D>,
    f32: Decode<'d, D> + sqlx::Type<D>,
    bool: Decode<'d, D> + sqlx::Type<D>,
    chrono::DateTime<FixedOffset>: Decode<'d, D> + sqlx::Type<D>,
    chrono::NaiveDateTime: Decode<'d, D> + sqlx::Type<D>,
{
    let t = col.type_info().name();
    let t = NormalizedType::from_raw(t);
    let i = col.name();
    let res = match t {
        NormalizedType::Text => row.try_get::<Option<String>, _>(i)?.into(),
        NormalizedType::Integer => {
            if let Ok(i) = row.try_get::<Option<i64>, _>(i) {
                i.into()
            } else if let Ok(i) = row.try_get::<Option<i32>, _>(i) {
                i.into()
            } else {
                NormalizedValue::Null
            }
        }
        NormalizedType::Boolean => row.try_get::<Option<bool>, _>(i)?.into(),
        NormalizedType::Timestamp => {
            if let Ok(i) = row.try_get::<chrono::DateTime<FixedOffset>, _>(i) {
                i.into()
            } else if let Ok(i) = row.try_get::<Option<NaiveDateTime>, _>(i) {
                i.map(|dt| dt.and_utc().fixed_offset()).into()
            } else {
                // Fallback: return as text so callers can try custom parsing
                // (e.g., timestamps without seconds that sqlx can't handle)
                row.try_get::<Option<String>, _>(i)?.into()
            }
        }
        NormalizedType::Float => {
            if let Ok(i) = row.try_get::<Option<f64>, _>(i) {
                i.into()
            } else if let Ok(i) = row.try_get::<Option<f32>, _>(i) {
                i.into()
            } else {
                NormalizedValue::Null
            }
        }
        _ => {
            // Dynamic column type with no static SQL type (e.g. `count(*)`, CASE WHEN).
            // Integers are tried before bool: SQLite's decoder is permissive and will
            // happily read any non-zero integer as `true`, which turned `count(*)` into
            // `Boolean(true)`. PostgreSQL type-checks its decodes, so a genuine boolean
            // expression still falls through to the bool arm there.
            if let Ok(n) = row.try_get::<Option<i64>, _>(i) {
                n.into()
            } else if let Ok(n) = row.try_get::<Option<i32>, _>(i) {
                n.into()
            } else if let Ok(n) = row.try_get::<Option<f64>, _>(i) {
                n.into()
            } else if let Ok(b) = row.try_get::<Option<bool>, _>(i) {
                b.into()
            } else if let Ok(s) = row.try_get::<Option<String>, _>(i) {
                s.into()
            } else {
                NormalizedValue::Null
            }
        }
    };
    Ok(res)
}

/// Map a sqlx row to a `Vec<NormalizedValue>`, one per column.
/// Columns that fail to decode yield [`NormalizedValue::Null`].
#[cfg(feature = "sql")]
fn rows_to_values<R, D>(row: R) -> Vec<NormalizedValue>
where
    D: sqlx::Database,
    R: Row<Database = D>,
    for<'d> &'d str: ColumnIndex<R>,
    for<'d> String: Decode<'d, D> + sqlx::Type<D>,
    for<'d> i64: Decode<'d, D> + sqlx::Type<D>,
    for<'d> i32: Decode<'d, D> + sqlx::Type<D>,
    for<'d> f64: Decode<'d, D> + sqlx::Type<D>,
    for<'d> f32: Decode<'d, D> + sqlx::Type<D>,
    for<'d> bool: Decode<'d, D> + sqlx::Type<D>,
    for<'d> chrono::DateTime<FixedOffset>: Decode<'d, D> + sqlx::Type<D>,
    for<'d> chrono::NaiveDateTime: Decode<'d, D> + sqlx::Type<D>,
{
    row.columns()
        .iter()
        .map(|col| extract_row_column_value(&row, col).unwrap_or_default())
        .collect()
}

/// Map a sqlx row to `{column_name -> stringified value}`.
/// Used by the preview/string-rows API for display purposes.
#[cfg(feature = "sql")]
fn row_to_named_strings<R, D>(row: R) -> HashMap<String, String>
where
    D: sqlx::Database,
    R: Row<Database = D>,
    for<'d> &'d str: ColumnIndex<R>,
    for<'d> String: Decode<'d, D> + sqlx::Type<D>,
    for<'d> i64: Decode<'d, D> + sqlx::Type<D>,
    for<'d> i32: Decode<'d, D> + sqlx::Type<D>,
    for<'d> f64: Decode<'d, D> + sqlx::Type<D>,
    for<'d> f32: Decode<'d, D> + sqlx::Type<D>,
    for<'d> bool: Decode<'d, D> + sqlx::Type<D>,
    for<'d> chrono::DateTime<FixedOffset>: Decode<'d, D> + sqlx::Type<D>,
    for<'d> chrono::NaiveDateTime: Decode<'d, D> + sqlx::Type<D>,
{
    row.columns()
        .iter()
        .map(|col| {
            let value = extract_row_column_value(&row, col).unwrap_or_default();
            (col.name().to_string(), value.to_string())
        })
        .collect()
}

impl DataSourceInner {
    pub async fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        match self {
            // With no backend feature enabled `DataSourceInner` is uninhabited, so this
            // is the only arm and it is unreachable. With any feature on it is cfg'd out.
            #[cfg(not(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            )))]
            _ => match *self {},
            #[cfg(feature = "sql")]
            DataSourceInner::SQL(sql) => sql.get_tables().await,
            #[cfg(feature = "sqlite")]
            DataSourceInner::Sqlite(source) => discovery::sqlite::discover(source),
            #[cfg(feature = "csv")]
            DataSourceInner::CSV(csv) => csv.get_tables().await,
            #[cfg(feature = "parquet")]
            DataSourceInner::Parquet(p) => p.get_tables().await,
            #[cfg(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            ))]
            DataSourceInner::TableSet(set) => {
                let mut tables = HashMap::new();
                for (name, m) in &set.members {
                    // Boxed because this recurses into `get_tables`, and an `async fn` cannot
                    // name its own future type.
                    let mut inner = Box::pin(m.inner.get_tables()).await?;
                    let Some(mut info) = inner.remove(&m.inner_table) else {
                        anyhow::bail!(
                            "table '{name}' names '{}', which its source does not have",
                            m.inner_table
                        );
                    };
                    info.name.clone_from(name);
                    tables.insert(name.clone(), info);
                }
                Ok(tables)
            }
        }
    }

    pub fn get_first_records(
        &self,
        table: &str,
        columns: &[&str],
        limit: Option<usize>,
        unique: bool,
    ) -> anyhow::Result<Vec<Vec<NormalizedValue>>> {
        match self {
            // With no backend feature enabled `DataSourceInner` is uninhabited, so this
            // is the only arm and it is unreachable. With any feature on it is cfg'd out.
            #[cfg(not(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            )))]
            _ => match *self {},
            #[cfg(feature = "sql")]
            DataSourceInner::SQL(sql) => {
                let query = build_select_query(columns, table, None, limit, unique);
                match sql {
                    #[cfg(feature = "postgres")]
                    SQLPool::Postgres(pg_pool) => {
                        let rows = block_on(sqlx::query(&query).fetch_all(pg_pool))??;
                        Ok(rows
                            .into_iter()
                            .map(|row| truncated(rows_to_values(row), columns.len()))
                            .collect())
                    }
                }
            }
            #[cfg(feature = "sqlite")]
            DataSourceInner::Sqlite(source) => {
                let query = build_select_query(columns, table, None, limit, unique);
                let mut rows = source.rows(&query, limit)?;
                for row in &mut rows {
                    row.truncate(columns.len());
                }
                Ok(rows)
            }
            #[cfg(feature = "csv")]
            DataSourceInner::CSV(csv) => {
                let _ = (table, unique);
                let mut rdr = csv.reader()?;

                let headers = rdr.headers()?.clone();
                let indices = columns
                    .iter()
                    .map(|col| {
                        headers
                            .iter()
                            .position(|h| h == *col)
                            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in CSV", col))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut result = Vec::new();
                for record in rdr.into_records() {
                    let record = record?;
                    let row = indices
                        .iter()
                        .map(|&i| {
                            record
                                .get(i)
                                .map(|v| NormalizedValue::Text(v.to_string()))
                                .unwrap_or_default()
                        })
                        .collect();
                    result.push(row);
                    if let Some(limit) = limit
                        && result.len() >= limit
                    {
                        break;
                    }
                }
                Ok(result)
            }
            #[cfg(feature = "parquet")]
            DataSourceInner::Parquet(p) => {
                // `unique` is ignored: Parquet has no native DISTINCT, same as CSV.
                let _ = unique;
                let reader = p.open()?;
                let schema = reader.metadata().file_metadata().schema();
                let field_names: Vec<&str> = schema.get_fields().iter().map(|f| f.name()).collect();
                let ts_units: Vec<Option<TimeUnit>> = schema
                    .get_fields()
                    .iter()
                    .map(|f| parquet_timestamp_unit(f))
                    .collect();
                let indices: Vec<usize> = columns
                    .iter()
                    .map(|col| {
                        field_names
                            .iter()
                            .position(|h| h == col)
                            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in Parquet", col))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut result: Vec<Vec<NormalizedValue>> = Vec::new();
                for row in reader.get_row_iter(None)? {
                    let row = row?;
                    let fields: Vec<&Field> = row.get_column_iter().map(|(_, f)| f).collect();
                    let values: Vec<NormalizedValue> = indices
                        .iter()
                        .map(|&i| {
                            fields
                                .get(i)
                                .map(|f| parquet_field_to_value_hinted(f, ts_units[i]))
                                .unwrap_or_default()
                        })
                        .collect();
                    result.push(values);
                    if let Some(limit) = limit
                        && result.len() >= limit
                    {
                        break;
                    }
                }
                let _ = table;
                Ok(result)
            }
            #[cfg(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            ))]
            DataSourceInner::TableSet(set) => {
                let m = set.member(table)?;
                m.inner
                    .get_first_records(&m.inner_table, columns, limit, unique)
            }
        }
    }

    /// See [`DataSource::scan`], which this backs.
    pub fn scan(
        &self,
        table: &str,
        columns: &[&str],
        order_by: Option<&[&str]>,
        handler: &mut dyn FnMut(&[NormalizedValue]) -> ControlFlow<()>,
    ) -> anyhow::Result<()> {
        #[cfg(any(feature = "sql", feature = "sqlite"))]
        let query = build_select_query(columns, table, order_by, None, false);
        match self {
            // With no backend feature enabled `DataSourceInner` is uninhabited, so this
            // is the only arm and it is unreachable. With any feature on it is cfg'd out.
            #[cfg(not(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            )))]
            _ => match *self {},
            #[cfg(feature = "sql")]
            DataSourceInner::SQL(sql) => match sql {
                #[cfg(feature = "postgres")]
                SQLPool::Postgres(pool) => {
                    use futures::StreamExt;
                    // One `block_on` for the whole scan, not one per row: the future below
                    // owns the row loop, so the calling thread blocks once and the handler
                    // is called from inside it, synchronously.
                    block_on(async {
                        let mut buffer: Vec<NormalizedValue> = Vec::new();
                        let mut stream = sqlx::query(&query).fetch(pool);
                        while let Some(result) = stream.next().await {
                            let row = result?;
                            buffer.clear();
                            buffer.extend(row.columns().iter().map(|col| {
                                extract_row_column_value(&row, col).unwrap_or_default()
                            }));
                            buffer.truncate(columns.len());
                            if handler(&buffer).is_break() {
                                break;
                            }
                        }
                        Ok::<_, anyhow::Error>(())
                    })??;
                }
            },
            #[cfg(feature = "sqlite")]
            DataSourceInner::Sqlite(source) => {
                let want = columns.len();
                source.for_each(&query, &mut |row| handler(&row[..want]))?;
            }
            #[cfg(feature = "csv")]
            DataSourceInner::CSV(csv) => {
                let _ = table;
                if order_by.is_some_and(|o| !o.is_empty()) {
                    eprintln!(
                        "[dbcon] warning: order_by ignored for CSV source '{}': CSV does not support ordered reads",
                        csv.data.describe()
                    );
                }
                let mut rdr = csv.reader()?;
                let headers = rdr.headers()?.clone();
                let indices: Vec<usize> = columns
                    .iter()
                    .map(|col| {
                        headers
                            .iter()
                            .position(|h| h == *col)
                            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in CSV", col))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut buffer = vec![NormalizedValue::Null; indices.len()];
                for record in rdr.into_records() {
                    let record = record?;
                    for (slot, &i) in buffer.iter_mut().zip(&indices) {
                        *slot = record
                            .get(i)
                            .map(|v| NormalizedValue::Text(v.to_string()))
                            .unwrap_or_default();
                    }
                    if handler(&buffer).is_break() {
                        break;
                    }
                }
            }
            #[cfg(feature = "parquet")]
            DataSourceInner::Parquet(p) => {
                let _ = table;
                if order_by.is_some_and(|o| !o.is_empty()) {
                    eprintln!(
                        "[dbcon] warning: order_by ignored for Parquet source '{}': Parquet does not support ordered reads",
                        p.data.describe()
                    );
                }
                let reader = p.open()?;
                let schema = reader.metadata().file_metadata().schema();
                let field_names: Vec<&str> = schema.get_fields().iter().map(|f| f.name()).collect();
                let ts_units: Vec<Option<TimeUnit>> = schema
                    .get_fields()
                    .iter()
                    .map(|f| parquet_timestamp_unit(f))
                    .collect();
                let indices: Vec<usize> = columns
                    .iter()
                    .map(|col| {
                        field_names
                            .iter()
                            .position(|h| h == col)
                            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in Parquet", col))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut buffer = vec![NormalizedValue::Null; indices.len()];
                for row in reader.get_row_iter(None)? {
                    let row = row?;
                    let fields: Vec<&Field> = row.get_column_iter().map(|(_, f)| f).collect();
                    for (slot, &i) in buffer.iter_mut().zip(&indices) {
                        *slot = fields
                            .get(i)
                            .map(|f| parquet_field_to_value_hinted(f, ts_units[i]))
                            .unwrap_or_default();
                    }
                    if handler(&buffer).is_break() {
                        break;
                    }
                }
            }
            #[cfg(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            ))]
            DataSourceInner::TableSet(set) => {
                let m = set.member(table)?;
                m.inner.scan(&m.inner_table, columns, order_by, handler)?;
            }
        }
        Ok(())
    }

    /// Stream rows for an arbitrary SQL query. SQL backends only; CSV and Parquet
    /// return an error. See [`DataSource::for_each_row_sql`] for the public wrapper.
    pub fn for_each_row_sql(
        &self,
        sql: &str,
        handler: &mut dyn FnMut(Vec<(String, NormalizedValue)>),
    ) -> anyhow::Result<()> {
        match self {
            // With no backend feature enabled `DataSourceInner` is uninhabited, so this
            // is the only arm and it is unreachable. With any feature on it is cfg'd out.
            #[cfg(not(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            )))]
            _ => match *self {},
            #[cfg(feature = "sql")]
            DataSourceInner::SQL(sql_pool) => {
                match sql_pool {
                    #[cfg(feature = "postgres")]
                    SQLPool::Postgres(pool) => {
                        use futures::StreamExt;
                        block_on(async {
                            let mut stream = sqlx::query(sql).fetch(pool);
                            while let Some(result) = stream.next().await {
                                let row = result?;
                                let named: Vec<(String, NormalizedValue)> = row
                                    .columns()
                                    .iter()
                                    .map(|col| {
                                        (
                                            col.name().to_string(),
                                            extract_row_column_value(&row, col).unwrap_or_default(),
                                        )
                                    })
                                    .collect();
                                handler(named);
                            }
                            Ok::<_, anyhow::Error>(())
                        })??;
                    }
                }
                Ok(())
            }
            #[cfg(feature = "sqlite")]
            DataSourceInner::Sqlite(source) => source.for_each_named(sql, handler),
            #[cfg(feature = "csv")]
            DataSourceInner::CSV(_) => {
                let _ = (sql, &mut *handler);
                anyhow::bail!("for_each_row_sql is not supported for CSV data sources")
            }
            #[cfg(feature = "parquet")]
            DataSourceInner::Parquet(_) => {
                let _ = (sql, &mut *handler);
                anyhow::bail!("for_each_row_sql is not supported for Parquet data sources")
            }
            #[cfg(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            ))]
            DataSourceInner::TableSet(_) => {
                let _ = (sql, &mut *handler);
                anyhow::bail!("for_each_row_sql is not supported for file-backed data sources")
            }
        }
    }

    /// Get distinct values of a single column
    pub fn get_distinct_values(&self, table: &str, column: &str) -> anyhow::Result<Vec<String>> {
        match self {
            // With no backend feature enabled `DataSourceInner` is uninhabited, so this
            // is the only arm and it is unreachable. With any feature on it is cfg'd out.
            #[cfg(not(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            )))]
            _ => match *self {},
            #[cfg(feature = "sql")]
            DataSourceInner::SQL(sql) => {
                let query = format!("SELECT DISTINCT \"{}\" FROM \"{}\"", column, table);
                match sql {
                    #[cfg(feature = "postgres")]
                    SQLPool::Postgres(pool) => {
                        let rows = block_on(sqlx::query(&query).fetch_all(pool))??;
                        Ok(rows
                            .iter()
                            .filter_map(|row| row.try_get::<Option<String>, _>(0).ok().flatten())
                            .collect())
                    }
                }
            }
            #[cfg(feature = "sqlite")]
            DataSourceInner::Sqlite(source) => source.text_column(&format!(
                "SELECT DISTINCT \"{}\" FROM \"{}\"",
                column, table
            )),
            #[cfg(feature = "csv")]
            DataSourceInner::CSV(csv) => {
                let _ = table;
                let mut rdr = csv.reader()?;
                let headers = rdr.headers()?.clone();
                let idx = headers
                    .iter()
                    .position(|h| h == column)
                    .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in CSV", column))?;
                let mut values = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for record in rdr.into_records() {
                    let record = record?;
                    if let Some(v) = record.get(idx)
                        && !v.is_empty()
                        && seen.insert(v.to_string())
                    {
                        values.push(v.to_string());
                    }
                }
                Ok(values)
            }
            #[cfg(feature = "parquet")]
            DataSourceInner::Parquet(p) => {
                let reader = p.open()?;
                let schema = reader.metadata().file_metadata().schema();
                let idx = schema
                    .get_fields()
                    .iter()
                    .position(|f| f.name() == column)
                    .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in Parquet", column))?;
                let unit = parquet_timestamp_unit(&schema.get_fields()[idx]);
                let mut values = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for row in reader.get_row_iter(None)? {
                    let row = row?;
                    if let Some((_, field)) = row.get_column_iter().nth(idx) {
                        if matches!(field, Field::Null) {
                            continue;
                        }
                        let s = parquet_field_to_value_hinted(field, unit).to_string();
                        if !s.is_empty() && seen.insert(s.clone()) {
                            values.push(s);
                        }
                    }
                }
                let _ = table;
                Ok(values)
            }
            #[cfg(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            ))]
            DataSourceInner::TableSet(set) => {
                let m = set.member(table)?;
                m.inner.get_distinct_values(&m.inner_table, column)
            }
        }
    }

    pub fn get_first_rows(
        &self,
        table: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        #[cfg(any(feature = "sql", feature = "sqlite"))]
        let select_all = match limit {
            Some(n) => format!("SELECT * FROM \"{}\" LIMIT {}", table, n),
            None => format!("SELECT * FROM \"{}\"", table),
        };
        match self {
            // With no backend feature enabled `DataSourceInner` is uninhabited, so this
            // is the only arm and it is unreachable. With any feature on it is cfg'd out.
            #[cfg(not(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            )))]
            _ => match *self {},
            #[cfg(feature = "sql")]
            DataSourceInner::SQL(sql) => match sql {
                #[cfg(feature = "postgres")]
                SQLPool::Postgres(pg_pool) => {
                    let rows = block_on(sqlx::query(&select_all).fetch_all(pg_pool))??;
                    Ok(rows.into_iter().map(row_to_named_strings).collect())
                }
            },
            #[cfg(feature = "sqlite")]
            DataSourceInner::Sqlite(source) => source.named_string_rows(&select_all, limit),
            #[cfg(feature = "csv")]
            DataSourceInner::CSV(csv) => {
                let _ = table;
                let mut rdr = csv.reader()?;

                let headers = rdr.headers()?.clone();
                let mut result = Vec::new();
                for record in rdr.records() {
                    let record = record?;
                    let row = headers
                        .iter()
                        .zip(record.iter())
                        .map(|(h, v)| (h.to_string(), v.to_string()))
                        .collect();
                    result.push(row);
                    if let Some(limit) = limit
                        && result.len() >= limit
                    {
                        break;
                    }
                }
                Ok(result)
            }
            #[cfg(feature = "parquet")]
            DataSourceInner::Parquet(p) => {
                let reader = p.open()?;
                let schema = reader.metadata().file_metadata().schema();
                let ts_units: HashMap<String, Option<TimeUnit>> = schema
                    .get_fields()
                    .iter()
                    .map(|f| (f.name().to_string(), parquet_timestamp_unit(f)))
                    .collect();
                let mut result: Vec<HashMap<String, String>> = Vec::new();
                for row in reader.get_row_iter(None)? {
                    let row = row?;
                    let named = row
                        .get_column_iter()
                        .map(|(name, field)| {
                            let unit = ts_units.get(name).copied().flatten();
                            (
                                name.clone(),
                                parquet_field_to_value_hinted(field, unit).to_string(),
                            )
                        })
                        .collect();
                    result.push(named);
                    if let Some(limit) = limit
                        && result.len() >= limit
                    {
                        break;
                    }
                }
                let _ = table;
                Ok(result)
            }
            #[cfg(any(
                feature = "sql",
                feature = "sqlite",
                feature = "csv",
                feature = "parquet"
            ))]
            DataSourceInner::TableSet(set) => {
                let m = set.member(table)?;
                m.inner.get_first_rows(&m.inner_table, limit)
            }
        }
    }
}

#[cfg(feature = "csv")]
/// A CSV file treated as a single-table data source.
#[derive(Debug)]
pub struct CSVSource {
    pub data: SourceData,
    pub delimiter: u8,
    pub has_headers: bool,
}

/// The table name used for a [`CSVSource`]. A CSV file is exposed as a single table
/// under this name, since CSVs don't carry multi-table schema information.
#[cfg(feature = "csv")]
pub const CSV_TABLE_NAME: &str = "main";

/// Where a file-backed source's bytes live.
///
/// `Memory` exists because a path is not always available: a browser `File`, a `wasm32` build with
/// no filesystem, or bytes that arrived over a network all have contents but no name to open.
///
/// Owned and cheap to clone rather than a reader, because both file-backed sources re-read from
/// the start on every operation (schema, scan, distinct values, preview). A one-shot
/// `impl Read` cannot serve that; an `Arc<[u8]>` can, at one allocation.
#[cfg(any(feature = "csv", feature = "parquet"))]
#[derive(Debug, Clone)]
pub enum SourceData {
    /// A file on disk, opened afresh for each read.
    Path(String),
    /// Bytes held in memory, shared between reads.
    Memory(std::sync::Arc<[u8]>),
}

#[cfg(any(feature = "csv", feature = "parquet"))]
impl SourceData {
    /// The path, when there is one. `None` for in-memory bytes, which have no name to report.
    pub fn path(&self) -> Option<&str> {
        match self {
            SourceData::Path(p) => Some(p),
            SourceData::Memory(_) => None,
        }
    }

    /// A fresh reader over the contents.
    pub fn reader(&self) -> std::io::Result<Box<dyn std::io::Read + Send + '_>> {
        match self {
            SourceData::Path(p) => Ok(Box::new(std::io::BufReader::new(std::fs::File::open(p)?))),
            SourceData::Memory(bytes) => Ok(Box::new(std::io::Cursor::new(&bytes[..]))),
        }
    }

    /// How this source names itself in a message.
    pub fn describe(&self) -> String {
        match self {
            SourceData::Path(p) => p.clone(),
            SourceData::Memory(bytes) => format!("<{} bytes in memory>", bytes.len()),
        }
    }
}

impl From<String> for SourceData {
    fn from(path: String) -> Self {
        SourceData::Path(path)
    }
}

/// Candidate CSV delimiters tried by [`detect_csv_delimiter`], in order of preference.
#[cfg(feature = "csv")]
const CSV_DELIMITER_CANDIDATES: &[u8] = b",;\t|";

/// Heuristically detect the delimiter of a CSV file by parsing its first few rows
/// with each candidate delimiter and picking the one that yields the most columns
/// with a consistent column count across rows. Returns `None` if the file cannot
/// be opened or no candidate produces more than one column.
#[cfg(feature = "csv")]
pub fn detect_csv_delimiter(path: &str) -> Option<u8> {
    detect_csv_delimiter_in(&SourceData::Path(path.to_string()))
}

/// [`detect_csv_delimiter`], for contents that may not be on disk.
#[cfg(feature = "csv")]
pub fn detect_csv_delimiter_in(data: &SourceData) -> Option<u8> {
    const SAMPLE_ROWS: usize = 8;
    let mut best: Option<(u8, usize)> = None;
    for &delim in CSV_DELIMITER_CANDIDATES {
        let Ok(reader) = data.reader() else { continue };
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(delim)
            .has_headers(false)
            .flexible(true)
            .from_reader(reader);
        let counts: Vec<usize> = rdr
            .records()
            .take(SAMPLE_ROWS)
            .filter_map(|r| r.ok().map(|r| r.len()))
            .collect();
        if counts.is_empty() {
            continue;
        }
        let cols = counts[0];
        if cols < 2 {
            continue;
        }
        let consistent = counts.iter().all(|&c| c == cols);
        // Prefer delimiters that split every sampled row into the same number of
        // columns. Score multiplies by 10 when consistent so a 3-column consistent
        // split wins over a 5-column inconsistent one.
        let score = if consistent { cols * 10 } else { cols };
        if best.is_none_or(|(_, s)| score > s) {
            best = Some((delim, score));
        }
    }
    best.map(|(d, _)| d)
}

#[cfg(feature = "csv")]
impl CSVSource {
    /// Build a [`CSVSource`] for `path`, auto-detecting the delimiter.
    /// Falls back to `,` if detection fails.
    /// A reader over this CSV, configured with its delimiter and header setting.
    ///
    /// Every read builds a fresh one: `csv::Reader` consumes its input, and a source is read
    /// several times over its life (schema, scan, distinct values, preview).
    pub fn reader(&self) -> anyhow::Result<csv::Reader<Box<dyn std::io::Read + Send + '_>>> {
        Ok(csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_headers)
            .flexible(true)
            .from_reader(self.data.reader()?))
    }

    /// A CSV held in memory, with its delimiter detected from the contents.
    pub fn from_bytes_autodetect(bytes: impl Into<std::sync::Arc<[u8]>>) -> Self {
        let data = SourceData::Memory(bytes.into());
        let delimiter = detect_csv_delimiter_in(&data).unwrap_or(b',');
        Self {
            data,
            delimiter,
            has_headers: true,
        }
    }

    /// A CSV held in memory, with an explicit dialect.
    pub fn from_bytes(
        bytes: impl Into<std::sync::Arc<[u8]>>,
        delimiter: u8,
        has_headers: bool,
    ) -> Self {
        Self {
            data: SourceData::Memory(bytes.into()),
            delimiter,
            has_headers,
        }
    }

    pub fn from_path_autodetect(path: String) -> Self {
        let data = SourceData::Path(path);
        let delimiter = detect_csv_delimiter_in(&data).unwrap_or(b',');
        Self {
            data,
            delimiter,
            has_headers: true,
        }
    }

    /// Parse the part of a `csv://` connection string after the scheme.
    ///
    /// Accepts an optional `?delimiter=<spec>` query string to override
    /// delimiter detection. `<spec>` may be a single character, `tab`, `\t`,
    /// `pipe`, `semicolon`, or `comma`. When no override is given, the
    /// delimiter is auto-detected via [`detect_csv_delimiter`].
    pub fn from_csv_spec(rest: &str) -> Self {
        let (path, query) = match rest.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (rest, None),
        };
        let override_delim = query.and_then(parse_csv_query_delimiter);
        let delimiter = override_delim
            .or_else(|| detect_csv_delimiter(path))
            .unwrap_or(b',');
        Self {
            data: SourceData::Path(path.to_string()),
            delimiter,
            has_headers: true,
        }
    }
}

#[cfg(feature = "csv")]
fn parse_csv_query_delimiter(query: &str) -> Option<u8> {
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=')?;
        if key == "delimiter" {
            let decoded = percent_decode(value);
            return parse_delimiter_spec(&decoded);
        }
    }
    None
}

/// Decode `%XX` hex escapes in a URL-encoded string. Returns the original string
/// on any malformed escape. We only need the minimal subset for delimiter values.
#[cfg(feature = "csv")]
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| s.to_string())
}

/// Parse a human-friendly delimiter spec (`,`, `;`, `tab`, `\t`, `pipe`, etc.)
/// into a single byte. Returns `None` for multi-byte or unrecognised values.
#[cfg(feature = "csv")]
pub fn parse_delimiter_spec(spec: &str) -> Option<u8> {
    match spec {
        "" => None,
        "\\t" | "tab" | "TAB" => Some(b'\t'),
        "pipe" | "PIPE" => Some(b'|'),
        "semicolon" | "SEMICOLON" => Some(b';'),
        "comma" | "COMMA" => Some(b','),
        s if s.len() == 1 => Some(s.as_bytes()[0]),
        _ => None,
    }
}

#[cfg(feature = "csv")]
impl CSVSource {
    /// Read the CSV header and build a single-table schema under [`CSV_TABLE_NAME`].
    /// All columns are reported as nullable text since CSV carries no type information.
    pub async fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        let mut rdr = self.reader()?;

        let headers = rdr.headers()?.clone();
        let columns = headers
            .iter()
            .map(|h| {
                (
                    h.to_string(),
                    DataColumnInfo {
                        name: h.to_string(),
                        col_type: NormalizedType::Text,
                        is_nullable: true,
                    },
                )
            })
            .collect();

        let mut tables = HashMap::new();
        tables.insert(
            CSV_TABLE_NAME.to_string(),
            DataTableInfo {
                name: CSV_TABLE_NAME.to_string(),
                columns,
                primary_keys: vec![],
                foreign_keys: vec![],
            },
        );

        Ok(tables)
    }
}

/// A Parquet file treated as a single-table data source.
///
/// The schema is read from the file's footer, so there are no delimiter or header
/// options as with [`CSVSource`].
#[cfg(feature = "parquet")]
#[derive(Debug)]
pub struct ParquetSource {
    pub data: SourceData,
}

/// Table name under which a [`ParquetSource`] exposes its single table.
#[cfg(feature = "parquet")]
pub const PARQUET_TABLE_NAME: &str = "main";

#[cfg(feature = "parquet")]
impl ParquetSource {
    /// A Parquet file on disk.
    pub fn from_path(path: impl Into<String>) -> Self {
        Self {
            data: SourceData::Path(path.into()),
        }
    }

    /// A Parquet file held in memory.
    pub fn from_bytes(bytes: impl Into<std::sync::Arc<[u8]>>) -> Self {
        Self {
            data: SourceData::Memory(bytes.into()),
        }
    }

    /// A reader over the file's contents, from disk or from memory.
    ///
    /// Boxed because the two are different types: `SerializedFileReader` is generic over its
    /// `ChunkReader`, and `parquet` itself passes `Box<dyn FileReader>` around, so this costs
    /// nothing the crate was not already paying.
    fn open(&self) -> anyhow::Result<Box<dyn FileReader>> {
        match &self.data {
            SourceData::Path(path) => Ok(Box::new(SerializedFileReader::new(File::open(path)?)?)),
            // `bytes::Bytes` implements `ChunkReader`, so in-memory parquet needs no temp file.
            // `bytes::Bytes` is what `parquet` implements `ChunkReader` for, so in-memory
            // parquet needs no temporary file.
            SourceData::Memory(bytes) => Ok(Box::new(SerializedFileReader::new(
                bytes::Bytes::copy_from_slice(bytes),
            )?)),
        }
    }

    /// Build a single-table entry under [`PARQUET_TABLE_NAME`], one column per top-level
    /// field with its `NormalizedType` inferred via [`parquet_field_type`].
    pub async fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        let reader = self.open()?;
        let schema = reader.metadata().file_metadata().schema();
        let columns = schema
            .get_fields()
            .iter()
            .map(|f| {
                let name = f.name().to_string();
                let col_type = parquet_field_type(f);
                let is_nullable = matches!(
                    f.get_basic_info().repetition(),
                    parquet::basic::Repetition::OPTIONAL
                );
                (
                    name.clone(),
                    DataColumnInfo {
                        name,
                        col_type,
                        is_nullable,
                    },
                )
            })
            .collect();

        let mut tables = HashMap::new();
        tables.insert(
            PARQUET_TABLE_NAME.to_string(),
            DataTableInfo {
                name: PARQUET_TABLE_NAME.to_string(),
                columns,
                primary_keys: vec![],
                foreign_keys: vec![],
            },
        );
        Ok(tables)
    }
}

/// Map a top-level Parquet schema field to a [`NormalizedType`].
/// Primitive leaves combine physical + logical/converted type; anything else
/// (nested Group/List/Map) falls through to `Unknown`.
#[cfg(feature = "parquet")]
fn parquet_field_type(t: &ParquetType) -> NormalizedType {
    let basic = t.get_basic_info();
    // Logical types take precedence over the older converted types.
    if let Some(lt) = basic.logical_type_ref() {
        match lt {
            LogicalType::String | LogicalType::Enum | LogicalType::Uuid => {
                return NormalizedType::Text;
            }
            LogicalType::Json | LogicalType::Bson => return NormalizedType::Json,
            LogicalType::Date | LogicalType::Timestamp { .. } | LogicalType::Time { .. } => {
                return NormalizedType::Timestamp;
            }
            LogicalType::Integer { .. } => return NormalizedType::Integer,
            LogicalType::Decimal { .. } => return NormalizedType::Float,
            LogicalType::Float16 => return NormalizedType::Float,
            _ => {}
        }
    }
    match basic.converted_type() {
        ConvertedType::UTF8 | ConvertedType::ENUM => return NormalizedType::Text,
        ConvertedType::JSON | ConvertedType::BSON => return NormalizedType::Json,
        ConvertedType::DATE
        | ConvertedType::TIMESTAMP_MILLIS
        | ConvertedType::TIMESTAMP_MICROS
        | ConvertedType::TIME_MILLIS
        | ConvertedType::TIME_MICROS => return NormalizedType::Timestamp,
        ConvertedType::INT_8
        | ConvertedType::INT_16
        | ConvertedType::INT_32
        | ConvertedType::INT_64
        | ConvertedType::UINT_8
        | ConvertedType::UINT_16
        | ConvertedType::UINT_32
        | ConvertedType::UINT_64 => return NormalizedType::Integer,
        ConvertedType::DECIMAL => return NormalizedType::Float,
        _ => {}
    }
    if let ParquetType::PrimitiveType { physical_type, .. } = t {
        return match physical_type {
            PhysicalType::BOOLEAN => NormalizedType::Boolean,
            PhysicalType::INT32 | PhysicalType::INT64 => NormalizedType::Integer,
            PhysicalType::INT96 => NormalizedType::Timestamp,
            PhysicalType::FLOAT | PhysicalType::DOUBLE => NormalizedType::Float,
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                NormalizedType::Unknown("bytes".to_string())
            }
        };
    }
    NormalizedType::Unknown(format!("{:?}", t))
}

/// Return the [`TimeUnit`] of a top-level Parquet field if it is a timestamp column,
/// covering both `LogicalType::Timestamp` and the legacy
/// `ConvertedType::TIMESTAMP_{MILLIS,MICROS}`.
#[cfg(feature = "parquet")]
fn parquet_timestamp_unit(t: &ParquetType) -> Option<TimeUnit> {
    let basic = t.get_basic_info();
    if let Some(LogicalType::Timestamp { unit, .. }) = basic.logical_type_ref() {
        return Some(*unit);
    }
    match basic.converted_type() {
        ConvertedType::TIMESTAMP_MILLIS => Some(TimeUnit::MILLIS),
        ConvertedType::TIMESTAMP_MICROS => Some(TimeUnit::MICROS),
        _ => None,
    }
}

/// Convert an integer `n` interpreted in the given `unit` since the Unix epoch into
/// a `NormalizedValue::Timestamp`, falling back to `Integer(n)` if the value is
/// out of `chrono`'s representable range.
#[cfg(feature = "parquet")]
fn integer_as_timestamp(n: i64, unit: TimeUnit) -> NormalizedValue {
    let dt = match unit {
        TimeUnit::MILLIS => chrono::DateTime::from_timestamp_millis(n),
        TimeUnit::MICROS => chrono::DateTime::from_timestamp_micros(n),
        TimeUnit::NANOS => {
            let secs = n.div_euclid(1_000_000_000);
            let nanos = n.rem_euclid(1_000_000_000) as u32;
            chrono::DateTime::from_timestamp(secs, nanos)
        }
    };
    match dt {
        Some(dt) => NormalizedValue::Timestamp(dt.fixed_offset()),
        None => NormalizedValue::Integer(n),
    }
}

/// Convert a Parquet `Field` into a `NormalizedValue`, using an optional timestamp
/// `unit` hint from the column schema. The hint re-tags integer fields as timestamps,
/// needed for `TIMESTAMP(NANOS)` columns: parquet-rs surfaces these as plain
/// `Field::Long` because its `Field` enum has no nanosecond timestamp variant.
#[cfg(feature = "parquet")]
fn parquet_field_to_value_hinted(field: &Field, unit: Option<TimeUnit>) -> NormalizedValue {
    if let Some(unit) = unit {
        match field {
            Field::Byte(n) => return integer_as_timestamp(*n as i64, unit),
            Field::Short(n) => return integer_as_timestamp(*n as i64, unit),
            Field::Int(n) => return integer_as_timestamp(*n as i64, unit),
            Field::Long(n) => return integer_as_timestamp(*n, unit),
            Field::UByte(n) => return integer_as_timestamp(*n as i64, unit),
            Field::UShort(n) => return integer_as_timestamp(*n as i64, unit),
            Field::UInt(n) => return integer_as_timestamp(*n as i64, unit),
            Field::ULong(n) if *n <= i64::MAX as u64 => {
                return integer_as_timestamp(*n as i64, unit);
            }
            _ => {}
        }
    }
    parquet_field_to_value(field)
}

/// Convert a Parquet `Field` into a `NormalizedValue`.
///
/// Numeric widenings are lossless except for `ULong` values above `i64::MAX`,
/// which are stringified into `Unknown` rather than silently wrapping. Nested
/// `Group`/`List`/`Map` become `Unknown(Debug-repr)`.
#[cfg(feature = "parquet")]
fn parquet_field_to_value(field: &Field) -> NormalizedValue {
    match field {
        Field::Null => NormalizedValue::Null,
        Field::Bool(b) => NormalizedValue::Boolean(*b),
        Field::Byte(n) => NormalizedValue::Integer(*n as i64),
        Field::Short(n) => NormalizedValue::Integer(*n as i64),
        Field::Int(n) => NormalizedValue::Integer(*n as i64),
        Field::Long(n) => NormalizedValue::Integer(*n),
        Field::UByte(n) => NormalizedValue::Integer(*n as i64),
        Field::UShort(n) => NormalizedValue::Integer(*n as i64),
        Field::UInt(n) => NormalizedValue::Integer(*n as i64),
        Field::ULong(n) => {
            if *n <= i64::MAX as u64 {
                NormalizedValue::Integer(*n as i64)
            } else {
                NormalizedValue::Unknown(n.to_string())
            }
        }
        Field::Float(f) => NormalizedValue::Float(*f as f64),
        Field::Float16(f) => NormalizedValue::Float(f64::from(*f)),
        Field::Double(f) => NormalizedValue::Float(*f),
        Field::Str(s) => NormalizedValue::Text(s.clone()),
        Field::Bytes(b) => NormalizedValue::Unknown(format!("{:?}", b.data())),
        Field::Date(days) => {
            // Parquet DATE: days since 1970-01-01 UTC.
            match chrono::DateTime::from_timestamp((*days as i64) * 86_400, 0) {
                Some(dt) => NormalizedValue::Timestamp(dt.fixed_offset()),
                None => NormalizedValue::Unknown(format!("date({})", days)),
            }
        }
        Field::TimestampMillis(ms) => match chrono::DateTime::from_timestamp_millis(*ms) {
            Some(dt) => NormalizedValue::Timestamp(dt.fixed_offset()),
            None => NormalizedValue::Unknown(format!("timestamp_ms({})", ms)),
        },
        Field::TimestampMicros(us) => match chrono::DateTime::from_timestamp_micros(*us) {
            Some(dt) => NormalizedValue::Timestamp(dt.fixed_offset()),
            None => NormalizedValue::Unknown(format!("timestamp_us({})", us)),
        },
        // TIME types have no associated date; stringify into Text since NormalizedValue
        // has no time-of-day variant.
        Field::TimeMillis(_) | Field::TimeMicros(_) => NormalizedValue::Text(format!("{}", field)),
        // Decimal has no Display at the struct level, but `Field`'s Display
        // writes it via `convert_decimal_to_string`, so reuse that.
        Field::Decimal(_) => {
            let s = format!("{}", field);
            match s.parse::<f64>() {
                Ok(f) => NormalizedValue::Float(f),
                Err(_) => NormalizedValue::Text(s),
            }
        }
        Field::Group(_) | Field::ListInternal(_) | Field::MapInternal(_) => {
            NormalizedValue::Unknown(format!("{:?}", field))
        }
    }
}

/// The sqlx-backed pools. SQLite is deliberately absent -- it goes through
/// [`SqliteSource`] and `rusqlite`; see [`mod@sqlite`].
#[cfg(feature = "sql")]
#[derive(Debug)]
pub enum SQLPool {
    #[cfg(feature = "postgres")]
    Postgres(PgPool),
}

#[cfg(feature = "postgres")]
impl From<PgPool> for SQLPool {
    fn from(pool: PgPool) -> Self {
        Self::Postgres(pool)
    }
}

#[cfg(feature = "sql")]
impl SQLPool {
    /// Discover the full schema of this connection.
    ///
    /// Delegates to the per-dialect catalog queries in [`crate::discovery`]; this crate
    /// owns those queries outright, so adding a backend does not depend on a third party
    /// having written a discoverer for it.
    pub async fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        match self {
            #[cfg(feature = "postgres")]
            SQLPool::Postgres(pool) => {
                discovery::postgres::discover(pool, discovery::postgres::DEFAULT_SCHEMA).await
            }
        }
    }
}

#[cfg(test)]
mod test {
    #[cfg(feature = "csv")]
    #[test]
    fn delimiter_spec_parses_known_aliases() {
        use crate::parse_delimiter_spec;
        assert_eq!(parse_delimiter_spec(";"), Some(b';'));
        assert_eq!(parse_delimiter_spec("\\t"), Some(b'\t'));
        assert_eq!(parse_delimiter_spec("tab"), Some(b'\t'));
        assert_eq!(parse_delimiter_spec("pipe"), Some(b'|'));
        assert_eq!(parse_delimiter_spec("semicolon"), Some(b';'));
        assert_eq!(parse_delimiter_spec(""), None);
        assert_eq!(parse_delimiter_spec("two_chars"), None);
    }

    #[cfg(feature = "csv")]
    #[test]
    fn csv_spec_url_encoded_delimiter_is_honoured() {
        // Frontend encodes `;` as `%3B` via encodeURIComponent; backend must decode.
        let src = crate::CSVSource::from_csv_spec("/nonexistent.csv?delimiter=%3B");
        assert_eq!(src.delimiter, b';');
        let src = crate::CSVSource::from_csv_spec("/nonexistent.csv?delimiter=tab");
        assert_eq!(src.delimiter, b'\t');
    }

    /// Roundtrip: write a tiny Parquet file with known schema + rows, then read
    /// it back through DataSource and check types/values survive the trip.
    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn parquet_roundtrip_reads_typed_values() -> anyhow::Result<()> {
        use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::parser::parse_message_type;
        use std::sync::Arc;

        let dir = std::env::temp_dir();
        let path = dir.join(format!("dbcon_roundtrip_{}.parquet", std::process::id()));
        let path_str = path.to_string_lossy().to_string();

        let schema = Arc::new(parse_message_type(
            "message schema {
                REQUIRED INT64 id;
                REQUIRED BYTE_ARRAY name (UTF8);
            }",
        )?);
        let props = Arc::new(WriterProperties::default());
        {
            let file = std::fs::File::create(&path)?;
            let mut writer = SerializedFileWriter::new(file, schema, props)?;
            let mut rg = writer.next_row_group()?;

            let mut c0 = rg.next_column()?.unwrap();
            c0.typed::<Int64Type>()
                .write_batch(&[1, 2, 3], None, None)?;
            c0.close()?;

            let mut c1 = rg.next_column()?.unwrap();
            let names: Vec<ByteArray> = ["alice", "bob", "carol"]
                .iter()
                .map(|s| ByteArray::from(*s))
                .collect();
            c1.typed::<ByteArrayType>()
                .write_batch(&names, None, None)?;
            c1.close()?;

            rg.close()?;
            writer.close()?;
        }

        let ds = crate::DataSource::new_parquet("rt".into(), path_str.clone()).await?;

        let table = ds.tables.get("main").expect("main table present");
        let id_col = table.columns.get("id").expect("id col");
        let name_col = table.columns.get("name").expect("name col");
        assert_eq!(id_col.col_type, crate::NormalizedType::Integer);
        assert_eq!(name_col.col_type, crate::NormalizedType::Text);

        let rows = ds.get_all_records("main", &["id", "name"], false)?;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], crate::NormalizedValue::Integer(1));
        assert_eq!(
            rows[2][1],
            crate::NormalizedValue::Text("carol".to_string())
        );

        let distinct = ds.get_distinct_values("main", "name")?;
        let mut sorted = distinct.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["alice", "bob", "carol"]);

        let _ = std::fs::remove_file(&path);
        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn parquet_dispatch_recognises_suffix_and_scheme() -> anyhow::Result<()> {
        use crate::{DataSource, DataSourceInner};
        // new_any_without_discovery only constructs the source, it does not open the file.
        let ds =
            DataSource::new_any_without_discovery("x".into(), "some/path.parquet".into()).await?;
        assert!(matches!(&ds.inner, DataSourceInner::Parquet(_)));

        let ds =
            DataSource::new_any_without_discovery("x".into(), "parquet:///tmp/y.parquet".into())
                .await?;
        assert!(matches!(&ds.inner, DataSourceInner::Parquet(_)));
        Ok(())
    }

    /// A connection string for a backend this build was not compiled with must fail with
    /// an error that names what the build *does* support, not a fixed list.
    #[tokio::test]
    async fn unsupported_connection_string_names_the_enabled_backends() {
        let err = crate::DataSource::new_any_without_discovery(
            "x".into(),
            "mongodb://localhost/x".into(),
        )
        .await
        .expect_err("mongodb is not a dbcon backend");
        let msg = err.to_string();
        assert!(msg.contains("mongodb://localhost/x"), "{msg}");
        assert!(
            msg.contains("Rebuild with the matching cargo feature"),
            "{msg}"
        );
    }
}
