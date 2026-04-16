//! # dbcon
//!
//! A small universal connector for tabular data sources.
//!
//! dbcon offers a uniform API over SQLite, PostgreSQL, and CSV files: schema discovery
//! (tables, columns, primary and foreign keys), row iteration, streaming, and distinct-
//! value queries. Values are exposed via a normalised [`NormalizedValue`] enum so callers
//! can handle data from any backend without knowing the source-specific type system.
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

use serde::{Deserialize, Serialize};
use sqlx::{ColumnIndex, Decode, Row};
pub mod manual;

/// A backend-agnostic classification of a column's data type.
///
/// Raw SQL / CSV types are mapped to one of these variants via
/// [`NormalizedType::from_raw`] or the conversions from `sea_schema` types.
/// `Unknown` preserves the original lowercase type string so callers can
/// make their own decisions.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum NormalizedType {
    Text,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Json,
    Unknown(String),
}

/// A backend-agnostic row value.
///
/// All supported sources decode their native types into one of these variants.
/// `Unknown` carries the string form of values that couldn't be decoded into a
/// typed variant, so callers can still inspect or re-parse them.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub enum NormalizedValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Timestamp(chrono::DateTime<chrono::FixedOffset>),
    Json(serde_json::Value),
    Unknown(String),
    #[default]
    Null,
}

impl NormalizedValue {
    /// Returns a string slice for Text/Unknown variants without cloning.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            NormalizedValue::Text(s) | NormalizedValue::Unknown(s) => Some(s),
            _ => None,
        }
    }

    /// Returns the timestamp directly if this is a Timestamp variant.
    #[inline]
    pub fn as_timestamp(&self) -> Option<&chrono::DateTime<chrono::FixedOffset>> {
        match self {
            NormalizedValue::Timestamp(t) => Some(t),
            _ => None,
        }
    }
}

impl Display for NormalizedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NormalizedValue::Text(s) => write!(f, "{}", s),
            NormalizedValue::Integer(i) => write!(f, "{}", i),
            NormalizedValue::Float(fl) => write!(f, "{}", fl),
            NormalizedValue::Boolean(b) => write!(f, "{}", b),
            NormalizedValue::Timestamp(t) => write!(f, "{}", t),
            NormalizedValue::Json(j) => write!(f, "{}", j),
            NormalizedValue::Unknown(u) => write!(f, "{}", u),
            NormalizedValue::Null => write!(f, "NULL"),
        }
    }
}

impl<T> From<Option<T>> for NormalizedValue
where
    T: Into<NormalizedValue>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Self::Null,
        }
    }
}

impl From<String> for NormalizedValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for NormalizedValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<i32> for NormalizedValue {
    fn from(value: i32) -> Self {
        Self::Integer(value.into())
    }
}

impl From<f64> for NormalizedValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<f32> for NormalizedValue {
    fn from(value: f32) -> Self {
        Self::Float(value.into())
    }
}

impl From<bool> for NormalizedValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<chrono::DateTime<chrono::FixedOffset>> for NormalizedValue {
    fn from(value: chrono::DateTime<chrono::FixedOffset>) -> Self {
        Self::Timestamp(value)
    }
}

impl From<&sea_schema::sea_query::ColumnType> for NormalizedType {
    fn from(col_type: &sea_schema::sea_query::ColumnType) -> Self {
        match col_type {
            sea_schema::sea_query::ColumnType::Char(_) => Self::Text,
            sea_schema::sea_query::ColumnType::String(_) => Self::Text,
            sea_schema::sea_query::ColumnType::Text => Self::Text,
            sea_schema::sea_query::ColumnType::TinyInteger => Self::Integer,
            sea_schema::sea_query::ColumnType::SmallInteger => Self::Integer,
            sea_schema::sea_query::ColumnType::Integer => Self::Integer,
            sea_schema::sea_query::ColumnType::BigInteger => Self::Integer,
            sea_schema::sea_query::ColumnType::TinyUnsigned => Self::Integer,
            sea_schema::sea_query::ColumnType::SmallUnsigned => Self::Integer,
            sea_schema::sea_query::ColumnType::Unsigned => Self::Integer,
            sea_schema::sea_query::ColumnType::BigUnsigned => Self::Integer,
            sea_schema::sea_query::ColumnType::Float => Self::Float,
            sea_schema::sea_query::ColumnType::Double => Self::Float,
            sea_schema::sea_query::ColumnType::Decimal(_) => Self::Float,
            sea_schema::sea_query::ColumnType::DateTime => Self::Timestamp,
            sea_schema::sea_query::ColumnType::Timestamp => Self::Timestamp,
            sea_schema::sea_query::ColumnType::TimestampWithTimeZone => Self::Timestamp,
            sea_schema::sea_query::ColumnType::Uuid => Self::Text,
            sea_schema::sea_query::ColumnType::Boolean => Self::Boolean,
            sea_schema::sea_query::ColumnType::Json => Self::Json,
            x => Self::Unknown(format!("{:?}", x)),
        }
    }
}

impl From<&sea_schema::postgres::def::Type> for NormalizedType {
    fn from(col_type: &sea_schema::postgres::def::Type) -> Self {
        match col_type {
            sea_schema::postgres::def::Type::Char(_) => Self::Text,
            sea_schema::postgres::def::Type::Varchar(_) => Self::Text,
            sea_schema::postgres::def::Type::Text => Self::Text,
            sea_schema::postgres::def::Type::SmallInt => Self::Integer,
            sea_schema::postgres::def::Type::Integer => Self::Integer,
            sea_schema::postgres::def::Type::BigInt => Self::Integer,
            sea_schema::postgres::def::Type::DoublePrecision => Self::Float,
            sea_schema::postgres::def::Type::Decimal(_) => Self::Float,
            sea_schema::postgres::def::Type::Date => Self::Timestamp,
            sea_schema::postgres::def::Type::Timestamp(_) => Self::Timestamp,
            sea_schema::postgres::def::Type::TimeWithTimeZone(_) => Self::Timestamp,
            sea_schema::postgres::def::Type::TimestampWithTimeZone(_) => Self::Timestamp,
            sea_schema::postgres::def::Type::Uuid => Self::Text,
            sea_schema::postgres::def::Type::Boolean => Self::Boolean,
            sea_schema::postgres::def::Type::Json => Self::Json,
            x => Self::Unknown(format!("{:?}", x)),
        }
    }
}
impl NormalizedType {
    /// Infer a [`NormalizedType`] from a raw SQL type name string (case-insensitive).
    ///
    /// Recognised names include the common SQL/standard forms (`INTEGER`, `VARCHAR(n)`,
    /// `TIMESTAMP WITH TIME ZONE`, etc.) and a few DB-specific synonyms (`SERIAL`, `CLOB`,
    /// `NUMERIC`). Unrecognised names yield [`NormalizedType::Unknown`] with the lowercase
    /// original preserved, allowing callers to decide how to handle them.
    pub fn from_raw(raw_type: &str) -> Self {
        let raw = raw_type.to_lowercase();
        // Strip size/precision suffixes like "(10,2)" so "varchar(255)" and "numeric(10,2)"
        // match the same as their bare names.
        let base = raw.split('(').next().unwrap_or(&raw).trim();

        match base {
            "timestamp"
            | "timestamptz"
            | "timestamp with time zone"
            | "timestamp without time zone"
            | "datetime"
            | "datetime2"
            | "date"
            | "time"
            | "time with time zone"
            | "time without time zone"
            | "timetz" => Self::Timestamp,

            "int"
            | "int2"
            | "int4"
            | "int8"
            | "integer"
            | "bigint"
            | "smallint"
            | "tinyint"
            | "mediumint"
            | "serial"
            | "bigserial"
            | "smallserial" => Self::Integer,

            "text"
            | "ntext"
            | "char"
            | "nchar"
            | "varchar"
            | "nvarchar"
            | "character"
            | "character varying"
            | "clob"
            | "uuid"
            | "string" => Self::Text,

            "real"
            | "float"
            | "float4"
            | "float8"
            | "double"
            | "double precision"
            | "numeric"
            | "decimal"
            | "money" => Self::Float,

            "bool" | "boolean" => Self::Boolean,

            "json" | "jsonb" => Self::Json,

            _ => {
                // Fallback: a few common variants carry a distinguishing prefix/suffix.
                // Order matters — check the most specific categories first.
                if base.starts_with("timestamp") || base.starts_with("datetime") {
                    Self::Timestamp
                } else if base.ends_with("char") || base.ends_with("text") {
                    Self::Text
                } else if base.ends_with("serial") || base.ends_with("int") {
                    Self::Integer
                } else if base.ends_with("float") || base.ends_with("double") {
                    Self::Float
                } else {
                    Self::Unknown(raw)
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use sqlx::{PgPool, SqlitePool};

    use crate::{CSVSource, DataSource};

    #[tokio::test]
    async fn test_postgres() -> anyhow::Result<()> {
        dotenvy::dotenv().ok();
        let url = std::env::var("POSTGRES_URL")
            .expect("POSTGRES_URL must be set (see .env.example)");
        let pool = PgPool::connect(&url).await?;
        let ds = DataSource::new("Postgres Test".to_string(), pool).await?;

        println!("Postgres:");
        println!("{:?}", ds.tables.values().next().unwrap());
        for row in ds
            .get_first_rows(ds.get_all_tables().next().unwrap(), 5)
            .await?
        {
            println!("\t{:?}", row);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sqlite() -> anyhow::Result<()> {
        dotenvy::dotenv().ok();
        let path = std::env::var("SQLITE_PATH")
            .expect("SQLITE_PATH must be set (see .env.example)");
        let pool = SqlitePool::connect(&path).await?;
        let ds = DataSource::new("SQLite Test".to_string(), pool).await?;

        println!("SQLite:");
        println!("{:?}", ds.tables.values().next().unwrap());
        for row in ds
            .get_first_rows(ds.get_all_tables().next().unwrap(), 5)
            .await?
        {
            println!("\t{:?}", row);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_csv() -> anyhow::Result<()> {
        dotenvy::dotenv().ok();
        let path =
            std::env::var("CSV_PATH").expect("CSV_PATH must be set (see .env.example)");
        let csv = CSVSource {
            path,
            delimiter: b',',
            has_headers: true,
        };
        let ds = DataSource::new("CSV Test".to_string(), csv).await?;

        println!("CSV:");
        println!("{:?}", ds.tables.values().next().unwrap());
        for row in ds
            .get_first_rows(ds.get_all_tables().next().unwrap(), 5)
            .await?
        {
            println!("\t{:?}", row);
        }
        Ok(())
    }
}

use sea_schema::postgres::discovery::SchemaDiscovery as PgDiscoverer;
use sea_schema::sqlite::discovery::SchemaDiscovery as SqliteDiscoverer;
use sqlx::types::chrono::{self, FixedOffset, NaiveDateTime};
use std::collections::HashMap;
use std::fmt::Display;
// use sea_schema::mysql::discoverer::SchemaDiscoverer as MySqlDiscoverer;
use sqlx::{Column, PgPool, SqlitePool, TypeInfo};

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

    pub async fn new_postgres(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(name, PgPool::connect(&connection_string).await?).await
    }

    pub async fn new_sqlite(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(name, SqlitePool::connect(&connection_string).await?).await
    }

    pub async fn new_csv(name: String, path: String) -> anyhow::Result<Self> {
        let delimiter = detect_csv_delimiter(&path).unwrap_or(b',');
        Self::new(
            name,
            CSVSource {
                path,
                delimiter,
                has_headers: true,
            },
        )
        .await
    }

    pub async fn new_any(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(name, Self::inner_from_connection_string(&connection_string).await?).await
    }

    /// Connect without schema discovery — only establishes the connection for querying.
    /// Much faster than `new_any` for databases with many tables.
    pub async fn new_any_without_discovery(
        name: String,
        connection_string: String,
    ) -> anyhow::Result<Self> {
        Self::new_without_discovery(name, Self::inner_from_connection_string(&connection_string).await?).await
    }

    async fn inner_from_connection_string(
        connection_string: &str,
    ) -> anyhow::Result<DataSourceInner> {
        if connection_string.starts_with("postgres://")
            || connection_string.starts_with("postgresql://")
        {
            Ok(PgPool::connect(connection_string).await?.into())
        } else if connection_string.starts_with("sqlite:") {
            Ok(SqlitePool::connect(connection_string).await?.into())
        } else if let Some(rest) = connection_string.strip_prefix("csv://") {
            Ok(CSVSource::from_csv_spec(rest).into())
        } else if connection_string.ends_with(".csv") {
            Ok(CSVSource::from_path_autodetect(connection_string.to_string()).into())
        } else {
            anyhow::bail!(
                "Unsupported data source. Expected a connection string starting with \
                 `postgres://`, `postgresql://`, `sqlite:`, or `csv://`, or a path ending \
                 in `.csv`."
            )
        }
    }

    pub async fn get_first_rows_of_all_tables(
        &self,
        n: usize,
    ) -> anyhow::Result<HashMap<String, Vec<HashMap<String, String>>>> {
        let mut result = HashMap::new();
        for table in self.get_all_tables() {
            let rows = self.get_first_rows(table, n).await?;
            result.insert(table.clone(), rows);
        }
        Ok(result)
    }

    pub fn get_all_tables(&self) -> impl Iterator<Item = &String> {
        self.tables.keys()
    }

    pub async fn get_all_records(
        &self,
        table: &str,
        columns: &[&str],
        unique: bool,
    ) -> anyhow::Result<Vec<Vec<NormalizedValue>>> {
        self.inner
            .get_first_records(table, columns, None, unique)
            .await
    }

    pub async fn get_all_rows(&self, table: &str) -> anyhow::Result<Vec<HashMap<String, String>>> {
        self.inner.get_first_rows(table, None).await
    }

    pub async fn get_first_rows(
        &self,
        table: &str,
        n: usize,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        self.inner.get_first_rows(table, Some(n)).await
    }

    /// Process rows one at a time by calling `handler` for each row.
    /// Rows are streamed from the DB and never fully materialized in memory.
    /// Optional `order_by` appends ORDER BY columns to the query.
    pub async fn for_each_record(
        &self,
        table: &str,
        columns: &[&str],
        order_by: Option<&[&str]>,
        handler: impl FnMut(Vec<NormalizedValue>),
    ) -> anyhow::Result<()> {
        self.inner
            .for_each_record(table, columns, order_by, handler)
            .await
    }

    /// Run SELECT DISTINCT on a single column. Useful for discovering attribute names.
    pub async fn get_distinct_values(
        &self,
        table: &str,
        column: &str,
    ) -> anyhow::Result<Vec<String>> {
        self.inner.get_distinct_values(table, column).await
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
    SQL(SQLPool),
    CSV(CSVSource),
}
impl<T> From<T> for DataSourceInner
where
    T: Into<SQLPool>,
{
    fn from(value: T) -> Self {
        let value: SQLPool = value.into();
        Self::SQL(value)
    }
}
impl From<CSVSource> for DataSourceInner {
    fn from(csv: CSVSource) -> Self {
        Self::CSV(csv)
    }
}

/// Build a SELECT query string from columns, table, and optional ORDER BY
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
    let distinct = if unique { "DISTINCT " } else { "" };
    let mut query = format!("SELECT {}{} FROM \"{}\"", distinct, col_str, table);
    if let Some(order_cols) = order_by {
        if !order_cols.is_empty() {
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
    }
    if let Some(limit) = limit {
        query.push_str(&format!(" LIMIT {}", limit));
    }
    query
}

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
        _ => row.try_get::<Option<String>, _>(i)?.into(),
    };
    Ok(res)
}

/// Map a sqlx row to a `Vec<NormalizedValue>`, one per column.
/// Columns that fail to decode yield [`NormalizedValue::Null`].
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
            DataSourceInner::SQL(sql) => sql.get_tables().await,
            DataSourceInner::CSV(csv) => csv.get_tables().await,
        }
    }

    pub async fn get_first_records(
        &self,
        table: &str,
        columns: &[&str],
        limit: Option<usize>,
        unique: bool,
    ) -> anyhow::Result<Vec<Vec<NormalizedValue>>> {
        match self {
            DataSourceInner::SQL(sql) => {
                let query = build_select_query(columns, table, None, limit, unique);
                match sql {
                    SQLPool::Postgres(pg_pool) => {
                        let rows = sqlx::query(&query).fetch_all(pg_pool).await?;
                        Ok(rows.into_iter().map(rows_to_values).collect())
                    }
                    SQLPool::Sqlite(sqlite_pool) => {
                        let rows = sqlx::query(&query).fetch_all(sqlite_pool).await?;
                        Ok(rows.into_iter().map(rows_to_values).collect())
                    }
                }
            }
            DataSourceInner::CSV(csv) => {
                let mut rdr = csv::ReaderBuilder::new()
                    .delimiter(csv.delimiter)
                    .has_headers(csv.has_headers)
                    .from_path(&csv.path)?;

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
                    if let Some(limit) = limit {
                        if result.len() >= limit {
                            break;
                        }
                    }
                }
                Ok(result)
            }
        }
    }

    /// Process rows one at a time by calling `handler` for each row.
    /// Rows are streamed from the DB and never fully materialized in memory.
    /// Optional `order_by` appends ORDER BY columns to the query.
    pub async fn for_each_record(
        &self,
        table: &str,
        columns: &[&str],
        order_by: Option<&[&str]>,
        mut handler: impl FnMut(Vec<NormalizedValue>),
    ) -> anyhow::Result<()> {
        use futures::StreamExt;
        let query = build_select_query(columns, table, order_by, None, false);
        match self {
            DataSourceInner::SQL(sql) => match sql {
                SQLPool::Postgres(pool) => {
                    let mut stream = sqlx::query(&query).fetch(pool);
                    while let Some(result) = stream.next().await {
                        let row = result?;
                        let values = row
                            .columns()
                            .iter()
                            .map(|col| extract_row_column_value(&row, col).unwrap_or_default())
                            .collect();
                        handler(values);
                    }
                }
                SQLPool::Sqlite(pool) => {
                    let mut stream = sqlx::query(&query).fetch(pool);
                    while let Some(result) = stream.next().await {
                        let row = result?;
                        let values = row
                            .columns()
                            .iter()
                            .map(|col| extract_row_column_value(&row, col).unwrap_or_default())
                            .collect();
                        handler(values);
                    }
                }
            },
            DataSourceInner::CSV(csv) => {
                let mut rdr = csv::ReaderBuilder::new()
                    .delimiter(csv.delimiter)
                    .has_headers(csv.has_headers)
                    .from_path(&csv.path)?;
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
                for record in rdr.into_records() {
                    let record = record?;
                    let values = indices
                        .iter()
                        .map(|&i| {
                            record
                                .get(i)
                                .map(|v| NormalizedValue::Text(v.to_string()))
                                .unwrap_or_default()
                        })
                        .collect();
                    handler(values);
                }
            }
        }
        Ok(())
    }

    /// Get distinct values of a single column
    pub async fn get_distinct_values(
        &self,
        table: &str,
        column: &str,
    ) -> anyhow::Result<Vec<String>> {
        match self {
            DataSourceInner::SQL(sql) => {
                let query = format!("SELECT DISTINCT \"{}\" FROM \"{}\"", column, table);
                match sql {
                    SQLPool::Postgres(pool) => {
                        let rows = sqlx::query(&query).fetch_all(pool).await?;
                        Ok(rows
                            .iter()
                            .filter_map(|row| row.try_get::<Option<String>, _>(0).ok().flatten())
                            .collect())
                    }
                    SQLPool::Sqlite(pool) => {
                        let rows = sqlx::query(&query).fetch_all(pool).await?;
                        Ok(rows
                            .iter()
                            .filter_map(|row| row.try_get::<Option<String>, _>(0).ok().flatten())
                            .collect())
                    }
                }
            }
            DataSourceInner::CSV(csv) => {
                let mut rdr = csv::ReaderBuilder::new()
                    .delimiter(csv.delimiter)
                    .has_headers(csv.has_headers)
                    .from_path(&csv.path)?;
                let headers = rdr.headers()?.clone();
                let idx = headers
                    .iter()
                    .position(|h| h == column)
                    .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in CSV", column))?;
                let mut values = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for record in rdr.into_records() {
                    let record = record?;
                    if let Some(v) = record.get(idx) {
                        if !v.is_empty() && seen.insert(v.to_string()) {
                            values.push(v.to_string());
                        }
                    }
                }
                Ok(values)
            }
        }
    }

    pub async fn get_first_rows(
        &self,
        table: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        match self {
            DataSourceInner::SQL(sql) => {
                let query = match limit {
                    Some(n) => format!("SELECT * FROM \"{}\" LIMIT {}", table, n),
                    None => format!("SELECT * FROM \"{}\"", table),
                };
                match sql {
                    SQLPool::Postgres(pg_pool) => {
                        let rows = sqlx::query(&query).fetch_all(pg_pool).await?;
                        Ok(rows.into_iter().map(row_to_named_strings).collect())
                    }
                    SQLPool::Sqlite(sqlite_pool) => {
                        let rows = sqlx::query(&query).fetch_all(sqlite_pool).await?;
                        Ok(rows.into_iter().map(row_to_named_strings).collect())
                    }
                }
            }
            DataSourceInner::CSV(csv) => {
                let mut rdr = csv::ReaderBuilder::new()
                    .delimiter(csv.delimiter)
                    .has_headers(csv.has_headers)
                    .from_path(&csv.path)?;

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
                    if let Some(limit) = limit {
                        if result.len() >= limit {
                            break;
                        }
                    }
                }
                Ok(result)
            }
        }
    }
}

/// A CSV file treated as a single-table data source.
#[derive(Debug)]
pub struct CSVSource {
    pub path: String,
    pub delimiter: u8,
    pub has_headers: bool,
}

/// The table name used for a [`CSVSource`]. A CSV file is exposed as a single table
/// under this name, since CSVs don't carry multi-table schema information.
pub const CSV_TABLE_NAME: &str = "main";

/// Candidate CSV delimiters tried by [`detect_csv_delimiter`], in order of preference.
const CSV_DELIMITER_CANDIDATES: &[u8] = b",;\t|";

/// Heuristically detect the delimiter of a CSV file by parsing its first few rows
/// with each candidate delimiter and picking the one that yields the most columns
/// with a consistent column count across rows. Returns `None` if the file cannot
/// be opened or no candidate produces more than one column.
pub fn detect_csv_delimiter(path: &str) -> Option<u8> {
    const SAMPLE_ROWS: usize = 8;
    let mut best: Option<(u8, usize)> = None;
    for &delim in CSV_DELIMITER_CANDIDATES {
        let Ok(mut rdr) = csv::ReaderBuilder::new()
            .delimiter(delim)
            .has_headers(false)
            .flexible(true)
            .from_path(path)
        else {
            continue;
        };
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

impl CSVSource {
    /// Build a [`CSVSource`] for `path`, auto-detecting the delimiter.
    /// Falls back to `,` if detection fails.
    pub fn from_path_autodetect(path: String) -> Self {
        let delimiter = detect_csv_delimiter(&path).unwrap_or(b',');
        Self {
            path,
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
        let (path, query) = match rest.find('?') {
            Some(i) => (&rest[..i], Some(&rest[i + 1..])),
            None => (rest, None),
        };
        let override_delim = query.and_then(parse_csv_query_delimiter);
        let delimiter = override_delim
            .or_else(|| detect_csv_delimiter(path))
            .unwrap_or(b',');
        Self {
            path: path.to_string(),
            delimiter,
            has_headers: true,
        }
    }
}

fn parse_csv_query_delimiter(query: &str) -> Option<u8> {
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next()?;
        let value = parts.next()?;
        if key == "delimiter" {
            return parse_delimiter_spec(value);
        }
    }
    None
}

/// Parse a human-friendly delimiter spec (`,`, `;`, `tab`, `\t`, `pipe`, etc.)
/// into a single byte. Returns `None` for multi-byte or unrecognised values.
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

impl CSVSource {
    /// Read the CSV header and build a single-table schema under [`CSV_TABLE_NAME`].
    /// All columns are reported as nullable text since CSV carries no type information.
    pub async fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.has_headers)
            .from_path(&self.path)?;

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

#[derive(Debug)]
pub enum SQLPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl From<PgPool> for SQLPool {
    fn from(pool: PgPool) -> Self {
        Self::Postgres(pool)
    }
}
impl From<SqlitePool> for SQLPool {
    fn from(pool: SqlitePool) -> Self {
        Self::Sqlite(pool)
    }
}

impl SQLPool {
    pub async fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        match self {
            SQLPool::Postgres(pg_pool) => {
                let discoverer = PgDiscoverer::new(pg_pool.clone(), "public");
                let schema = discoverer.discover().await?;

                let tables = schema
                    .tables
                    .into_iter()
                    .map(|table| {
                        let columns = table
                            .columns
                            .into_iter()
                            .map(|col| {
                                (
                                    col.name.clone(),
                                    DataColumnInfo {
                                        name: col.name,
                                        col_type: NormalizedType::from(&col.col_type),
                                        is_nullable: col.not_null.is_none(),
                                    },
                                )
                            })
                            .collect();

                        (
                            table.info.name.clone(),
                            DataTableInfo {
                                name: table.info.name,
                                columns,
                                primary_keys: table
                                    .primary_key_constraints
                                    .into_iter()
                                    .map(|c| PrimaryKey {
                                        name: c.name,
                                        columns: c.columns,
                                    })
                                    .collect(),
                                foreign_keys: table
                                    .reference_constraints
                                    .into_iter()
                                    .map(|c| ForeignKey {
                                        name: c.name,
                                        from_columns: c.columns,
                                        to_table: c.table,
                                        to_columns: c.foreign_columns,
                                    })
                                    .collect(),
                            },
                        )
                    })
                    .collect();

                Ok(tables)
            }
            SQLPool::Sqlite(sqlite_pool) => {
                let discoverer = SqliteDiscoverer::new(sqlite_pool.clone());
                let schema = discoverer.discover().await?;

                let tables = schema
                    .tables
                    .into_iter()
                    .map(|table| {
                        let mut primary_key_columns = Vec::new();
                        let columns = table
                            .columns
                            .into_iter()
                            .map(|col| {
                                if col.primary_key {
                                    primary_key_columns.push(col.name.clone());
                                }
                                (
                                    col.name.clone(),
                                    DataColumnInfo {
                                        name: col.name,
                                        col_type: NormalizedType::from(&col.r#type),
                                        is_nullable: !col.not_null,
                                    },
                                )
                            })
                            .collect();

                        let mut primary_keys: Vec<PrimaryKey> = table
                            .constraints
                            .into_iter()
                            .filter(|x| x.unique)
                            .map(|x| PrimaryKey {
                                name: x.index_name,
                                columns: x.columns,
                            })
                            .collect();
                        if !primary_key_columns.is_empty() {
                            primary_keys.push(PrimaryKey {
                                name: primary_key_columns.join("_") + "_pk",
                                columns: primary_key_columns,
                            });
                        }

                        (
                            table.name.clone(),
                            DataTableInfo {
                                name: table.name,
                                columns,
                                primary_keys,
                                foreign_keys: table
                                    .foreign_keys
                                    .into_iter()
                                    .map(|x| ForeignKey {
                                        name: x.id.to_string(),
                                        from_columns: x.from,
                                        to_table: x.table,
                                        to_columns: x.to,
                                    })
                                    .collect(),
                            },
                        )
                    })
                    .collect();

                Ok(tables)
            }
        }
    }
}

