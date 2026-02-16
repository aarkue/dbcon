use serde::{Deserialize, Serialize};
use sqlx::{ColumnIndex, Decode, Row};

pub mod manual;

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
    pub fn from_raw(raw_type: &str) -> Self {
        let raw_type = raw_type.to_lowercase();

        match raw_type.as_str() {
            // Timestamps
            t if t.contains("timestamp") || t.contains("datetime") => Self::Timestamp,

            // Integers
            t if t.contains("int") || t.contains("serial") => Self::Integer,

            // Text / Strings
            t if t.contains("char") || t.contains("text") || t.contains("clob") => Self::Text,

            // Floats / Decimals
            t if t.contains("float")
                || t.contains("double")
                || t.contains("numeric")
                || t.contains("decimal") =>
            {
                Self::Float
            }

            // Booleans
            t if t.contains("bool") || t.contains("boolean") => Self::Boolean,

            // JSON
            t if t.contains("json") => Self::Json,

            // Fallback
            _ => Self::Unknown(raw_type),
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

    pub async fn new_postgres(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(name, PgPool::connect(&connection_string).await?).await
    }

    pub async fn new_sqlite(name: String, connection_string: String) -> anyhow::Result<Self> {
        Self::new(name, SqlitePool::connect(&connection_string).await?).await
    }

    pub async fn new_csv(name: String, path: String) -> anyhow::Result<Self> {
        Self::new(
            name,
            CSVSource {
                path,
                delimiter: b',',
                has_headers: true,
            },
        )
        .await
    }

    pub async fn new_any(name: String, connection_string: String) -> anyhow::Result<Self> {
        if connection_string.starts_with("postgres://") {
            Self::new_postgres(name, connection_string).await
        } else if connection_string.starts_with("sqlite:") {
            Self::new_sqlite(name, connection_string).await
        } else if connection_string.starts_with("csv://") {
            let path = connection_string.trim_start_matches("csv://").to_string();
            Self::new_csv(name, path).await
        } else if connection_string.ends_with(".csv") {
            Self::new_csv(name, connection_string).await
        } else {
            anyhow::bail!("Unsupported data source type")
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

    pub fn get_all_tables<'a>(&'a self) -> impl Iterator<Item = &'a String> {
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataTableInfo {
    pub name: String,
    pub columns: HashMap<String, DataColumnInfo>,
    /// The names of the columns
    pub primary_keys: Vec<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PrimaryKey {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub from_columns: Vec<String>,
    pub to_table: String,
    pub to_columns: Vec<String>,
}

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
                NormalizedValue::Null
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
            DataSourceInner::SQL(sql) => match sql {
                SQLPool::Postgres(pg_pool) => {
                    let mut col_str = String::new();
                    for (i, col) in columns.iter().enumerate() {
                        if i > 0 {
                            col_str.push_str(", ");
                        }
                        col_str.push_str(&format!("\"{}\"", col));
                    }
                    let query = if let Some(limit) = limit {
                        format!(
                            "SELECT {} {} FROM \"{}\" LIMIT {}",
                            if unique { "DISTINCT " } else { "" },
                            col_str,
                            table,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} {} FROM \"{}\"",
                            if unique { "DISTINCT " } else { "" },
                            col_str,
                            table
                        )
                    };
                    let rows = sqlx::query(&query).fetch_all(pg_pool).await?;
                    Ok(rows
                        .into_iter()
                        .map(|row| {
                            row.columns()
                                .iter()
                                .map(|col| {
                                    let x = extract_row_column_value(&row, col);
                                    let r = x.unwrap_or_default();
                                    r
                                })
                                .collect()
                        })
                        .collect())
                }
                SQLPool::Sqlite(sqlite_pool) => {
                    let mut col_str = String::new();
                    for (i, col) in columns.iter().enumerate() {
                        if i > 0 {
                            col_str.push_str(", ");
                        }
                        col_str.push_str(&format!("\"{}\"", col));
                    }
                    let query = if let Some(limit) = limit {
                        format!(
                            "SELECT {} {} FROM \"{}\" LIMIT {}",
                            if unique { "DISTINCT " } else { "" },
                            col_str,
                            table,
                            limit
                        )
                    } else {
                        format!(
                            "SELECT {} {} FROM \"{}\"",
                            if unique { "DISTINCT " } else { "" },
                            col_str,
                            table
                        )
                    };
                    let rows = sqlx::query(&query).fetch_all(sqlite_pool).await?;

                    Ok(rows
                        .into_iter()
                        .map(|row| {
                            row.columns()
                                .iter()
                                .map(|col| {
                                    let x = extract_row_column_value(&row, col);
                                    let r = x.unwrap_or_default();
                                    r
                                })
                                .collect()
                        })
                        .collect())
                }
            },
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

    pub async fn get_first_rows(
        &self,
        table: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        match self {
            DataSourceInner::SQL(sql) => match sql {
                SQLPool::Postgres(pg_pool) => {
                    let query = if let Some(limit) = limit {
                        format!("SELECT * FROM \"{}\" LIMIT {}", table, limit)
                    } else {
                        format!("SELECT * FROM \"{}\"", table)
                    };
                    let rows = sqlx::query(&query).fetch_all(pg_pool).await?;
                    Ok(rows
                        .into_iter()
                        .map(|row| {
                            row.columns()
                                .iter()
                                .map(|col| {
                                    let x = extract_row_column_value(&row, col);
                                    let r = x.unwrap_or_default();
                                    (col.name().to_string(), r.to_string())
                                })
                                .collect()
                        })
                        .collect())
                }
                SQLPool::Sqlite(sqlite_pool) => {
                    let query = if let Some(limit) = limit {
                        format!("SELECT * FROM \"{}\" LIMIT {}", table, limit)
                    } else {
                        format!("SELECT * FROM \"{}\"", table)
                    };
                    let rows = sqlx::query(&query).fetch_all(sqlite_pool).await?;

                    Ok(rows
                        .into_iter()
                        .map(|row| {
                            row.columns()
                                .iter()
                                .map(|col| {
                                    let x = extract_row_column_value(&row, col);
                                    let r = x.unwrap_or_default();
                                    (col.name().to_string(), r.to_string())
                                })
                                .collect()
                        })
                        .collect())
                }
            },
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

#[derive(Debug)]
pub struct CSVSource {
    pub path: String,
    pub delimiter: u8,
    pub has_headers: bool,
}

impl CSVSource {
    pub async fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        // For simplicity, we treat the CSV file as a single table named "csv_table"
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
                        col_type: NormalizedType::Text, // Assume all CSV columns are text for simplicity
                        is_nullable: true,              // Assume all CSV columns are nullable
                    },
                )
            })
            .collect();

        let mut tables = HashMap::new();
        tables.insert(
            "main".to_string(),
            DataTableInfo {
                name: "main".to_string(),
                columns,
                primary_keys: vec![], // No primary keys in CSV
                foreign_keys: vec![], // No foreign keys in CSV
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
                                // println!("PRIMARY KEY: {} {:?} : {:?}",col.name,col.primary_key, table.constraints);
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

                        (
                            table.name.clone(),
                            DataTableInfo {
                                name: table.name,
                                columns,
                                // TODO: Double check if this is correct?
                                // Unique constraints vs. primary keys
                                primary_keys: table
                                    .constraints
                                    .into_iter()
                                    .filter(|x| x.unique)
                                    .map(|x| PrimaryKey {
                                        name: x.index_name,
                                        columns: x.columns,
                                    })
                                    .chain(vec![PrimaryKey {
                                        name: primary_key_columns.join("_") + "_pk",
                                        columns: primary_key_columns,
                                    }])
                                    .collect(), // SQLite discovery doesn't currently include PK info
                                foreign_keys: table
                                    .foreign_keys
                                    .into_iter()
                                    .map(|x| ForeignKey {
                                        name: x.id.to_string(),
                                        from_columns: x.from,
                                        to_table: x.table,
                                        to_columns: x.to,
                                    })
                                    .collect(), // SQLite discovery doesn't currently include FK info
                            },
                        )
                    })
                    .collect();

                Ok(tables)
            }
        }
    }
}

