//! Manual schema discovery POC using raw SQL queries (no sea-schema dependency).
//!
//! This module demonstrates how to discover database schema using `sqlx::AnyPool`
//! and plain SQL, as an alternative to the sea-schema-based approach in the main module.

use std::collections::HashMap;

use sqlx::{AnyPool, ConnectOptions, Row};

use crate::{DataColumnInfo, DataTableInfo, NormalizedType};

/// Discover all tables and their columns from a database using raw SQL.
///
/// Supports SQLite and PostgreSQL via `AnyPool` dialect detection.
pub async fn get_database_schema(
    pool: &AnyPool,
) -> Result<HashMap<String, DataTableInfo>, sqlx::Error> {
    let conn_str = pool.connect_options().to_url_lossy().to_string();
    let is_sqlite = conn_str.starts_with("sqlite");

    let table_query = if is_sqlite {
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    } else {
        "SELECT CAST(table_name AS VARCHAR) as name FROM information_schema.tables WHERE table_schema = 'public'"
    };

    let table_rows = sqlx::query(table_query).fetch_all(pool).await?;
    let mut schema = HashMap::new();

    for row in table_rows {
        let table_name: String = row.get(0);

        let columns = if is_sqlite {
            fetch_sqlite_columns(pool, &table_name).await?
        } else {
            fetch_postgres_columns(pool, &table_name).await?
        };

        schema.insert(
            table_name.clone(),
            DataTableInfo {
                name: table_name,
                columns,
                primary_keys: vec![],
                foreign_keys: vec![],
            },
        );
    }

    Ok(schema)
}

async fn fetch_postgres_columns(
    pool: &AnyPool,
    table: &str,
) -> Result<HashMap<String, DataColumnInfo>, sqlx::Error> {
    let sql = format!(
        "SELECT
            CAST(column_name AS VARCHAR) as column_name,
            CAST(data_type AS VARCHAR) as data_type,
            CAST(is_nullable AS VARCHAR) as is_nullable
         FROM information_schema.columns
         WHERE table_name = '{}'
         ORDER BY ordinal_position",
        table
    );

    let rows = sqlx::query(&sql).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let name: String = r.get("column_name");
            let raw_type: String = r.get("data_type");
            (
                name.clone(),
                DataColumnInfo {
                    name,
                    col_type: NormalizedType::from_raw(&raw_type),
                    is_nullable: r.get::<String, _>("is_nullable").to_uppercase() == "YES",
                },
            )
        })
        .collect())
}

async fn fetch_sqlite_columns(
    pool: &AnyPool,
    table: &str,
) -> Result<HashMap<String, DataColumnInfo>, sqlx::Error> {
    let sql = format!("PRAGMA table_info('{}')", table);
    let rows = sqlx::query(&sql).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let name: String = r.get("name");
            let raw_type: String = r.get("type");
            (
                name.clone(),
                DataColumnInfo {
                    name,
                    col_type: NormalizedType::from_raw(&raw_type),
                    is_nullable: r.get::<i32, _>("notnull") == 0,
                },
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use sqlx::{any, AnyPool};

    use super::get_database_schema;

    #[tokio::test]
    async fn test_sqlite_schema_discovery() -> anyhow::Result<()> {
        dotenvy::dotenv().ok();
        any::install_default_drivers();
        let path =
            std::env::var("SQLITE_PATH").expect("SQLITE_PATH must be set (see .env.example)");
        let pool = AnyPool::connect(&path).await?;
        let schema = get_database_schema(&pool).await?;
        println!("SQLite Schema: {:#?}", schema);
        Ok(())
    }

    #[tokio::test]
    async fn test_postgres_schema_discovery() -> anyhow::Result<()> {
        dotenvy::dotenv().ok();
        any::install_default_drivers();
        let url = std::env::var("POSTGRES_URL")
            .expect("POSTGRES_URL must be set (see .env.example)");
        let pool = AnyPool::connect(&url).await?;
        let schema = get_database_schema(&pool).await?;
        println!("PostgreSQL Schema: {:#?}", schema);
        Ok(())
    }
}
