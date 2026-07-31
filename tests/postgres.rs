//! PostgreSQL discovery, against a live server given by `POSTGRES_URL`.
//!
//! **There is no PostgreSQL corpus file.** A Postgres schema lives in a running server,
//! not in a file that can be committed or pointed at with `DBCON_CORPUS`, so unlike the
//! SQLite half of discovery this half has no offline coverage at all: the catalog queries
//! in `src/discovery/postgres.rs` are exercised only when `POSTGRES_URL` points at a
//! reachable database. When it does not, this test prints that it did not run.
//!
//! Set `DBCON_POSTGRES_REQUIRED=1` to turn "no server" into a failure, the same contract
//! `tests/corpus.rs` uses for `DBCON_CORPUS`.

#![cfg(feature = "postgres")]

use dbcon::DataSource;

#[tokio::test]
async fn postgres_discovery_against_a_live_server() {
    dotenvy::dotenv().ok();
    let required = std::env::var("DBCON_POSTGRES_REQUIRED").as_deref() == Ok("1");
    let Ok(url) = std::env::var("POSTGRES_URL") else {
        let msg = "POSTGRES_URL is not set: PostgreSQL discovery was NOT exercised. \
                   There is no Postgres corpus file on this machine; set POSTGRES_URL to \
                   a reachable server to cover src/discovery/postgres.rs.";
        assert!(!required, "{msg}");
        eprintln!("SKIPPED: {msg}");
        return;
    };

    let ds = match DataSource::new_any("postgres".into(), url).await {
        Ok(ds) => ds,
        Err(e) => {
            let msg = format!("POSTGRES_URL is set but unusable ({e}); discovery NOT exercised");
            assert!(!required, "{msg}");
            eprintln!("SKIPPED: {msg}");
            return;
        }
    };

    // Nothing here asserts a particular schema - the server's contents are unknown. What
    // it does assert is that all four catalog queries ran and agree with each other.
    for table in ds.tables.values() {
        assert!(
            !table.columns.is_empty(),
            "table {} discovered with no columns",
            table.name
        );
        for key in &table.primary_keys {
            for col in &key.columns {
                assert!(
                    table.columns.contains_key(col),
                    "key {} names column {col}, which {} does not have",
                    key.name,
                    table.name
                );
            }
        }
        for fk in &table.foreign_keys {
            assert_eq!(
                fk.from_columns.len(),
                fk.to_columns.len(),
                "foreign key {} pairs {} columns with {}",
                fk.name,
                fk.from_columns.len(),
                fk.to_columns.len()
            );
            for col in &fk.from_columns {
                assert!(
                    table.columns.contains_key(col),
                    "foreign key {} names column {col}, which {} does not have",
                    fk.name,
                    table.name
                );
            }
        }
    }
    eprintln!(
        "postgres: {} tables discovered, {} column(s) with unmapped types: {:?}",
        ds.tables.len(),
        ds.unknown_column_types().len(),
        ds.unknown_column_types()
    );
}
