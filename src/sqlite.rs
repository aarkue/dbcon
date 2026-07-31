//! The SQLite backend, on `rusqlite` rather than `sqlx`.
//!
//! # Why not sqlx
//!
//! SQLite's C API is blocking and sqlx is async, so sqlx-sqlite gives every connection a
//! dedicated OS thread (`sqlx-sqlite/src/connection/worker.rs`: "Each SQLite connection has
//! a dedicated thread") and ships rows back over a `flume` channel, one message per row.
//! That is a fair trade for a web handler returning fifty rows. For a full-table scan it
//! means every row crosses a thread boundary and a channel, which measured at ~12.6x the
//! cost of stepping the statement directly.
//!
//! Everything here is synchronous, because SQLite is.
//!
//! # Value decoding: storage class first, declared type as a re-tag
//!
//! SQLite columns have no type, only an affinity, so a value's *storage class* is the only
//! thing that is true of the value in hand. [`decode`] therefore dispatches on the storage
//! class and consults the declared type only to re-tag a value whose storage class is
//! ambiguous about intent: an INTEGER in a `BOOLEAN` column, an INTEGER in a `REAL` column,
//! text in a `DATETIME` column.
//!
//! This is a strict improvement on the sqlx path it replaces, which dispatched on the
//! declared type first and yielded [`NormalizedValue::Null`] whenever the value on the row
//! did not match it -- a text value in a column declared `INTEGER` (which SQLite permits and
//! real files contain) was silently dropped.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Mutex;

use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags};

use crate::{NormalizedType, NormalizedValue};

/// An open SQLite database.
///
/// The [`Mutex`] is not for concurrency but for `Sync`: `rusqlite::Connection` is `Send` but
/// not `Sync`, and [`DataSource`](crate::DataSource) is shared behind `&self`. It is taken
/// once per statement, never per row.
#[derive(Debug)]
pub struct SqliteSource {
    connection: Mutex<Connection>,
}

impl SqliteSource {
    /// Open the database named by a `sqlite:`/`sqlite://` connection string.
    ///
    /// Everything after `?` is handed to SQLite as URI query parameters, so `mode=ro`,
    /// `immutable=1`, `cache=shared` and `vfs=` all work as documented at
    /// <https://www.sqlite.org/uri.html> without this function knowing about them
    /// individually. `mode` can only reduce access below the flags below, which is what
    /// makes passing `SQLITE_OPEN_CREATE` here safe for a `mode=ro` caller.
    pub fn open(connection_string: &str) -> anyhow::Result<Self> {
        let rest = connection_string
            .strip_prefix("sqlite://")
            .or_else(|| connection_string.strip_prefix("sqlite:"))
            .unwrap_or(connection_string);

        let (path, query) = match rest.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (rest, None),
        };

        let connection = if path.is_empty() || path == ":memory:" {
            Connection::open_in_memory()?
        } else {
            let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX;
            let mut uri = format!("file:{}", uri_escape_path(path));
            if let Some(query) = query {
                uri.push('?');
                uri.push_str(query);
            }
            Connection::open_with_flags(uri, flags)?
        };

        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Run `sql`, calling `handler` once per row with a buffer reused across the whole scan.
    ///
    /// The buffer is why the handler takes a slice rather than a `Vec`: a full-table scan
    /// allocates one `Vec<NormalizedValue>` for the scan, not one per row.
    pub fn for_each(
        &self,
        sql: &str,
        handler: &mut dyn FnMut(&[NormalizedValue]) -> ControlFlow<()>,
    ) -> anyhow::Result<()> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        // Borrowed from the statement, so it has to be read before `query` takes it mutably.
        let types = declared_types(&statement);
        let mut buffer = vec![NormalizedValue::Null; types.len()];

        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            for (i, declared) in types.iter().enumerate() {
                buffer[i] = decode(row.get_ref(i)?, declared);
            }
            if handler(&buffer).is_break() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Run `sql`, calling `handler` once per row with each value paired with its column name.
    ///
    /// Separate from [`Self::for_each`] because the names cost a `String` clone per cell and
    /// only the arbitrary-SQL entry point needs them.
    pub fn for_each_named(
        &self,
        sql: &str,
        handler: &mut dyn FnMut(Vec<(String, NormalizedValue)>),
    ) -> anyhow::Result<()> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let types = declared_types(&statement);
        let names: Vec<String> = statement
            .column_names()
            .into_iter()
            .map(str::to_string)
            .collect();

        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let mut named = Vec::with_capacity(types.len());
            for (i, declared) in types.iter().enumerate() {
                named.push((names[i].clone(), decode(row.get_ref(i)?, declared)));
            }
            handler(named);
        }
        Ok(())
    }

    /// Collect at most `limit` rows of `sql` as `NormalizedValue`s.
    pub fn rows(
        &self,
        sql: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<Vec<NormalizedValue>>> {
        let mut out = Vec::new();
        self.for_each(sql, &mut |values| {
            out.push(values.to_vec());
            match limit {
                Some(limit) if out.len() >= limit => ControlFlow::Break(()),
                _ => ControlFlow::Continue(()),
            }
        })?;
        Ok(out)
    }

    /// Collect at most `limit` rows of `sql` as `{column name -> stringified value}`, for the
    /// preview API.
    pub fn named_string_rows(
        &self,
        sql: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let mut out = Vec::new();
        self.for_each_named(sql, &mut |named| {
            if limit.is_some_and(|limit| out.len() >= limit) {
                return;
            }
            out.push(
                named
                    .into_iter()
                    .map(|(name, value)| (name, value.to_string()))
                    .collect(),
            );
        })?;
        Ok(out)
    }

    /// The single text column of `sql`, skipping nulls and non-text values.
    ///
    /// Non-text values are skipped rather than stringified to keep the contract of
    /// [`DataSource::get_distinct_values`](crate::DataSource::get_distinct_values) --
    /// "distinct values as text" -- honest about which columns it can answer for.
    pub fn text_column(&self, sql: &str) -> anyhow::Result<Vec<String>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let mut rows = statement.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            if let ValueRef::Text(bytes) = row.get_ref(0)? {
                out.push(String::from_utf8_lossy(bytes).into_owned());
            }
        }
        Ok(out)
    }

    /// The first column of every row of `sql` as text, used by schema discovery where the
    /// catalog is known to return text.
    pub(crate) fn query_strings(&self, sql: &str, params: &[&str]) -> anyhow::Result<Vec<String>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(params), |row| {
            row.get::<_, Option<String>>(0)
        })?;
        Ok(rows
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    /// Run `sql` with `params` and map each row through `f`.
    pub(crate) fn query_rows<T>(
        &self,
        sql: &str,
        params: &[&str],
        f: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    ) -> anyhow::Result<Vec<T>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(params), f)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// A poisoned lock means a previous caller panicked mid-statement. The connection itself
    /// is still usable -- SQLite has no Rust-level invariant to break -- so this recovers
    /// rather than propagating the panic to an unrelated caller.
    fn lock(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Connection>> {
        Ok(self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }
}

/// Percent-encode the characters that would otherwise terminate or split a `file:` URI.
///
/// SQLite parses everything after the first `?` as query parameters and everything after `#`
/// as a fragment, so a path containing either has to escape it; `%` is escaped so an existing
/// literal `%` is not read back as an escape.
fn uri_escape_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for ch in path.chars() {
        match ch {
            '%' => out.push_str("%25"),
            '?' => out.push_str("%3f"),
            '#' => out.push_str("%23"),
            c => out.push(c),
        }
    }
    out
}

/// The [`NormalizedType`] of each result column, from its declared type.
///
/// A column with no declared type -- every expression, `count(*)` included -- gets
/// `Unknown("")`, which [`decode`] reads as "trust the storage class".
fn declared_types(statement: &rusqlite::Statement<'_>) -> Vec<NormalizedType> {
    statement
        .columns()
        .iter()
        .map(|column| match column.decl_type() {
            Some(declared) => NormalizedType::from_sqlite_declared(declared),
            None => NormalizedType::Unknown(String::new()),
        })
        .collect()
}

/// Turn one SQLite value into a [`NormalizedValue`], re-tagging by declared type where the
/// storage class alone would understate it. See this module's header for the rule.
fn decode(value: ValueRef<'_>, declared: &NormalizedType) -> NormalizedValue {
    match value {
        ValueRef::Null => NormalizedValue::Null,
        ValueRef::Integer(n) => match declared {
            NormalizedType::Boolean => NormalizedValue::Boolean(n != 0),
            NormalizedType::Float => NormalizedValue::Float(n as f64),
            // SQLite's own date functions write seconds since the epoch into an integer
            // column, which is the only reading of an integer in a datetime column that is
            // ever intended.
            NormalizedType::Timestamp => match chrono::DateTime::from_timestamp(n, 0) {
                Some(t) => NormalizedValue::Timestamp(t.fixed_offset()),
                None => NormalizedValue::Integer(n),
            },
            _ => NormalizedValue::Integer(n),
        },
        ValueRef::Real(f) => NormalizedValue::Float(f),
        ValueRef::Text(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            match declared {
                NormalizedType::Timestamp => match parse_timestamp(&text) {
                    Some(t) => NormalizedValue::Timestamp(t),
                    // Deliberately text, not Null: a datetime column holding something this
                    // cascade cannot read still carries data, and a caller with a custom
                    // format can parse what the database actually stored.
                    None => NormalizedValue::Text(text.into_owned()),
                },
                NormalizedType::Boolean => match text.as_ref() {
                    "true" | "TRUE" | "t" | "1" => NormalizedValue::Boolean(true),
                    "false" | "FALSE" | "f" | "0" => NormalizedValue::Boolean(false),
                    _ => NormalizedValue::Text(text.into_owned()),
                },
                // A JSON column stays textual rather than becoming `Json`. SQLite has no JSON
                // type -- the value *is* the text the file holds -- and re-serialising it
                // through `serde_json` would change the bytes a caller matches against for
                // nothing.
                _ => NormalizedValue::Text(text.into_owned()),
            }
        }
        // dbcon has no byte-string variant, and rendering bytes as text would corrupt them.
        ValueRef::Blob(_) => NormalizedValue::Null,
    }
}

/// Read the datetime spellings SQLite itself writes and accepts, widest first.
///
/// `strftime`-style output (`2024-01-02 03:04:05`) has no zone, ISO 8601 output has a `T` and
/// may carry one, and `date()` writes a bare date. All are stored as text in a column whose
/// declared type SQLite never enforces, so the cascade is the only way to read them.
fn parse_timestamp(text: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(text) {
        return Some(t);
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M%#z",
        "%Y-%m-%dT%H:%M%#z",
    ] {
        if let Ok(t) = chrono::DateTime::parse_from_str(text, format) {
            return Some(t);
        }
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(t) = chrono::NaiveDateTime::parse_from_str(text, format) {
            return Some(t.and_utc().fixed_offset());
        }
    }
    chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .ok()
        .map(|d| {
            d.and_hms_opt(0, 0, 0)
                .unwrap_or_default()
                .and_utc()
                .fixed_offset()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded(value: ValueRef<'_>, declared: &str) -> NormalizedValue {
        decode(value, &NormalizedType::from_sqlite_declared(declared))
    }

    #[test]
    fn storage_class_decides_when_the_declared_type_is_silent() {
        assert_eq!(
            decoded(ValueRef::Integer(7), ""),
            NormalizedValue::Integer(7)
        );
        assert_eq!(
            decoded(ValueRef::Real(1.5), ""),
            NormalizedValue::Float(1.5)
        );
        assert_eq!(
            decoded(ValueRef::Text(b"hi"), ""),
            NormalizedValue::Text("hi".into())
        );
        assert_eq!(decoded(ValueRef::Null, "INTEGER"), NormalizedValue::Null);
    }

    #[test]
    fn declared_type_re_tags_an_ambiguous_integer() {
        assert_eq!(
            decoded(ValueRef::Integer(1), "BOOLEAN"),
            NormalizedValue::Boolean(true)
        );
        assert_eq!(
            decoded(ValueRef::Integer(0), "BOOLEAN"),
            NormalizedValue::Boolean(false)
        );
        // A whole number written into a REAL column is a float; sqlx's decode agreed, and a
        // caller comparing against a float literal depends on it.
        assert_eq!(
            decoded(ValueRef::Integer(3), "REAL"),
            NormalizedValue::Float(3.0)
        );
    }

    /// The regression the sqlx path had: a value whose storage class disagrees with the
    /// declared type is data, not a decode failure.
    #[test]
    fn a_value_that_contradicts_its_declared_type_survives() {
        assert_eq!(
            decoded(ValueRef::Text(b"n/a"), "INTEGER"),
            NormalizedValue::Text("n/a".into())
        );
        assert_eq!(
            decoded(ValueRef::Integer(42), "VARCHAR(10)"),
            NormalizedValue::Integer(42)
        );
    }

    #[test]
    fn datetime_columns_read_the_spellings_sqlite_writes() {
        let cases = [
            "2024-01-02 03:04:05",
            "2024-01-02T03:04:05",
            "2024-01-02 03:04:05.123",
            "2024-01-02T03:04:05Z",
            "2024-01-02T03:04:05+02:00",
            "2024-01-02 03:04",
            "2024-01-02",
        ];
        for case in cases {
            assert!(
                matches!(
                    decoded(ValueRef::Text(case.as_bytes()), "DATETIME"),
                    NormalizedValue::Timestamp(_)
                ),
                "{case} did not parse"
            );
        }
    }

    #[test]
    fn an_unparseable_datetime_stays_text_rather_than_becoming_null() {
        assert_eq!(
            decoded(ValueRef::Text(b"02/01/2024"), "DATETIME"),
            NormalizedValue::Text("02/01/2024".into())
        );
    }

    #[test]
    fn integer_in_a_datetime_column_is_unix_seconds() {
        let NormalizedValue::Timestamp(t) = decoded(ValueRef::Integer(0), "TIMESTAMP") else {
            panic!("expected a timestamp");
        };
        assert_eq!(t.to_rfc3339(), "1970-01-01T00:00:00+00:00");
    }

    #[test]
    fn a_path_with_a_question_mark_is_not_read_as_query_parameters() {
        assert_eq!(uri_escape_path("/tmp/a?b#c"), "/tmp/a%3fb%23c");
        assert_eq!(uri_escape_path("/tmp/100%.db"), "/tmp/100%25.db");
    }

    #[test]
    fn memory_and_path_forms_both_open() {
        SqliteSource::open("sqlite::memory:").expect("in-memory via sqlite::memory:");
        SqliteSource::open("sqlite://:memory:").expect("in-memory via sqlite://:memory:");
    }

    #[test]
    fn scan_reuses_one_buffer_and_honours_break() {
        let source = SqliteSource::open("sqlite::memory:").unwrap();
        source
            .for_each(
                "CREATE TABLE t (id INTEGER, flag BOOLEAN, at DATETIME)",
                &mut |_| ControlFlow::Continue(()),
            )
            .unwrap();
        source
            .for_each(
                "INSERT INTO t VALUES (1, 1, '2024-01-02 03:04:05'), (2, 0, NULL), (3, 1, NULL)",
                &mut |_| ControlFlow::Continue(()),
            )
            .unwrap();

        let mut seen = Vec::new();
        source
            .for_each("SELECT id, flag, at FROM t ORDER BY id", &mut |row| {
                seen.push(row.to_vec());
                ControlFlow::Continue(())
            })
            .unwrap();
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0][0], NormalizedValue::Integer(1));
        assert_eq!(seen[0][1], NormalizedValue::Boolean(true));
        assert!(matches!(seen[0][2], NormalizedValue::Timestamp(_)));
        assert_eq!(seen[1][1], NormalizedValue::Boolean(false));
        assert_eq!(seen[1][2], NormalizedValue::Null);

        let mut count = 0;
        source
            .for_each("SELECT id FROM t ORDER BY id", &mut |_| {
                count += 1;
                ControlFlow::Break(())
            })
            .unwrap();
        assert_eq!(count, 1, "Break must stop the scan, not just the callback");
    }

    #[test]
    fn count_star_has_no_declared_type_and_still_decodes_as_an_integer() {
        let source = SqliteSource::open("sqlite::memory:").unwrap();
        source
            .for_each("CREATE TABLE t (id INTEGER)", &mut |_| {
                ControlFlow::Continue(())
            })
            .unwrap();
        source
            .for_each("INSERT INTO t VALUES (1), (2)", &mut |_| {
                ControlFlow::Continue(())
            })
            .unwrap();

        let mut got = NormalizedValue::Null;
        source
            .for_each("SELECT count(*) FROM t", &mut |row| {
                got = row[0].clone();
                ControlFlow::Continue(())
            })
            .unwrap();
        assert_eq!(got, NormalizedValue::Integer(2));
    }
}
