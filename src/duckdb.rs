//! The `DuckDB` backend, on the `duckdb` crate.
//!
//! # Value decoding: the column's type is the value's type
//!
//! Unlike SQLite, `DuckDB` is statically typed: a column declared `BIGINT` yields a `BigInt` on
//! every row, so there is no storage-class-versus-declared-type disagreement to resolve and
//! [`decode`] dispatches on the value in hand alone. That is why this file has no equivalent of
//! `sqlite::declared_types`.
//!
//! The width variants all fold into [`NormalizedValue::Integer`] (an `i64`), which is the widest
//! integer the normalized model has. `HugeInt`/`UBigInt` can exceed it: those are carried as
//! [`NormalizedValue::Unknown`] holding the decimal text rather than being wrapped silently, so a
//! caller sees the real value and can re-parse it.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Mutex;

use duckdb::types::{TimeUnit, ValueRef};
use duckdb::Connection;

use crate::{NormalizedType, NormalizedValue};

/// An open `DuckDB` database.
///
/// The [`Mutex`] is not for concurrency but for `Sync`, exactly as in
/// [`SqliteSource`](crate::SqliteSource): the connection is `Send` but not `Sync`, and
/// [`DataSource`](crate::DataSource) is shared behind `&self`. Taken once per statement, never
/// per row.
#[derive(Debug)]
pub struct DuckDbSource {
    connection: Mutex<Connection>,
}

impl DuckDbSource {
    /// Open the database named by a `duckdb:`/`duckdb://` connection string.
    ///
    /// An empty path or `:memory:` opens a private in-memory database, matching the SQLite
    /// backend. A file that does not exist is created, which is `DuckDB`'s own default and what
    /// makes a freshly named path usable as an output.
    pub fn open(connection_string: &str) -> anyhow::Result<Self> {
        let rest = connection_string
            .strip_prefix("duckdb://")
            .or_else(|| connection_string.strip_prefix("duckdb:"))
            .unwrap_or(connection_string);
        // `DuckDB` takes no URI query parameters, so unlike the SQLite backend there is nothing to
        // split off and forward; a `?` here is part of the path.
        let connection = if rest.is_empty() || rest == ":memory:" {
            Connection::open_in_memory()?
        } else {
            Connection::open(rest)?
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
        let mut rows = statement.query([])?;
        let mut buffer: Vec<NormalizedValue> = Vec::new();
        while let Some(row) = rows.next()? {
            // The column count is only known once a row has been produced: `DuckDB` reports it
            // from the result set, and an empty result never enters this loop at all.
            if buffer.is_empty() {
                buffer = vec![NormalizedValue::Null; row.as_ref().column_count()];
            }
            for (i, slot) in buffer.iter_mut().enumerate() {
                *slot = decode(row.get_ref(i)?);
            }
            if handler(&buffer).is_break() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Run `sql`, calling `handler` once per row with each value paired with its column name.
    ///
    /// Separate from [`Self::for_each`] because the names cost a `String` clone per cell and only
    /// the arbitrary-SQL entry point needs them.
    pub fn for_each_named(
        &self,
        sql: &str,
        handler: &mut dyn FnMut(Vec<(String, NormalizedValue)>),
    ) -> anyhow::Result<()> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let mut rows = statement.query([])?;
        let mut names: Vec<String> = Vec::new();
        while let Some(row) = rows.next()? {
            if names.is_empty() {
                names = row
                    .as_ref()
                    .column_names()
                    .into_iter()
                    .map(|n| n.to_string())
                    .collect();
            }
            let mut named = Vec::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                named.push((name.clone(), decode(row.get_ref(i)?)));
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
    /// Non-text values are skipped rather than stringified, keeping
    /// [`DataSource::get_distinct_values`](crate::DataSource::get_distinct_values)'s contract --
    /// "distinct values as text" -- honest about which columns it can answer for. Same rule as the
    /// SQLite backend.
    pub fn text_column(&self, sql: &str) -> anyhow::Result<Vec<String>> {
        self.query_strings(sql, &[])
    }

    /// The first column of every row of `sql` as text, for schema discovery, where the catalog is
    /// known to return text.
    pub(crate) fn query_strings(&self, sql: &str, params: &[&str]) -> anyhow::Result<Vec<String>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let mut rows = statement.query(duckdb::params_from_iter(params.iter()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            if let ValueRef::Text(bytes) = row.get_ref(0)? {
                out.push(String::from_utf8_lossy(bytes).into_owned());
            }
        }
        Ok(out)
    }

    /// Every row of `sql` as a `Vec<String>`, nulls rendered as the empty string. Discovery reads
    /// several text columns at once this way.
    pub(crate) fn query_string_rows(
        &self,
        sql: &str,
        params: &[&str],
    ) -> anyhow::Result<Vec<Vec<String>>> {
        let connection = self.lock()?;
        let mut statement = connection.prepare(sql)?;
        let mut rows = statement.query(duckdb::params_from_iter(params.iter()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let n = row.as_ref().column_count();
            let mut fields = Vec::with_capacity(n);
            for i in 0..n {
                fields.push(match row.get_ref(i)? {
                    ValueRef::Text(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    ValueRef::Null => String::new(),
                    other => decode(other).to_string(),
                });
            }
            out.push(fields);
        }
        Ok(out)
    }

    /// A poisoned lock means a previous caller panicked mid-statement. The connection itself
    /// is still usable -- `DuckDB` has no Rust-level invariant to break -- so this recovers
    /// rather than propagating the panic to an unrelated caller.
    fn lock(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Connection>> {
        Ok(self
            .connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner))
    }
}

/// `value` counted in `unit`s since the Unix epoch, as a UTC instant.
///
/// Each unit gets its own constructor rather than being folded into nanoseconds first. A
/// nanosecond count only spans 1677..2262, and `DuckDB`'s default `TIMESTAMP` is microseconds, so
/// converting through it would lose every ordinary date outside that window.
fn instant(unit: TimeUnit, value: i64) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    match unit {
        TimeUnit::Second => chrono::DateTime::from_timestamp(value, 0),
        TimeUnit::Millisecond => chrono::DateTime::from_timestamp_millis(value),
        TimeUnit::Microsecond => chrono::DateTime::from_timestamp_micros(value),
        TimeUnit::Nanosecond => Some(chrono::DateTime::from_timestamp_nanos(value)),
    }
    .map(|t| t.fixed_offset())
}

/// One `DuckDB` value as a [`NormalizedValue`]. See this module's header for the rule.
pub(crate) fn decode(value: ValueRef<'_>) -> NormalizedValue {
    match value {
        ValueRef::Null => NormalizedValue::Null,
        ValueRef::Boolean(b) => NormalizedValue::Boolean(b),
        ValueRef::TinyInt(i) => NormalizedValue::Integer(i.into()),
        ValueRef::SmallInt(i) => NormalizedValue::Integer(i.into()),
        ValueRef::Int(i) => NormalizedValue::Integer(i.into()),
        ValueRef::BigInt(i) => NormalizedValue::Integer(i),
        ValueRef::UTinyInt(i) => NormalizedValue::Integer(i.into()),
        ValueRef::USmallInt(i) => NormalizedValue::Integer(i.into()),
        ValueRef::UInt(i) => NormalizedValue::Integer(i.into()),
        // Wider than the normalized model's `i64`. Narrowed when it fits, carried as text when it
        // does not, so a value past `i64::MAX` is visible rather than wrapped or dropped.
        ValueRef::UBigInt(i) => i64::try_from(i)
            .map(NormalizedValue::Integer)
            .unwrap_or_else(|_| NormalizedValue::Unknown(i.to_string())),
        ValueRef::HugeInt(i) => i64::try_from(i)
            .map(NormalizedValue::Integer)
            .unwrap_or_else(|_| NormalizedValue::Unknown(i.to_string())),
        ValueRef::Float(f) => NormalizedValue::Float(f.into()),
        ValueRef::Double(f) => NormalizedValue::Float(f),
        // `Decimal` is exact and `f64` is not, so its text form is kept rather than rounded into a
        // `Float` that no longer equals what the column holds.
        ValueRef::Decimal(d) => NormalizedValue::Unknown(d.to_string()),
        ValueRef::Timestamp(unit, v) => {
            instant(unit, v).map_or(NormalizedValue::Null, NormalizedValue::Timestamp)
        }
        ValueRef::Text(bytes) => NormalizedValue::Text(String::from_utf8_lossy(bytes).into_owned()),
        // dbcon has no byte-string variant, and rendering bytes as text would corrupt them.
        ValueRef::Blob(_) => NormalizedValue::Null,
        // A date has no time zone; anchored to UTC midnight, which is how the extractor reads a
        // naive date elsewhere.
        ValueRef::Date32(days) => chrono::DateTime::from_timestamp(i64::from(days) * 86_400, 0)
            .map_or(NormalizedValue::Null, |d| {
                NormalizedValue::Timestamp(d.fixed_offset())
            }),
        // A time of day is not an instant, so it stays text rather than being anchored to an
        // arbitrary date.
        ValueRef::Time64(unit, v) => NormalizedValue::Unknown(match unit {
            TimeUnit::Second => format!("{v}s"),
            TimeUnit::Millisecond => format!("{v}ms"),
            TimeUnit::Microsecond => format!("{v}us"),
            TimeUnit::Nanosecond => format!("{v}ns"),
        }),
        other => NormalizedValue::Unknown(format!("{other:?}")),
    }
}

/// A `DuckDB` type name as a [`NormalizedType`].
///
/// [`NormalizedType::from_raw`] carries the spellings every backend shares, including parameter
/// lists and the array types it keeps as `Unknown`. Only the names `DuckDB` alone reports are
/// resolved here, from the `Unknown` that mapping hands back, so a `BLOB` is spelled the same way
/// it is everywhere else.
pub(crate) fn normalize_type(declared: &str) -> NormalizedType {
    match NormalizedType::from_raw(declared) {
        NormalizedType::Unknown(raw) => match raw.as_str() {
            "hugeint" | "utinyint" | "usmallint" | "uinteger" | "ubigint" | "int1" | "short"
            | "long" | "signed" => NormalizedType::Integer,
            "logical" => NormalizedType::Boolean,
            "timestamp_s" | "timestamp_ms" | "timestamp_ns" => NormalizedType::Timestamp,
            _ => NormalizedType::Unknown(raw),
        },
        known => known,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> DuckDbSource {
        DuckDbSource::open("duckdb://:memory:").expect("in-memory database opens")
    }

    #[test]
    fn every_integer_width_normalizes_to_one_variant() {
        let s = source();
        let rows = s
            .rows(
                "SELECT CAST(1 AS TINYINT), CAST(2 AS SMALLINT), CAST(3 AS INTEGER), \
                 CAST(4 AS BIGINT), CAST(5 AS UINTEGER)",
                None,
            )
            .expect("query runs");
        assert_eq!(
            rows[0],
            vec![
                NormalizedValue::Integer(1),
                NormalizedValue::Integer(2),
                NormalizedValue::Integer(3),
                NormalizedValue::Integer(4),
                NormalizedValue::Integer(5),
            ]
        );
    }

    /// Past `i64`, the value is kept as text rather than wrapped -- the one case where a width
    /// variant cannot become an `Integer`.
    #[test]
    fn an_integer_wider_than_i64_is_carried_as_text() {
        let s = source();
        let rows = s
            .rows("SELECT CAST(170141183460469231731687303715884105727 AS HUGEINT)", None)
            .expect("query runs");
        assert_eq!(
            rows[0][0],
            NormalizedValue::Unknown("170141183460469231731687303715884105727".to_string())
        );
    }

    #[test]
    fn text_booleans_doubles_and_nulls_decode_to_their_own_variants() {
        let s = source();
        let rows = s
            .rows("SELECT 'x', TRUE, CAST(1.5 AS DOUBLE), NULL", None)
            .expect("query runs");
        assert_eq!(
            rows[0],
            vec![
                NormalizedValue::Text("x".to_string()),
                NormalizedValue::Boolean(true),
                NormalizedValue::Float(1.5),
                NormalizedValue::Null,
            ]
        );
    }

    #[test]
    fn a_timestamp_decodes_to_the_instant_it_names() {
        let s = source();
        let rows = s
            .rows("SELECT CAST('2024-03-04 05:06:07' AS TIMESTAMP)", None)
            .expect("query runs");
        let NormalizedValue::Timestamp(ts) = &rows[0][0] else {
            panic!("expected a timestamp, got {:?}", rows[0][0]);
        };
        assert_eq!(ts.to_utc().to_rfc3339(), "2024-03-04T05:06:07+00:00");
    }

    /// A `DECIMAL` is exact; rounding it into an `f64` would change the value, so it stays text.
    #[test]
    fn a_decimal_keeps_its_exact_text() {
        let s = source();
        let rows = s
            .rows("SELECT CAST('1.05' AS DECIMAL(18,2))", None)
            .expect("query runs");
        assert_eq!(rows[0][0], NormalizedValue::Unknown("1.05".to_string()));
    }

    #[test]
    fn a_scan_stops_when_the_handler_breaks() {
        let s = source();
        let mut seen = 0usize;
        s.for_each("SELECT * FROM range(100)", &mut |_| {
            seen += 1;
            if seen == 3 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .expect("scan runs");
        assert_eq!(seen, 3, "the scan must stop at the first Break");
    }

    #[test]
    fn named_rows_carry_the_column_names() {
        let s = source();
        let rows = s
            .named_string_rows("SELECT 1 AS a, 'b' AS b", None)
            .expect("query runs");
        assert_eq!(rows[0].get("a").map(String::as_str), Some("1"));
        assert_eq!(rows[0].get("b").map(String::as_str), Some("b"));
    }

    /// A date `DuckDB` stores happily but a nanosecond count cannot hold.
    #[test]
    fn a_timestamp_outside_the_nanosecond_range_still_decodes() {
        let s = source();
        let rows = s
            .rows("SELECT TIMESTAMP '2300-01-01 00:00:00'", None)
            .expect("query runs");
        let NormalizedValue::Timestamp(ts) = &rows[0][0] else {
            panic!("expected a timestamp, got {:?}", rows[0][0]);
        };
        assert_eq!(ts.to_utc().to_rfc3339(), "2300-01-01T00:00:00+00:00");
    }

    #[test]
    fn type_names_normalize_with_and_without_parameters() {
        assert_eq!(normalize_type("BIGINT"), NormalizedType::Integer);
        assert_eq!(normalize_type("HUGEINT"), NormalizedType::Integer);
        assert_eq!(normalize_type("DECIMAL(18,2)"), NormalizedType::Float);
        assert_eq!(normalize_type("VARCHAR(64)"), NormalizedType::Text);
        assert_eq!(normalize_type("TIMESTAMP WITH TIME ZONE"), NormalizedType::Timestamp);
        assert_eq!(normalize_type("TIMESTAMP_MS"), NormalizedType::Timestamp);
        assert_eq!(normalize_type("BOOLEAN"), NormalizedType::Boolean);
        assert_eq!(normalize_type("LOGICAL"), NormalizedType::Boolean);
        assert!(matches!(
            normalize_type("STRUCT(a INT)"),
            NormalizedType::Unknown(_)
        ));
    }

    /// A list is not its element type, and an unmapped name is spelled as the other backends
    /// spell it.
    #[test]
    fn arrays_and_binaries_stay_unknown_lowercased() {
        assert_eq!(
            normalize_type("VARCHAR[]"),
            NormalizedType::Unknown("varchar[]".to_string())
        );
        assert_eq!(
            normalize_type("BLOB"),
            NormalizedType::Unknown("blob".to_string())
        );
    }
}
