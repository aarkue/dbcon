//! Backend-agnostic value and type model, plus the raw-type-string mapping.
//!
//! Every backend reports its columns as *declared type strings* (`VARCHAR(255)`,
//! `int4`, `TIMESTAMP WITH TIME ZONE`, ...). [`NormalizedType::from_raw`] is the single
//! place where those strings are turned into dbcon's six semantic variants, so a caller
//! doing literal coercion or join-key comparison only has to understand this one mapping.

use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// A backend-agnostic classification of a column's data type.
///
/// Produced from a backend's declared type string by [`NormalizedType::from_raw`]
/// (dialect-neutral) or [`NormalizedType::from_sqlite_declared`] (SQLite, which adds
/// the affinity rules).
///
/// # The `Unknown` contract
///
/// `Unknown` is the *only* fallback. A type string that dbcon does not recognise is
/// never quietly reported as [`NormalizedType::Text`]: it comes back as
/// `Unknown(declared_type_lowercased)` so the caller can see exactly what the database
/// said and decide for itself. Consumers keying literal coercion or join semantics off
/// this enum can therefore treat `Unknown` as "decode dynamically / do not assume", and
/// can enumerate the unmapped types they hit via
/// [`DataSource::unknown_column_types`](crate::DataSource::unknown_column_types).
///
/// Binary types (`BLOB`, `BYTEA`, `VARBINARY`, ...) are deliberately `Unknown`: dbcon has
/// no byte-string variant and pretending they are text would corrupt values.
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
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

/// Read the datetime spellings a text-carried timestamp may arrive in, widest first.
///
/// `strftime`-style output (`2024-01-02 03:04:05`) has no zone, ISO 8601 output has a `T`
/// and may carry one, and a bare date has no time at all. Shared by the backends that
/// store an instant as text and so cannot be told its shape in advance: SQLite, whose
/// declared column type it never enforces, and xlsx, whose `DateTimeIso` cell holds
/// whatever the writer wrote.
#[cfg(any(feature = "sqlite", feature = "xlsx"))]
pub(crate) fn parse_timestamp(text: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
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

/// SQLite's five column affinities, as defined by
/// <https://www.sqlite.org/datatype3.html#determination_of_column_affinity>.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SqliteAffinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

/// Apply SQLite's documented affinity algorithm to a declared type string.
///
/// The five rules are applied in order, exactly as SQLite does; the first match wins.
/// Note that the rules are substring rules, so `POINT` gets INTEGER affinity (it
/// contains `INT`) - that is SQLite's real behaviour, not an approximation.
pub fn sqlite_affinity(declared: &str) -> SqliteAffinity {
    let d = declared.to_ascii_uppercase();
    if d.contains("INT") {
        SqliteAffinity::Integer
    } else if d.contains("CHAR") || d.contains("CLOB") || d.contains("TEXT") {
        SqliteAffinity::Text
    } else if d.contains("BLOB") || d.trim().is_empty() {
        SqliteAffinity::Blob
    } else if d.contains("REAL") || d.contains("FLOA") || d.contains("DOUB") {
        SqliteAffinity::Real
    } else {
        SqliteAffinity::Numeric
    }
}

/// Lowercase, collapse runs of whitespace, and drop any parenthesised parameter list.
///
/// `"VARCHAR(255)"` -> `"varchar"`, `"NUMERIC(10, 2)"` -> `"numeric"`,
/// `"timestamp(3)  with time zone"` -> `"timestamp with time zone"`.
/// Parameters are removed rather than truncated at, so trailing modifiers survive.
fn canonical_base(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut depth = 0usize;
    let mut last_was_space = true;
    for ch in raw.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth > 0 => {}
            c if c.is_whitespace() => {
                if !last_was_space {
                    out.push(' ');
                    last_was_space = true;
                }
            }
            c => {
                out.extend(c.to_lowercase());
                last_was_space = false;
            }
        }
    }
    let mut base = out.trim().to_string();
    // MySQL numeric modifiers carry no type information, and stack in any order
    // (`int unsigned zerofill`), so strip until nothing changes.
    loop {
        let before = base.len();
        for suffix in [" unsigned", " signed", " zerofill"] {
            if let Some(stripped) = base.strip_suffix(suffix) {
                base = stripped.trim_end().to_string();
            }
        }
        if base.len() == before {
            return base;
        }
    }
}

impl NormalizedType {
    /// Map a raw declared SQL type name onto a [`NormalizedType`], case-insensitively.
    ///
    /// This is the dialect-neutral mapping: it understands the spellings emitted by
    /// PostgreSQL (`int4`, `timestamptz`, `character varying`, `double precision`),
    /// MySQL (`tinyint(1)`, `bigint unsigned`, `longtext`), MSSQL (`nvarchar`,
    /// `datetime2`, `uniqueidentifier`) and standard SQL. Parameter lists are ignored,
    /// so `VARCHAR(255)` and `varchar` agree.
    ///
    /// Anything not in the table below is [`NormalizedType::Unknown`] carrying the
    /// lowercased declared type. There is no guessing fallback: an unrecognised type is
    /// never silently reported as `Text`.
    ///
    /// | Variant | Recognised spellings |
    /// |---|---|
    /// | `Text` | char, character, nchar, bpchar, varchar, varchar2, nvarchar, nvarchar2, character varying, national character(, varying), text, ntext, tinytext, mediumtext, longtext, clob, nclob, string, name, citext, xml, enum, set, uuid, uniqueidentifier |
    /// | `Integer` | int, integer, int2, int4, int8, int16, int32, int64, smallint, mediumint, bigint, tinyint, serial, serial2/4/8, smallserial, bigserial, year |
    /// | `Float` | real, float, float4, float8, double, double precision, numeric, decimal, dec, number, money, smallmoney |
    /// | `Boolean` | bool, boolean |
    /// | `Timestamp` | timestamp, timestamptz, timestamp with/without time zone, datetime, datetime2, smalldatetime, datetimeoffset, date, time, timetz, time with/without time zone |
    /// | `Json` | json, jsonb |
    /// | `Unknown` | everything else, including all binary types (blob, bytea, binary, varbinary, image, raw) and array types |
    ///
    /// `TINYINT(1)` maps to `Integer`, not `Boolean`: MySQL stores it as an integer and
    /// only its clients treat 0/1 as a boolean, so calling it `Boolean` here would make
    /// `2` unrepresentable.
    pub fn from_raw(raw_type: &str) -> Self {
        let base = canonical_base(raw_type);
        // Array types (`int4[]`, `text[]`) share their element's name; they are not that
        // element and dbcon has no array variant, so they stay visible as Unknown.
        if base.contains('[') {
            return Self::Unknown(normalize_unknown(raw_type));
        }
        match base.as_str() {
            "timestamp"
            | "timestamptz"
            | "timestamp with time zone"
            | "timestamp without time zone"
            | "datetime"
            | "datetime2"
            | "smalldatetime"
            | "datetimeoffset"
            | "date"
            | "time"
            | "timetz"
            | "time with time zone"
            | "time without time zone" => Self::Timestamp,

            "int" | "integer" | "int2" | "int4" | "int8" | "int16" | "int32" | "int64"
            | "smallint" | "mediumint" | "bigint" | "tinyint" | "serial" | "serial2"
            | "serial4" | "serial8" | "smallserial" | "bigserial" | "year" => Self::Integer,

            "char"
            | "character"
            | "nchar"
            | "bpchar"
            | "varchar"
            | "varchar2"
            | "nvarchar"
            | "nvarchar2"
            | "character varying"
            | "national character"
            | "national character varying"
            | "text"
            | "ntext"
            | "tinytext"
            | "mediumtext"
            | "longtext"
            | "clob"
            | "nclob"
            | "string"
            | "name"
            | "citext"
            | "xml"
            | "enum"
            | "set"
            | "uuid"
            | "uniqueidentifier" => Self::Text,

            "real" | "float" | "float4" | "float8" | "double" | "double precision" | "numeric"
            | "decimal" | "dec" | "number" | "money" | "smallmoney" => Self::Float,

            "bool" | "boolean" => Self::Boolean,

            "json" | "jsonb" => Self::Json,

            _ => Self::Unknown(normalize_unknown(raw_type)),
        }
    }

    /// Map a SQLite *declared* column type (the `type` column of `PRAGMA table_info`).
    ///
    /// SQLite columns have no fixed type, only an affinity derived from whatever string
    /// was written in the `CREATE TABLE`. So this first tries [`NormalizedType::from_raw`]
    /// (which keeps the semantics of names SQLite itself would flatten, notably `BOOLEAN`
    /// and `DATETIME`/`TIMESTAMP`, both NUMERIC affinity to SQLite) and only then falls
    /// back to SQLite's documented affinity algorithm:
    ///
    /// | Affinity | Maps to | Why |
    /// |---|---|---|
    /// | INTEGER | `Integer` | `UNSIGNED BIG INT`, `INT8` |
    /// | TEXT | `Text` | `VARYING CHARACTER(255)`, `NATIVE CHARACTER(70)` |
    /// | REAL | `Float` | `DOUBLE`, `FLOA` |
    /// | BLOB | `Unknown` | a column with *no declared type at all* lands here; there is nothing to say about it |
    /// | NUMERIC | `Unknown` | SQLite stores these as integer, real *or* text depending on the value, so no single variant is correct |
    ///
    /// The two `Unknown` rows are the deliberate fallback: `Unknown` carries the declared
    /// string (empty string included) so the caller can see it, and dbcon's row decoding
    /// treats `Unknown` columns as "try each representation" rather than assuming text.
    pub fn from_sqlite_declared(declared: &str) -> Self {
        match Self::from_raw(declared) {
            Self::Unknown(raw) => match sqlite_affinity(&raw) {
                SqliteAffinity::Integer => Self::Integer,
                SqliteAffinity::Text => Self::Text,
                SqliteAffinity::Real => Self::Float,
                SqliteAffinity::Blob | SqliteAffinity::Numeric => Self::Unknown(raw),
            },
            known => known,
        }
    }

    /// The declared type string, if this type could not be mapped.
    pub fn unknown_type(&self) -> Option<&str> {
        match self {
            Self::Unknown(raw) => Some(raw),
            _ => None,
        }
    }
}

/// The form an unrecognised type is preserved in: lowercased and whitespace-collapsed,
/// but with any parameter list kept so the caller sees what the database actually said.
fn normalize_unknown(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_was_space = true;
    for ch in raw.chars() {
        if ch.is_whitespace() {
            if !last_was_space {
                out.push(' ');
                last_was_space = true;
            }
        } else {
            out.extend(ch.to_lowercase());
            last_was_space = false;
        }
    }
    out.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(raw: &str) -> NormalizedType {
        NormalizedType::from_raw(raw)
    }

    #[test]
    fn parameterised_forms_ignore_their_parameters() {
        assert_eq!(t("VARCHAR(255)"), NormalizedType::Text);
        assert_eq!(t("varchar (255)"), NormalizedType::Text);
        assert_eq!(t("NUMERIC(10,2)"), NormalizedType::Float);
        assert_eq!(t("NUMERIC(10, 2)"), NormalizedType::Float);
        assert_eq!(t("TINYINT(1)"), NormalizedType::Integer);
        assert_eq!(t("NVARCHAR(160)"), NormalizedType::Text);
        assert_eq!(t("timestamp(3) with time zone"), NormalizedType::Timestamp);
    }

    #[test]
    fn postgres_internal_spellings() {
        assert_eq!(t("int4"), NormalizedType::Integer);
        assert_eq!(t("int8"), NormalizedType::Integer);
        assert_eq!(t("float8"), NormalizedType::Float);
        assert_eq!(t("bpchar"), NormalizedType::Text);
        assert_eq!(t("TIMESTAMPTZ"), NormalizedType::Timestamp);
        assert_eq!(t("TIMESTAMP WITH TIME ZONE"), NormalizedType::Timestamp);
        assert_eq!(t("timestamp without time zone"), NormalizedType::Timestamp);
        assert_eq!(t("DOUBLE PRECISION"), NormalizedType::Float);
        assert_eq!(t("character varying"), NormalizedType::Text);
        assert_eq!(t("BOOL"), NormalizedType::Boolean);
        assert_eq!(t("jsonb"), NormalizedType::Json);
    }

    #[test]
    fn mysql_modifiers_are_stripped() {
        assert_eq!(t("BIGINT UNSIGNED"), NormalizedType::Integer);
        assert_eq!(t("int(11) unsigned zerofill"), NormalizedType::Integer);
        assert_eq!(t("DECIMAL(10,2) SIGNED"), NormalizedType::Float);
    }

    #[test]
    fn case_and_whitespace_are_irrelevant() {
        assert_eq!(
            t("  TiMeStAmP   WiTh  TiMe   ZoNe "),
            NormalizedType::Timestamp
        );
    }

    /// The core promise: unrecognised is visible, not silently `Text`.
    #[test]
    fn unrecognised_types_are_unknown_not_text() {
        for raw in [
            "BLOB",
            "bytea",
            "VARBINARY(16)",
            "image",
            "geometry",
            "_int4",
            "int4[]",
            "TEXT[]",
            "hstore",
        ] {
            let mapped = t(raw);
            assert!(
                matches!(mapped, NormalizedType::Unknown(_)),
                "{raw} must map to Unknown, got {mapped:?}"
            );
            assert_ne!(mapped, NormalizedType::Text);
        }
        assert_eq!(t("BYTEA").unknown_type(), Some("bytea"));
        assert_eq!(t("VARBINARY(16)").unknown_type(), Some("varbinary(16)"));
    }

    #[test]
    fn sqlite_affinity_follows_the_documented_rules() {
        assert_eq!(sqlite_affinity("INT"), SqliteAffinity::Integer);
        assert_eq!(sqlite_affinity("UNSIGNED BIG INT"), SqliteAffinity::Integer);
        assert_eq!(sqlite_affinity("POINT"), SqliteAffinity::Integer);
        assert_eq!(
            sqlite_affinity("VARYING CHARACTER(255)"),
            SqliteAffinity::Text
        );
        assert_eq!(
            sqlite_affinity("NATIVE CHARACTER(70)"),
            SqliteAffinity::Text
        );
        assert_eq!(sqlite_affinity("CLOB"), SqliteAffinity::Text);
        assert_eq!(sqlite_affinity(""), SqliteAffinity::Blob);
        assert_eq!(sqlite_affinity("BLOB"), SqliteAffinity::Blob);
        assert_eq!(sqlite_affinity("DOUBLE PRECISION"), SqliteAffinity::Real);
        assert_eq!(sqlite_affinity("FLOATING POINT"), SqliteAffinity::Integer); // contains INT
        assert_eq!(sqlite_affinity("DECIMAL(10,5)"), SqliteAffinity::Numeric);
        assert_eq!(sqlite_affinity("BLAH"), SqliteAffinity::Numeric);
    }

    #[test]
    fn sqlite_declared_types_from_the_corpus() {
        use NormalizedType::*;
        let cases = [
            ("INTEGER", Integer),
            ("BIGINT", Integer),
            ("NVARCHAR(160)", Text),
            ("VARCHAR", Text),
            ("TEXT", Text),
            ("REAL", Float),
            ("NUMERIC", Float),
            ("NUMERIC(10,2)", Float),
            ("DATE", Timestamp),
            ("DATETIME", Timestamp),
            ("TIMESTAMP", Timestamp),
            ("DOUBLE PRECISION", Float),
            ("BOOLEAN", Boolean),
            // affinity-only spellings
            ("UNSIGNED BIG INT", Integer),
            ("VARYING CHARACTER(255)", Text),
            ("NATIVE CHARACTER(70)", Text),
        ];
        for (raw, want) in cases {
            assert_eq!(
                NormalizedType::from_sqlite_declared(raw),
                want,
                "declared type {raw}"
            );
        }
    }

    #[test]
    fn sqlite_blob_and_numeric_affinity_stay_unknown() {
        assert_eq!(
            NormalizedType::from_sqlite_declared("BLOB"),
            NormalizedType::Unknown("blob".into())
        );
        // A column declared with no type at all: BLOB affinity, nothing to report.
        assert_eq!(
            NormalizedType::from_sqlite_declared(""),
            NormalizedType::Unknown(String::new())
        );
        // NUMERIC affinity: could be stored as integer, real or text.
        assert_eq!(
            NormalizedType::from_sqlite_declared("BLAH"),
            NormalizedType::Unknown("blah".into())
        );
    }
}
