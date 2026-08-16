//! Schema discovery, owned by dbcon.
//!
//! Each SQL backend answers four questions from its own catalog - which tables exist,
//! what columns they have, which columns form keys, and which columns reference other
//! tables - and dbcon converts the answers into [`DataTableInfo`](crate::DataTableInfo).
//! There is no cross-dialect schema abstraction in between: the dialect-specific part is
//! the catalog query, and the only shared logic is the type mapping in
//! [`crate::types`].
//!
//! Adding a backend means adding a module here with a `discover` function; nothing else
//! in the crate needs to know about it beyond one dispatch arm.

#[cfg(feature = "postgres")]
pub(crate) mod postgres;
#[cfg(feature = "duckdb")]
pub(crate) mod duckdb;
#[cfg(feature = "sqlite")]
pub(crate) mod sqlite;
