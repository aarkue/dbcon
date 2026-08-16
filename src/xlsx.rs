//! The XLSX backend, on `calamine`.
//!
//! # One table per sheet
//!
//! Unlike [`CSVSource`](crate::CSVSource) and `ParquetSource`, which are each one table under a
//! fixed name because their format carries no schema, a workbook names its sheets. Each sheet is a
//! table under its own name, which makes this the only file-backed source here with a real
//! multi-table schema and the reason it needs no
//! [`TableSetSource`](crate::TableSetSource) around it.
//!
//! # Columns are text, values are typed
//!
//! Column *types* are not inferred. Every column is reported as nullable `Text`, exactly as the CSV
//! backend does and for the same reason: a spreadsheet declares nothing, so any type would have to
//! come from sampling rows, and a column that looks like an integer for a thousand rows and holds
//! `"n/a"` on row 1001 would then be described wrongly. The compiler decides literal coercion and
//! identity rendering from declared types, so a wrong declaration is worse than a vague one.
//!
//! Individual *values* still carry their own type, the way SQLite's do: a cell that is a number is
//! read as a number. This is the pattern [`mod@crate::sqlite`] already documents -- the declared
//! type is a re-tag, the value in hand is the truth.
//!
//! # Dates
//!
//! An Excel date cell holds a *serial number* -- days since 1899-12-30 -- and is a date only
//! because a number format says so. Read naively it is a five-digit integer, and nothing errors:
//! `2024-03-04` silently becomes `45355`. `calamine`'s `dates` feature resolves the format, and
//! [`decode`] uses it, so a formatted date arrives as a [`NormalizedValue::Timestamp`].
//!
//! The date test is the cell's *variant*, never `DataType::as_datetime`. That method converts any
//! number as if it were a serial, which is the same bug pointed the other way: `42` would become
//! 1900-02-11 and a quantity column would silently turn into dates.
//!
//! Excel's 1900 leap-year bug (serial 60 is a 29 February 1900 that never existed) is handled
//! inside `calamine`'s conversion; dates before 1900-03-01 are the only region where a serial and a
//! real date disagree, and it is not this crate's job to re-derive that.
//!
//! # What this does not try to be
//!
//! A real spreadsheet is often not a table: title rows above the header, merged cells, several
//! tables on one sheet, notes underneath. The header is taken as the first non-empty row of a
//! sheet and everything below it as data. A workbook that does not fit that shape is out of scope
//! here rather than guessed at.

use std::collections::HashMap;
use std::io::Cursor;
use std::ops::ControlFlow;

use calamine::{Data, Reader, Xlsx};

use crate::{DataColumnInfo, DataTableInfo, NormalizedType, NormalizedValue, SourceData};

/// A workbook, read from a path or from bytes.
#[derive(Debug, Clone)]
pub struct XlsxSource {
    pub data: SourceData,
}

impl XlsxSource {
    /// A workbook at `path`.
    pub fn from_path(path: impl Into<String>) -> Self {
        Self {
            data: SourceData::Path(path.into()),
        }
    }

    /// A workbook held in memory. The route a browser `File` and a `wasm32` build take, neither of
    /// which has a path to open.
    pub fn from_bytes(bytes: impl Into<std::sync::Arc<[u8]>>) -> Self {
        Self {
            data: SourceData::Memory(bytes.into()),
        }
    }

    /// Open the workbook.
    ///
    /// Every read opens afresh, like the CSV backend's `reader`: `calamine` holds the whole
    /// decompressed sheet, and a source is read several times over its life (schema, scan,
    /// distinct values, preview).
    ///
    /// The bytes are read into memory either way. A zip needs `Seek`, so there is no streaming
    /// form to preserve, and `SourceData::reader` hands back a non-`Seek` reader.
    fn workbook(&self) -> anyhow::Result<Xlsx<Cursor<Vec<u8>>>> {
        let bytes = match &self.data {
            SourceData::Path(p) => std::fs::read(p)
                .map_err(|e| anyhow::anyhow!("could not read the workbook at {p}: {e}"))?,
            SourceData::Memory(bytes) => bytes.to_vec(),
        };
        Xlsx::new(Cursor::new(bytes)).map_err(|e| {
            anyhow::anyhow!(
                "{} is not a readable xlsx workbook: {e}",
                self.data.describe()
            )
        })
    }

    /// Sheet names, in workbook order.
    pub fn sheet_names(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.workbook()?.sheet_names().to_vec())
    }

    /// The header row of `sheet`, and the rows below it.
    ///
    /// Leading fully-empty rows are skipped, so a sheet with a blank line above the header still
    /// reads. Beyond that no layout is guessed at -- see this module's header.
    fn sheet(&self, sheet: &str) -> anyhow::Result<(Vec<String>, Vec<Vec<Data>>)> {
        let mut workbook = self.workbook()?;
        let range = workbook
            .worksheet_range(sheet)
            .map_err(|e| anyhow::anyhow!("no sheet {sheet:?} in {}: {e}", self.data.describe()))?;

        let mut rows = range.rows().skip_while(|row| row.iter().all(is_blank));
        let Some(header_row) = rows.next() else {
            return Ok((Vec::new(), Vec::new()));
        };
        // A blank header cell would otherwise produce an unaddressable column, so it is named by
        // position instead -- the same thing a spreadsheet UI shows in its column bar.
        let header: Vec<String> = header_row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let name = cell.to_string().trim().to_string();
                if name.is_empty() {
                    column_label(i)
                } else {
                    name
                }
            })
            .collect();
        Ok((header, rows.map(<[Data]>::to_vec).collect()))
    }

    /// Every sheet as a table of nullable text columns. See this module's header for why the types
    /// are not inferred.
    pub fn get_tables(&self) -> anyhow::Result<HashMap<String, DataTableInfo>> {
        let mut tables = HashMap::new();
        for sheet in self.sheet_names()? {
            let (header, _) = self.sheet(&sheet)?;
            let columns = header
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        DataColumnInfo {
                            name: name.clone(),
                            col_type: NormalizedType::Text,
                            is_nullable: true,
                        },
                    )
                })
                .collect();
            tables.insert(
                sheet.clone(),
                DataTableInfo {
                    name: sheet,
                    columns,
                    // A workbook declares no keys and no references. Inventing them from column
                    // names ("id", "*_id") is the kind of guess that silently rewires a blueprint.
                    primary_keys: Vec::new(),
                    foreign_keys: Vec::new(),
                },
            );
        }
        Ok(tables)
    }

    /// Rows of `sheet`, projected onto `columns`, calling `handler` once per row.
    ///
    /// A column the sheet does not have is an error rather than a column of nulls, matching the
    /// CSV backend: an all-null column is what a typo looks like, and nothing else reports it.
    /// A row that simply stops short of a column it does have still yields
    /// [`NormalizedValue::Null`], which is the CSV backend's rule for a short row.
    pub fn for_each<C: AsRef<str>>(
        &self,
        sheet: &str,
        columns: &[C],
        handler: &mut dyn FnMut(&[NormalizedValue]) -> ControlFlow<()>,
    ) -> anyhow::Result<()> {
        let (header, rows) = self.sheet(sheet)?;
        let indices: Vec<usize> = columns
            .iter()
            .map(|want| {
                header
                    .iter()
                    .position(|h| h == want.as_ref())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Column '{}' not found in sheet '{sheet}'",
                            want.as_ref()
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut buffer = vec![NormalizedValue::Null; columns.len()];
        for row in rows {
            for (slot, &index) in buffer.iter_mut().zip(&indices) {
                *slot = row.get(index).map_or(NormalizedValue::Null, decode);
            }
            if handler(&buffer).is_break() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// At most `limit` rows of `sheet` projected onto `columns`.
    pub fn rows<C: AsRef<str>>(
        &self,
        sheet: &str,
        columns: &[C],
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<Vec<NormalizedValue>>> {
        let mut out = Vec::new();
        self.for_each(sheet, columns, &mut |row| {
            out.push(row.to_vec());
            match limit {
                Some(limit) if out.len() >= limit => ControlFlow::Break(()),
                _ => ControlFlow::Continue(()),
            }
        })?;
        Ok(out)
    }

    /// Distinct values of `column` in `sheet`, as text, skipping empties.
    pub fn distinct_values(&self, sheet: &str, column: &str) -> anyhow::Result<Vec<String>> {
        let mut seen = std::collections::HashSet::new();
        let cols = vec![column.to_string()];
        self.for_each(sheet, &cols, &mut |row| {
            if !matches!(row[0], NormalizedValue::Null) {
                let text = row[0].to_string();
                if !text.is_empty() {
                    seen.insert(text);
                }
            }
            ControlFlow::Continue(())
        })?;
        let mut out: Vec<String> = seen.into_iter().collect();
        out.sort();
        Ok(out)
    }

    /// At most `limit` rows of `sheet` as `{column name -> text}`, for the preview API.
    pub fn named_string_rows(
        &self,
        sheet: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<Vec<HashMap<String, String>>> {
        let (header, _) = self.sheet(sheet)?;
        let rows = self.rows(sheet, &header, limit)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                header
                    .iter()
                    .cloned()
                    .zip(row.into_iter().map(|v| v.to_string()))
                    .collect()
            })
            .collect())
    }
}

/// Whether a cell contributes nothing to a row. Matched on the variant rather than on
/// `to_string()`, which allocates for every cell of every leading row of every sheet.
fn is_blank(cell: &Data) -> bool {
    match cell {
        Data::Empty => true,
        Data::String(s) | Data::DateTimeIso(s) | Data::DurationIso(s) => s.trim().is_empty(),
        _ => false,
    }
}

/// A spreadsheet column label for a headerless column: `A`, `B`, ... `Z`, `AA`.
fn column_label(index: usize) -> String {
    let mut label = String::new();
    let mut n = index as i64;
    while n >= 0 {
        label.insert(0, (b'A' + (n % 26) as u8) as char);
        n = n / 26 - 1;
    }
    label
}

/// One cell as a [`NormalizedValue`]. See this module's header for the rule on dates.
pub(crate) fn decode(cell: &Data) -> NormalizedValue {
    match cell {
        Data::Empty => NormalizedValue::Null,
        Data::String(s) => NormalizedValue::Text(s.clone()),
        Data::Int(i) => NormalizedValue::Integer(*i),
        Data::Float(f) => {
            // A spreadsheet has one numeric type, so a whole-valued cell is reported as an integer
            // rather than as `5.0` -- which is what the user typed and what an id column needs to
            // render as, since `"5" != "5.0"` once an identity is built from it.
            if f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 {
                NormalizedValue::Integer(*f as i64)
            } else {
                NormalizedValue::Float(*f)
            }
        }
        Data::Bool(b) => NormalizedValue::Boolean(*b),
        // Format-marked, so this is the one place a serial becomes an instant. `as_datetime`
        // carries Excel's 1900 leap-year quirk, which is why the conversion is not redone here.
        Data::DateTime(dt) => dt
            .as_datetime()
            .map_or(NormalizedValue::Null, |d| {
                NormalizedValue::Timestamp(d.and_utc().fixed_offset())
            }),
        // Already a datetime string in the file. Parsed when it parses, kept verbatim when it does
        // not, since a cell this cascade cannot read still carries what the writer wrote.
        Data::DateTimeIso(s) => crate::types::parse_timestamp(s)
            .map_or_else(|| NormalizedValue::Text(s.clone()), NormalizedValue::Timestamp),
        // A duration is not an instant, so it stays text.
        Data::DurationIso(s) => NormalizedValue::Text(s.clone()),
        // `#REF!`, `#N/A` and friends. Carried as text rather than as null: a formula that failed
        // is not the same as a blank cell, and hiding it would make the two indistinguishable.
        Data::Error(e) => NormalizedValue::Unknown(format!("{e:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A workbook written with `rust_xlsxwriter` would add a dependency for the tests alone, so
    /// the fixtures are built as the minimal OOXML a reader accepts.
    fn workbook(sheets: &[(&str, &[&[&str]])]) -> Vec<u8> {
        fn esc(s: &str) -> String {
            s.replace('&', "&amp;").replace('<', "&lt;")
        }
        fn col(i: usize) -> String {
            super::column_label(i)
        }
        let mut parts: Vec<(String, String)> = Vec::new();
        let mut sheet_entries = String::new();
        let mut rels = String::new();
        let mut overrides = String::new();
        for (n, (name, rows)) in sheets.iter().enumerate() {
            let id = n + 1;
            sheet_entries.push_str(&format!(
                r#"<sheet name="{}" sheetId="{id}" r:id="rId{id}"/>"#,
                esc(name)
            ));
            rels.push_str(&format!(
                r#"<Relationship Id="rId{id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{id}.xml"/>"#
            ));
            overrides.push_str(&format!(
                r#"<Override PartName="/xl/worksheets/sheet{id}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>"#
            ));
            let body: String = rows
                .iter()
                .enumerate()
                .map(|(r, row)| {
                    let cells: String = row
                        .iter()
                        .enumerate()
                        .map(|(c, v)| {
                            let re = format!("{}{}", col(c), r + 1);
                            if v.is_empty() {
                                String::new()
                            } else if v.parse::<f64>().is_ok() {
                                format!(r#"<c r="{re}"><v>{v}</v></c>"#)
                            } else {
                                format!(
                                    r#"<c r="{re}" t="inlineStr"><is><t>{}</t></is></c>"#,
                                    esc(v)
                                )
                            }
                        })
                        .collect();
                    format!(r#"<row r="{}">{cells}</row>"#, r + 1)
                })
                .collect();
            parts.push((
                format!("xl/worksheets/sheet{id}.xml"),
                format!(
                    r#"<?xml version="1.0"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>{body}</sheetData></worksheet>"#
                ),
            ));
        }
        parts.push((
            "[Content_Types].xml".into(),
            format!(
                r#"<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>{overrides}</Types>"#
            ),
        ));
        parts.push((
            "_rels/.rels".into(),
            r#"<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#.into(),
        ));
        parts.push((
            "xl/workbook.xml".into(),
            format!(
                r#"<?xml version="1.0"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>{sheet_entries}</sheets></workbook>"#
            ),
        ));
        parts.push((
            "xl/_rels/workbook.xml.rels".into(),
            format!(
                r#"<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">{rels}</Relationships>"#
            ),
        ));
        zip(&parts)
    }

    /// A stored (uncompressed) zip, so the fixture needs no deflate implementation.
    fn zip(parts: &[(String, String)]) -> Vec<u8> {
        fn crc32(bytes: &[u8]) -> u32 {
            let mut c: u32 = 0xffff_ffff;
            for &b in bytes {
                c ^= u32::from(b);
                for _ in 0..8 {
                    c = if c & 1 == 1 { 0xedb8_8320 ^ (c >> 1) } else { c >> 1 };
                }
            }
            c ^ 0xffff_ffff
        }
        let mut out = Vec::new();
        let mut directory = Vec::new();
        for (name, body) in parts {
            let offset = out.len() as u32;
            let data = body.as_bytes();
            let crc = crc32(data);
            let (n, len) = (name.len() as u16, data.len() as u32);
            out.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
            out.extend_from_slice(&[20, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&n.to_le_bytes());
            out.extend_from_slice(&0u16.to_le_bytes());
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(data);

            directory.extend_from_slice(&0x0201_4b50u32.to_le_bytes());
            directory.extend_from_slice(&[20, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            directory.extend_from_slice(&crc.to_le_bytes());
            directory.extend_from_slice(&len.to_le_bytes());
            directory.extend_from_slice(&len.to_le_bytes());
            directory.extend_from_slice(&n.to_le_bytes());
            directory.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            directory.extend_from_slice(&offset.to_le_bytes());
            directory.extend_from_slice(name.as_bytes());
        }
        let (start, size) = (out.len() as u32, directory.len() as u32);
        out.extend_from_slice(&directory);
        out.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
        out.extend_from_slice(&[0, 0, 0, 0]);
        let count = parts.len() as u16;
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&count.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out
    }

    fn source(sheets: &[(&str, &[&[&str]])]) -> XlsxSource {
        XlsxSource::from_bytes(workbook(sheets))
    }

    #[test]
    fn column_labels_follow_the_spreadsheet_column_bar() {
        assert_eq!(column_label(0), "A");
        assert_eq!(column_label(25), "Z");
        assert_eq!(column_label(26), "AA");
        assert_eq!(column_label(27), "AB");
    }

    /// The structural difference from CSV and Parquet: sheets are tables.
    #[test]
    fn every_sheet_becomes_its_own_table() {
        let s = source(&[
            ("orders", &[&["id", "total"], &["1", "9.5"]]),
            ("customers", &[&["id", "name"], &["1", "ada"]]),
        ]);
        let tables = s.get_tables().expect("schema reads");
        let mut names: Vec<&String> = tables.keys().collect();
        names.sort();
        assert_eq!(names, vec!["customers", "orders"]);
        assert_eq!(tables["orders"].columns.len(), 2);
        assert!(tables["orders"].columns.contains_key("total"));
    }

    /// No inference: a column of numbers is still declared text, exactly as CSV does.
    #[test]
    fn columns_are_declared_text_and_nullable_whatever_they_hold() {
        let s = source(&[("t", &[&["n"], &["1"], &["2"]])]);
        let tables = s.get_tables().expect("schema reads");
        let col = &tables["t"].columns["n"];
        assert_eq!(col.col_type, NormalizedType::Text);
        assert!(col.is_nullable);
    }

    /// A workbook declares neither, and guessing from column names would silently rewire joins.
    #[test]
    fn no_keys_are_invented() {
        let s = source(&[("t", &[&["id", "other_id"], &["1", "2"]])]);
        let tables = s.get_tables().expect("schema reads");
        assert!(tables["t"].primary_keys.is_empty());
        assert!(tables["t"].foreign_keys.is_empty());
    }

    #[test]
    fn values_keep_their_own_types_even_though_the_column_is_text() {
        let s = source(&[("t", &[&["a", "b", "c"], &["ada", "42", "1.5"]])]);
        let cols: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let rows = s.rows("t", &cols, None).expect("rows read");
        assert_eq!(
            rows[0],
            vec![
                NormalizedValue::Text("ada".into()),
                NormalizedValue::Integer(42),
                NormalizedValue::Float(1.5),
            ]
        );
    }

    /// A whole-valued cell is an integer, not `5.0`: a spreadsheet has one numeric type, and an id
    /// built from `5.0` is a different string than one built from `5`.
    #[test]
    fn a_whole_number_does_not_come_back_with_a_decimal_point() {
        let s = source(&[("t", &[&["id"], &["5"]])]);
        let rows = s
            .rows("t", &["id".to_string()], None)
            .expect("rows read");
        assert_eq!(rows[0][0], NormalizedValue::Integer(5));
        assert_eq!(rows[0][0].to_string(), "5");
    }

    /// A column of nulls is what a typo'd column name would look like, and nothing else would
    /// report it.
    #[test]
    fn a_column_the_sheet_does_not_have_is_an_error() {
        let s = source(&[("t", &[&["a"], &["1"]])]);
        let err = s
            .rows("t", &["a".to_string(), "nope".to_string()], None)
            .expect_err("a missing column is refused");
        assert!(err.to_string().contains("nope"), "got: {err}");
    }

    /// A row that stops short of a column the sheet does have is not a missing column.
    #[test]
    fn a_short_row_still_yields_null() {
        let s = source(&[("t", &[&["a", "b"], &["1"]])]);
        let rows = s
            .rows("t", &["a".to_string(), "b".to_string()], None)
            .expect("rows read");
        assert_eq!(rows[0][1], NormalizedValue::Null);
    }

    #[test]
    fn leading_blank_rows_above_the_header_are_skipped() {
        let s = source(&[("t", &[&["", ""], &["id", "name"], &["1", "ada"]])]);
        let tables = s.get_tables().expect("schema reads");
        assert!(
            tables["t"].columns.contains_key("id"),
            "header not found: {:?}",
            tables["t"].columns.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_headerless_column_is_named_by_its_position() {
        let s = source(&[("t", &[&["id", ""], &["1", "x"]])]);
        let tables = s.get_tables().expect("schema reads");
        assert!(
            tables["t"].columns.contains_key("B"),
            "{:?}",
            tables["t"].columns.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_scan_stops_when_the_handler_breaks() {
        let s = source(&[("t", &[&["n"], &["1"], &["2"], &["3"], &["4"]])]);
        let mut seen = 0;
        s.for_each("t", &["n".to_string()], &mut |_| {
            seen += 1;
            if seen == 2 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .expect("scan runs");
        assert_eq!(seen, 2);
    }

    #[test]
    fn distinct_values_are_deduplicated_text() {
        let s = source(&[("t", &[&["c"], &["a"], &["b"], &["a"]])]);
        let values = s.distinct_values("t", "c").expect("distinct reads");
        assert_eq!(values, vec!["a", "b"]);
    }

    /// The fixture writes bare numbers with no date format, so every one of these must stay a
    /// number. See this module's header for why.
    #[test]
    fn plain_numbers_are_never_reinterpreted_as_dates() {
        let s = source(&[("t", &[&["qty", "price"], &["42", "45355"]])]);
        let cols = ["qty".to_string(), "price".to_string()];
        let rows = s.rows("t", &cols, None).expect("rows read");
        assert_eq!(
            rows[0],
            vec![NormalizedValue::Integer(42), NormalizedValue::Integer(45355)],
            "a number with no date format must not become a date"
        );
    }

    /// The conversion itself, exercised directly: a format-marked cell is the only thing that
    /// becomes an instant, and it lands on the day the serial names.
    #[test]
    fn a_format_marked_date_cell_becomes_the_instant_it_names() {
        // 45355 days from 1899-12-30, the epoch Excel counts from.
        let cell = Data::DateTime(calamine::ExcelDateTime::new(
            45355.0,
            calamine::ExcelDateTimeType::DateTime,
            false,
        ));
        let NormalizedValue::Timestamp(ts) = decode(&cell) else {
            panic!("expected a timestamp, got {:?}", decode(&cell));
        };
        assert_eq!(ts.to_utc().date_naive().to_string(), "2024-03-04");
    }

    /// A fractional second, a zone suffix and a bare date are all spellings a writer emits, and
    /// none of them is the one exact format.
    #[test]
    fn an_iso_datetime_cell_reads_in_every_shape_it_is_written_in() {
        for text in [
            "2024-03-04T05:06:07",
            "2024-03-04T05:06:07.25",
            "2024-03-04T05:06:07Z",
            "2024-03-04T05:06:07+02:00",
            "2024-03-04",
        ] {
            let cell = Data::DateTimeIso(text.to_string());
            assert!(
                matches!(decode(&cell), NormalizedValue::Timestamp(_)),
                "{text} did not read as a timestamp: {:?}",
                decode(&cell)
            );
        }
    }

    #[test]
    fn an_unreadable_workbook_is_an_error_naming_the_source() {
        let s = XlsxSource::from_bytes(b"not a zip".to_vec());
        let err = s.get_tables().expect_err("garbage is refused");
        assert!(err.to_string().contains("xlsx"), "got: {err}");
    }
}
