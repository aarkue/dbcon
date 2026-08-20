//! Several single-table files presented as one multi-table source.

#![cfg(feature = "csv")]

use dbcon::{DataSource, SourceData, TableSetSource};

fn bytes(s: &str) -> SourceData {
    SourceData::Memory(s.as_bytes().to_vec().into())
}

fn orders_and_items() -> TableSetSource {
    let mut set = TableSetSource::default();
    set.insert_csv("orders", bytes("order_id,total\no1,10\no2,20\n"));
    set.insert_csv("order items", bytes("order_id;sku\no1;a\no1;b\no2;c\n"));
    set
}

#[test]
fn every_member_becomes_a_table_under_the_name_the_caller_gave_it() {
    let source = DataSource::new_table_set("bundle".to_string(), orders_and_items()).expect("open");

    let mut names: Vec<_> = source.tables.keys().cloned().collect();
    names.sort();
    assert_eq!(names, ["order items", "orders"]);
    // Not `main`, which is what each member calls its own single table.
    assert_eq!(source.tables["orders"].name, "orders");
    assert!(source.tables["orders"].columns.contains_key("total"));
    assert!(source.tables["order items"].columns.contains_key("sku"));
}

/// Each member keeps its own delimiter: one comma-separated, one semicolon-separated.
#[test]
fn rows_are_read_from_the_file_the_named_table_maps_to() {
    let source = DataSource::new_table_set("bundle".to_string(), orders_and_items()).expect("open");

    let orders = source.get_first_rows("orders", 10).expect("orders");
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0]["order_id"], "o1");

    let items = source.get_first_rows("order items", 10).expect("items");
    assert_eq!(items.len(), 3);
    assert_eq!(items[2]["sku"], "c");

    let skus = source
        .get_distinct_values("order items", "order_id")
        .expect("distinct");
    assert_eq!(skus.len(), 2, "{skus:?}");
}

#[test]
fn an_unknown_table_is_an_error_naming_it() {
    let source = DataSource::new_table_set("bundle".to_string(), orders_and_items()).expect("open");
    let err = source
        .get_first_rows("customers", 1)
        .expect_err("no such table");
    assert!(err.to_string().contains("customers"), "{err}");
}
