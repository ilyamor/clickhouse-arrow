use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use clickhouse_arrow::oneshot;
use clickhouse_arrow::prelude::Qid;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use tracing::debug;

/// Test oneshot query
///
/// # Panics
pub async fn test_oneshot_query(ch: Arc<ClickHouseContainer>) {
    let endpoint = ch.get_native_url();
    debug!("ClickHouse endpoint: {endpoint}");

    let batches = oneshot::query_with_auth(
        &endpoint,
        &ch.user,
        &ch.password,
        "SELECT number FROM system.numbers LIMIT 5",
    )
    .await
    .expect("query failed");

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 5);

    eprintln!("Oneshot query test passed");
}

/// Test oneshot scalar
///
/// # Panics
pub async fn test_oneshot_scalar(ch: Arc<ClickHouseContainer>) {
    let endpoint = ch.get_native_url();
    debug!("ClickHouse endpoint: {endpoint}");

    let result: u64 =
        oneshot::scalar_with_auth(&endpoint, &ch.user, &ch.password, "SELECT 42")
            .await
            .expect("scalar failed");
    assert_eq!(result, 42);

    eprintln!("Oneshot scalar test passed");
}

/// Test oneshot insert
///
/// # Panics
pub async fn test_oneshot_insert(ch: Arc<ClickHouseContainer>) {
    let endpoint = ch.get_native_url();
    debug!("ClickHouse endpoint: {endpoint}");

    // Create table
    let table_name = format!("test_oneshot_{}", Qid::new());
    oneshot::execute_with_auth(
        &endpoint,
        &ch.user,
        &ch.password,
        &format!("CREATE TABLE {table_name} (id UInt64, value String) ENGINE = MergeTree() ORDER BY id"),
    )
    .await
    .expect("create table failed");

    // Create test data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["one", "two", "three"])),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Insert
    oneshot::insert_with_auth(
        &endpoint,
        &ch.user,
        &ch.password,
        &format!("INSERT INTO {table_name} VALUES"),
        batch,
    )
    .await
    .expect("insert failed");

    // Verify
    let count: u64 = oneshot::scalar_with_auth(
        &endpoint,
        &ch.user,
        &ch.password,
        &format!("SELECT count() FROM {table_name}"),
    )
    .await
    .expect("count failed");
    assert_eq!(count, 3);

    // Cleanup
    drop(
        oneshot::execute_with_auth(
            &endpoint,
            &ch.user,
            &ch.password,
            &format!("DROP TABLE {table_name}"),
        )
        .await,
    );

    eprintln!("Oneshot insert test passed");
}
