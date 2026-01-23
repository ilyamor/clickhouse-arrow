use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use futures_util::TryStreamExt;
use tracing::debug;

/// Test that the factory creates connections and can execute queries
///
/// # Panics
pub async fn test_factory_basic_operations(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    // Create factory
    let factory = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build_arrow_factory()
        .await
        .expect("Failed to build factory");

    // Test scalar query
    let result: u64 = factory.scalar("SELECT 1", None).await.expect("Failed to query scalar");
    assert_eq!(result, 1);

    // Test query stream
    let stream = factory.query("SELECT number FROM system.numbers LIMIT 5", None).await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 5);

    eprintln!("Factory basic operations test passed");
}

/// Test that the factory can perform inserts
///
/// # Panics
pub async fn test_factory_insert(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    // Create factory
    let factory = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build_arrow_factory()
        .await
        .expect("Failed to build factory");

    // Create a test table with unique name
    let table_name = format!("test_factory_{}", Qid::new());
    let create_query =
        format!("CREATE TABLE {table_name} (id UInt64, value String) ENGINE = MergeTree() ORDER BY id");

    // Execute create table
    let stream = factory.query(&create_query, None).await.unwrap();
    drop(stream.try_collect::<Vec<_>>().await.unwrap());

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

    // Insert data
    let insert_query = format!("INSERT INTO {table_name} VALUES");
    factory.insert(&insert_query, batch, None).await.expect("Failed to insert");

    // Verify data was inserted
    let count: u64 = factory
        .scalar(&format!("SELECT count() FROM {table_name}"), None)
        .await
        .expect("Failed to count");
    assert_eq!(count, 3);

    // Query the data back
    let stream = factory
        .query(&format!("SELECT * FROM {table_name} ORDER BY id"), None)
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);

    // Clean up
    let drop_query = format!("DROP TABLE IF EXISTS {table_name}");
    let stream = factory.query(&drop_query, None).await.unwrap();
    drop(stream.try_collect::<Vec<_>>().await.unwrap());

    eprintln!("Factory insert test passed");
}

/// Test that multiple operations create separate connections
///
/// # Panics
pub async fn test_factory_multiple_connections(ch: Arc<ClickHouseContainer>) {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    // Create factory
    let factory = ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .build_arrow_factory()
        .await
        .expect("Failed to build factory");

    // Perform multiple queries - each should create a new connection
    for i in 0..5 {
        let result: u64 = factory
            .scalar(&format!("SELECT {i}"), None)
            .await
            .expect("Failed to query scalar");
        assert_eq!(result, i);
    }

    // Test concurrent queries
    let futures: Vec<_> = (0..3)
        .map(|i| {
            let factory = factory.clone();
            async move {
                let val = i + 10;
                let result: u64 = factory
                    .scalar(&format!("SELECT {val}"), None)
                    .await
                    .expect("Failed to query scalar");
                result
            }
        })
        .collect();

    let results = futures_util::future::join_all(futures).await;
    assert_eq!(results, vec![10, 11, 12]);

    eprintln!("Factory multiple connections test passed");
}
