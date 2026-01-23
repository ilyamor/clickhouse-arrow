//! One-shot query functions for simple, stateless ClickHouse operations.
//!
//! These functions provide the simplest possible API for ClickHouse operations:
//! connect, execute, return result, close. No client setup, no factories, no state.
//!
//! # Usage
//!
//! ```rust,ignore
//! use clickhouse_arrow::oneshot;
//!
//! // Simple query
//! let batches = oneshot::query("localhost:9000", "SELECT * FROM t").await?;
//!
//! // Get a single value
//! let count: u64 = oneshot::scalar("localhost:9000", "SELECT count() FROM t").await?;
//!
//! // Insert data
//! oneshot::insert("localhost:9000", "INSERT INTO t", batch).await?;
//! ```

use arrow::array::RecordBatch;
use futures_util::TryStreamExt;

use crate::client::{Client, ClientBuilder, ScalarValue};
use crate::formats::ArrowFormat;
use crate::Result;

/// Executes a query and returns all results.
///
/// This is the simplest way to query ClickHouse - no setup required.
/// Opens a connection, executes the query, collects results, closes connection.
///
/// # Parameters
/// - `endpoint`: ClickHouse endpoint (e.g., `"localhost:9000"`)
/// - `query`: SQL query to execute
///
/// # Returns
/// A vector of `RecordBatch` containing the query results.
///
/// # Errors
/// - Fails if the connection cannot be established
/// - Fails if the query execution fails
///
/// # Example
///
/// ```rust,ignore
/// let batches = clickhouse_arrow::oneshot::query(
///     "localhost:9000",
///     "SELECT * FROM system.numbers LIMIT 10"
/// ).await?;
/// ```
pub async fn query(endpoint: &str, query: &str) -> Result<Vec<RecordBatch>> {
    let client = connect(endpoint).await?;
    let stream = client.query(query, None).await?;
    stream.try_collect().await
}

/// Executes a query with credentials and returns all results.
///
/// # Parameters
/// - `endpoint`: ClickHouse endpoint (e.g., `"localhost:9000"`)
/// - `username`: Username for authentication
/// - `password`: Password for authentication
/// - `query`: SQL query to execute
///
/// # Returns
/// A vector of `RecordBatch` containing the query results.
pub async fn query_with_auth(
    endpoint: &str,
    username: &str,
    password: &str,
    query: &str,
) -> Result<Vec<RecordBatch>> {
    let client = connect_with_auth(endpoint, username, password).await?;
    let stream = client.query(query, None).await?;
    stream.try_collect().await
}

/// Executes a query and returns a single scalar value.
///
/// Useful for queries like `SELECT count() FROM table`.
///
/// # Parameters
/// - `endpoint`: ClickHouse endpoint (e.g., `"localhost:9000"`)
/// - `query`: SQL query that returns a single value
///
/// # Returns
/// The scalar value extracted from the first column of the first row.
///
/// # Errors
/// - Fails if the connection cannot be established
/// - Fails if the query execution fails
/// - Fails if no data is returned
/// - Fails if the type doesn't match
///
/// # Example
///
/// ```rust,ignore
/// let count: u64 = clickhouse_arrow::oneshot::scalar(
///     "localhost:9000",
///     "SELECT count() FROM system.numbers"
/// ).await?;
/// ```
pub async fn scalar<V: ScalarValue>(endpoint: &str, query: &str) -> Result<V> {
    let batches = self::query(endpoint, query).await?;

    let batch = batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .ok_or(crate::Error::NoData)?;

    V::extract(batch.column(0), 0)
}

/// Executes a query with credentials and returns a single scalar value.
pub async fn scalar_with_auth<V: ScalarValue>(
    endpoint: &str,
    username: &str,
    password: &str,
    query: &str,
) -> Result<V> {
    let batches = query_with_auth(endpoint, username, password, query).await?;

    let batch = batches
        .into_iter()
        .find(|b| b.num_rows() > 0)
        .ok_or(crate::Error::NoData)?;

    V::extract(batch.column(0), 0)
}

/// Inserts a RecordBatch into ClickHouse.
///
/// # Parameters
/// - `endpoint`: ClickHouse endpoint (e.g., `"localhost:9000"`)
/// - `query`: Insert query (e.g., `"INSERT INTO my_table"`)
/// - `data`: The RecordBatch to insert
///
/// # Errors
/// - Fails if the connection cannot be established
/// - Fails if the insert operation fails
///
/// # Example
///
/// ```rust,ignore
/// clickhouse_arrow::oneshot::insert(
///     "localhost:9000",
///     "INSERT INTO my_table",
///     batch
/// ).await?;
/// ```
pub async fn insert(endpoint: &str, query: &str, data: RecordBatch) -> Result<()> {
    let client = connect(endpoint).await?;
    let stream = client.insert(query, data, None).await?;
    tokio::pin!(stream);
    while let Some(result) = futures_util::StreamExt::next(&mut stream).await {
        result?;
    }
    Ok(())
}

/// Inserts a RecordBatch with credentials.
pub async fn insert_with_auth(
    endpoint: &str,
    username: &str,
    password: &str,
    query: &str,
    data: RecordBatch,
) -> Result<()> {
    let client = connect_with_auth(endpoint, username, password).await?;
    let stream = client.insert(query, data, None).await?;
    tokio::pin!(stream);
    while let Some(result) = futures_util::StreamExt::next(&mut stream).await {
        result?;
    }
    Ok(())
}

/// Inserts multiple RecordBatches into ClickHouse.
pub async fn insert_many(endpoint: &str, query: &str, data: Vec<RecordBatch>) -> Result<()> {
    let client = connect(endpoint).await?;
    let stream = client.insert_many(query, data, None).await?;
    tokio::pin!(stream);
    while let Some(result) = futures_util::StreamExt::next(&mut stream).await {
        result?;
    }
    Ok(())
}

/// Executes a statement without returning results (CREATE, DROP, etc).
///
/// # Example
///
/// ```rust,ignore
/// clickhouse_arrow::oneshot::execute(
///     "localhost:9000",
///     "CREATE TABLE t (id UInt64) ENGINE = Memory"
/// ).await?;
/// ```
pub async fn execute(endpoint: &str, query: &str) -> Result<()> {
    let client = connect(endpoint).await?;
    client.execute(query, None).await
}

/// Executes a statement with credentials.
pub async fn execute_with_auth(
    endpoint: &str,
    username: &str,
    password: &str,
    query: &str,
) -> Result<()> {
    let client = connect_with_auth(endpoint, username, password).await?;
    client.execute(query, None).await
}

// Internal: create a minimal client connection
async fn connect(endpoint: &str) -> Result<Client<ArrowFormat>> {
    ClientBuilder::new()
        .with_endpoint(endpoint)
        .with_ext(|mut ext| {
            ext.fast_mode_size = Some(1); // Single connection, no pool
            ext
        })
        .build_arrow()
        .await
}

async fn connect_with_auth(
    endpoint: &str,
    username: &str,
    password: &str,
) -> Result<Client<ArrowFormat>> {
    ClientBuilder::new()
        .with_endpoint(endpoint)
        .with_username(username)
        .with_password(password)
        .with_ext(|mut ext| {
            ext.fast_mode_size = Some(1);
            ext
        })
        .build_arrow()
        .await
}
