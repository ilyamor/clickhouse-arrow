//! A factory for creating fresh connections per operation.
//!
//! This module provides [`ClientFactory<T>`], which creates a new TCP connection
//! for each query or insert operation. This is useful for scenarios where you want
//! to ensure complete isolation between operations or when working with load balancers
//! that benefit from connection rotation.
//!
//! # Usage
//!
//! ```rust,ignore
//! use clickhouse_arrow::prelude::*;
//! use futures_util::TryStreamExt;
//!
//! // Create factory (verifies connection parameters once)
//! let factory = ClientBuilder::new()
//!     .with_endpoint("localhost:9000")
//!     .with_username("default")
//!     .build_arrow_factory()
//!     .await?;
//!
//! // Each query creates a fresh TCP connection
//! let stream = factory.query("SELECT * FROM my_table", None).await?;
//! let results: Vec<RecordBatch> = stream.try_collect().await?;
//! // Connection is closed when stream is dropped
//! ```

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use futures_util::{Stream, StreamExt, TryStreamExt};
use pin_project::pin_project;

use super::builder::ClientBuilder;
use super::response::ClickHouseResponse;
use super::Client;
use crate::formats::{ArrowFormat, ClientFormat, NativeFormat};
use crate::native::block::Block;
use crate::query::ParsedQuery;
use crate::Result;

/// Type alias for an Arrow-based client factory.
pub type ArrowClientFactory = ClientFactory<ArrowFormat>;

/// Type alias for a Native-based client factory.
pub type NativeClientFactory = ClientFactory<NativeFormat>;

/// A factory that creates fresh TCP connections for each operation.
///
/// Unlike [`Client`], which maintains a persistent connection, `ClientFactory`
/// creates a new connection for each `query`, `insert`, or `insert_many` call.
/// This is useful for:
///
/// - Working with load balancers that benefit from connection rotation
/// - Ensuring complete isolation between operations
/// - Scenarios where connection pooling is handled externally
///
/// The factory validates the connection parameters once during creation,
/// so subsequent operations can create connections without re-validating.
///
/// # Examples
///
/// ```rust,ignore
/// use clickhouse_arrow::prelude::*;
/// use futures_util::TryStreamExt;
///
/// let factory = ClientBuilder::new()
///     .with_endpoint("localhost:9000")
///     .with_username("default")
///     .build_arrow_factory()
///     .await?;
///
/// // Each operation creates a fresh connection
/// let stream = factory.query("SELECT 1", None).await?;
/// let batches: Vec<_> = stream.try_collect().await?;
/// ```
#[derive(Debug, Clone)]
pub struct ClientFactory<T: ClientFormat> {
    builder: ClientBuilder,
    _phantom: PhantomData<T>,
}

impl<T: ClientFormat> ClientFactory<T> {
    /// Creates a new `ClientFactory` from a verified `ClientBuilder`.
    ///
    /// The builder is verified to ensure the destination is valid before
    /// the factory is created. This allows subsequent operations to create
    /// connections without re-validating the destination.
    ///
    /// # Parameters
    /// - `builder`: A `ClientBuilder` with connection parameters configured.
    ///
    /// # Returns
    /// A `Result` containing the `ClientFactory`, or an error if verification fails.
    ///
    /// # Errors
    /// - Fails if the builder's destination cannot be verified.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let builder = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .with_username("default");
    ///
    /// let factory = ClientFactory::<ArrowFormat>::new(builder).await?;
    /// ```
    pub async fn new(builder: ClientBuilder) -> Result<Self> {
        // Verify the builder to ensure destination is valid
        let verified_builder = if builder.verified() {
            builder
        } else {
            builder.verify().await?
        };

        Ok(Self { builder: verified_builder, _phantom: PhantomData })
    }

    /// Creates a fresh connection and returns the client.
    async fn create_client(&self) -> Result<Client<T>> {
        self.builder.clone().build::<T>().await
    }

    /// Executes an insert query with a single data block using a fresh connection.
    ///
    /// This method creates a new TCP connection, sends the insert query with data,
    /// and waits for the operation to complete. The connection is closed after
    /// the insert is finished.
    ///
    /// # Parameters
    /// - `query`: The insert query (e.g., `"INSERT INTO my_table VALUES"`).
    /// - `block`: The data to insert.
    /// - `qid`: Optional query ID for tracking and debugging.
    ///
    /// # Returns
    /// A `Result` indicating success or failure of the insert operation.
    ///
    /// # Errors
    /// - Fails if the connection cannot be established.
    /// - Fails if the insert operation fails.
    pub async fn insert(
        &self,
        query: impl Into<ParsedQuery>,
        block: T::Data,
        qid: Option<crate::Qid>,
    ) -> Result<()> {
        let client = self.create_client().await?;
        let stream = client.insert(query, block, qid).await?;
        // Drain the stream to completion
        tokio::pin!(stream);
        while let Some(result) = stream.next().await {
            result?;
        }
        Ok(())
    }

    /// Executes an insert query with multiple data blocks using a fresh connection.
    ///
    /// This method creates a new TCP connection, sends the insert query with all
    /// data blocks, and waits for the operation to complete. The connection is
    /// closed after the insert is finished.
    ///
    /// # Parameters
    /// - `query`: The insert query (e.g., `"INSERT INTO my_table VALUES"`).
    /// - `blocks`: A vector of data blocks to insert.
    /// - `qid`: Optional query ID for tracking and debugging.
    ///
    /// # Returns
    /// A `Result` indicating success or failure of the insert operation.
    ///
    /// # Errors
    /// - Fails if the connection cannot be established.
    /// - Fails if the insert operation fails.
    pub async fn insert_many(
        &self,
        query: impl Into<ParsedQuery>,
        blocks: Vec<T::Data>,
        qid: Option<crate::Qid>,
    ) -> Result<()> {
        let client = self.create_client().await?;
        let stream = client.insert_many(query, blocks, qid).await?;
        // Drain the stream to completion
        tokio::pin!(stream);
        while let Some(result) = stream.next().await {
            result?;
        }
        Ok(())
    }
}

impl ClientFactory<ArrowFormat> {
    /// Executes a query using a fresh connection and streams Arrow `RecordBatch` results.
    ///
    /// This method creates a new TCP connection, sends the query, and returns a
    /// stream of `RecordBatch` results. The connection is kept alive until the
    /// stream is fully consumed or dropped.
    ///
    /// # Parameters
    /// - `query`: The SQL query to execute.
    /// - `qid`: Optional query ID for tracking and debugging.
    ///
    /// # Returns
    /// A `Result` containing a stream of `RecordBatch` results.
    ///
    /// # Errors
    /// - Fails if the connection cannot be established.
    /// - Fails if the query execution fails.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    /// use futures_util::TryStreamExt;
    ///
    /// let factory = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .build_arrow_factory()
    ///     .await?;
    ///
    /// let stream = factory.query("SELECT * FROM my_table", None).await?;
    /// let batches: Vec<RecordBatch> = stream.try_collect().await?;
    /// ```
    pub async fn query(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<crate::Qid>,
    ) -> Result<OwnedClientStream<ArrowFormat, ClickHouseResponse<RecordBatch>>> {
        let client = self.create_client().await?;
        let stream = client.query(query, qid).await?;
        Ok(OwnedClientStream::new(client, stream))
    }

    /// Executes a query and returns a single scalar value.
    ///
    /// This method creates a new TCP connection, executes the query, and extracts
    /// a single value from the first column of the first row. This is useful for
    /// queries that return a single value, such as `SELECT count() FROM table`.
    ///
    /// # Parameters
    /// - `query`: The SQL query to execute (should return a single value).
    /// - `qid`: Optional query ID for tracking and debugging.
    ///
    /// # Returns
    /// A `Result` containing the scalar value, or an error if the query fails
    /// or returns no data.
    ///
    /// # Errors
    /// - Fails if the connection cannot be established.
    /// - Fails if the query execution fails.
    /// - Fails if no data is returned.
    /// - Fails if the value cannot be converted to the expected type.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use clickhouse_arrow::prelude::*;
    ///
    /// let factory = ClientBuilder::new()
    ///     .with_endpoint("localhost:9000")
    ///     .build_arrow_factory()
    ///     .await?;
    ///
    /// let count: u64 = factory.scalar("SELECT count() FROM my_table", None).await?;
    /// ```
    pub async fn scalar<V: ScalarValue>(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<crate::Qid>,
    ) -> Result<V> {
        let stream = self.query(query, qid).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        // Find the first non-empty batch
        let batch = batches
            .into_iter()
            .find(|b| b.num_rows() > 0)
            .ok_or(crate::Error::NoData)?;

        // Get the first column and first row
        let column = batch.column(0);
        V::extract(column, 0)
    }
}

impl ClientFactory<NativeFormat> {
    /// Executes a query using a fresh connection and streams native Block results.
    ///
    /// This method creates a new TCP connection, sends the query, and returns a
    /// stream of `Block` results. The connection is kept alive until the stream
    /// is fully consumed or dropped.
    ///
    /// # Parameters
    /// - `query`: The SQL query to execute.
    /// - `qid`: Optional query ID for tracking and debugging.
    ///
    /// # Returns
    /// A `Result` containing a stream of `Block` results.
    ///
    /// # Errors
    /// - Fails if the connection cannot be established.
    /// - Fails if the query execution fails.
    pub async fn query(
        &self,
        query: impl Into<ParsedQuery>,
        qid: Option<crate::Qid>,
    ) -> Result<OwnedClientStream<NativeFormat, ClickHouseResponse<Block>>> {
        let client = self.create_client().await?;
        let qid_actual = qid.unwrap_or_default();
        let parsed: ParsedQuery = query.into();
        let stream = client
            .query_raw(parsed.0, None::<crate::QueryParams>, qid_actual)
            .await?;
        Ok(OwnedClientStream::new(client, ClickHouseResponse::new(Box::pin(stream))))
    }
}

/// A stream wrapper that keeps the client alive until the stream is consumed.
///
/// This struct ensures that the underlying `Client` (and its TCP connection)
/// remains alive while the response stream is being consumed. When the stream
/// is dropped, the client is also dropped, closing the connection.
#[pin_project]
pub struct OwnedClientStream<T: ClientFormat, S> {
    #[pin]
    stream: S,
    /// The client is kept alive but not used after creation.
    /// It will be dropped when this struct is dropped.
    _client: Client<T>,
}

impl<T: ClientFormat, S> OwnedClientStream<T, S> {
    /// Creates a new `OwnedClientStream` wrapping a stream and keeping the client alive.
    fn new(client: Client<T>, stream: S) -> Self {
        Self { stream, _client: client }
    }
}

impl<T: ClientFormat, S: Stream> Stream for OwnedClientStream<T, S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// A trait for extracting scalar values from Arrow arrays.
///
/// This trait is implemented for common scalar types that can be extracted
/// from the first element of an Arrow array.
pub trait ScalarValue: Sized {
    /// Extracts a value from the given array at the specified index.
    ///
    /// # Errors
    /// Returns an error if the array type doesn't match the expected type
    /// or if the value cannot be converted to the target type.
    fn extract(array: &dyn arrow::array::Array, index: usize) -> Result<Self>;
}

impl ScalarValue for u64 {
    fn extract(array: &dyn arrow::array::Array, index: usize) -> Result<Self> {
        use arrow::datatypes::DataType;

        match array.data_type() {
            DataType::UInt64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected UInt64Array".into()))?;
                Ok(arr.value(index))
            }
            DataType::UInt32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt32Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected UInt32Array".into()))?;
                Ok(u64::from(arr.value(index)))
            }
            DataType::UInt16 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt16Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected UInt16Array".into()))?;
                Ok(u64::from(arr.value(index)))
            }
            DataType::UInt8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt8Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected UInt8Array".into()))?;
                Ok(u64::from(arr.value(index)))
            }
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected Int64Array".into()))?;
                let val = arr.value(index);
                u64::try_from(val).map_err(|_| {
                    crate::Error::TypeMismatch(format!("Cannot convert negative value {val} to u64"))
                })
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected Int32Array".into()))?;
                let val = arr.value(index);
                u64::try_from(val).map_err(|_| {
                    crate::Error::TypeMismatch(format!("Cannot convert negative value {val} to u64"))
                })
            }
            dt => Err(crate::Error::TypeMismatch(format!(
                "Cannot extract u64 from {dt:?}"
            ))),
        }
    }
}

impl ScalarValue for i64 {
    fn extract(array: &dyn arrow::array::Array, index: usize) -> Result<Self> {
        use arrow::datatypes::DataType;

        match array.data_type() {
            DataType::Int64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected Int64Array".into()))?;
                Ok(arr.value(index))
            }
            DataType::Int32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected Int32Array".into()))?;
                Ok(i64::from(arr.value(index)))
            }
            DataType::UInt64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::UInt64Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected UInt64Array".into()))?;
                let val = arr.value(index);
                i64::try_from(val).map_err(|_| {
                    crate::Error::TypeMismatch(format!("Value {val} too large for i64"))
                })
            }
            dt => Err(crate::Error::TypeMismatch(format!(
                "Cannot extract i64 from {dt:?}"
            ))),
        }
    }
}

impl ScalarValue for String {
    fn extract(array: &dyn arrow::array::Array, index: usize) -> Result<Self> {
        use arrow::datatypes::DataType;

        match array.data_type() {
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected StringArray".into()))?;
                Ok(arr.value(index).to_string())
            }
            DataType::LargeUtf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected LargeStringArray".into()))?;
                Ok(arr.value(index).to_string())
            }
            dt => Err(crate::Error::TypeMismatch(format!(
                "Cannot extract String from {dt:?}"
            ))),
        }
    }
}

impl ScalarValue for f64 {
    fn extract(array: &dyn arrow::array::Array, index: usize) -> Result<Self> {
        use arrow::datatypes::DataType;

        match array.data_type() {
            DataType::Float64 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected Float64Array".into()))?;
                Ok(arr.value(index))
            }
            DataType::Float32 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .ok_or_else(|| crate::Error::TypeMismatch("Expected Float32Array".into()))?;
                Ok(f64::from(arr.value(index)))
            }
            dt => Err(crate::Error::TypeMismatch(format!(
                "Cannot extract f64 from {dt:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_factory_requires_verified_builder() {
        // Test that factory requires a valid destination
        let builder = ClientBuilder::new();
        let result = ClientFactory::<ArrowFormat>::new(builder).await;
        assert!(result.is_err());
    }
}
