//! Dynamic column serialization for FLATTENED JSON mode.
//!
//! This module handles serialization of Arrow arrays as ClickHouse Dynamic columns,
//! which are used in the FLATTENED JSON serialization format (version 2).
//!
//! ## Wire Format
//!
//! ```text
//! DynamicStructure:
//!   UInt64 (LE): 2               // FLATTENED version
//!   VarUInt: num_types           // number of distinct types (usually 1)
//!   For each type:
//!     String: type_name          // e.g., "Int64", "String"
//!
//! DynamicData:
//!   Indexes column:              // which type each row has (UInt8/16/32/64)
//!   For each type:
//!     [Type-specific column data]
//! ```

use arrow::array::*;
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result};

/// FLATTENED version for Dynamic columns
/// Note: In ClickHouse, Dynamic versions are: V1=1, V2=2, FLATTENED=3, V3=4
const DYNAMIC_FLATTENED_VERSION: u64 = 3;

/// Null discriminator index (num_types = null marker)
const NULL_DISCRIMINATOR: u8 = 1;

/// Converts an Arrow DataType to a ClickHouse type name string for Dynamic serialization.
pub(crate) fn arrow_to_clickhouse_type_name(data_type: &DataType) -> Result<String> {
    let type_name = match data_type {
        DataType::Boolean => "Bool",
        DataType::Int8 => "Int8",
        DataType::Int16 => "Int16",
        DataType::Int32 => "Int32",
        DataType::Int64 => "Int64",
        DataType::UInt8 => "UInt8",
        DataType::UInt16 => "UInt16",
        DataType::UInt32 => "UInt32",
        DataType::UInt64 => "UInt64",
        DataType::Float32 => "Float32",
        DataType::Float64 => "Float64",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "String",
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "String",
        DataType::Date32 => "Date",
        DataType::Date64 => "DateTime64(3)",
        DataType::Timestamp(unit, tz) => {
            let precision = match unit {
                arrow::datatypes::TimeUnit::Second => 0,
                arrow::datatypes::TimeUnit::Millisecond => 3,
                arrow::datatypes::TimeUnit::Microsecond => 6,
                arrow::datatypes::TimeUnit::Nanosecond => 9,
            };
            let tz_name = tz.as_deref().unwrap_or("UTC");
            return Ok(format!("DateTime64({precision}, '{tz_name}')"));
        }
        DataType::List(inner) | DataType::LargeList(inner) => {
            let inner_name = arrow_to_clickhouse_type_name(inner.data_type())?;
            return Ok(format!("Array({inner_name})"));
        }
        DataType::Struct(fields) => {
            let field_types: Result<Vec<String>> = fields
                .iter()
                .map(|f| arrow_to_clickhouse_type_name(f.data_type()))
                .collect();
            let types_str = field_types?.join(", ");
            return Ok(format!("Tuple({types_str})"));
        }
        DataType::Null => "Null",
        _ => {
            return Err(Error::ArrowSerialize(format!(
                "Unsupported Arrow type for Dynamic column: {data_type:?}"
            )));
        }
    };
    Ok(type_name.to_string())
}

/// Serializes an Arrow array as a ClickHouse Dynamic column in FLATTENED mode.
///
/// This writes the DynamicStructure header followed by DynamicData.
pub(crate) async fn serialize_dynamic_async<W: ClickHouseWrite>(
    writer: &mut W,
    array: &ArrayRef,
    state: &mut SerializerState,
) -> Result<()> {
    let num_rows = array.len();
    let null_count = array.null_count();

    // Determine the ClickHouse type name for non-null values
    let type_name = arrow_to_clickhouse_type_name(array.data_type())?;

    // Write DynamicStructure
    writer.write_u64_le(DYNAMIC_FLATTENED_VERSION).await?;

    // Number of variant types (1 for the actual type)
    writer.write_var_uint(1).await?;
    // Write type name
    writer.write_string(&type_name).await?;

    // Write DynamicData

    // Write indexes column (discriminator for each row)
    // 0 = first variant type, 1 = NULL (since num_types = 1)
    for i in 0..num_rows {
        if array.is_null(i) {
            writer.write_u8(NULL_DISCRIMINATOR).await?;
        } else {
            writer.write_u8(0).await?; // First (and only) variant type
        }
    }

    // Write variant column data (only non-null values)
    if num_rows > null_count {
        serialize_variant_values_async(writer, array, state).await?;
    }

    Ok(())
}

/// Serializes an Arrow array as a ClickHouse Dynamic column in FLATTENED mode (sync version).
pub(crate) fn serialize_dynamic<W: ClickHouseBytesWrite>(
    writer: &mut W,
    array: &ArrayRef,
    state: &mut SerializerState,
) -> Result<()> {
    let num_rows = array.len();
    let null_count = array.null_count();

    // Determine the ClickHouse type name for non-null values
    let type_name = arrow_to_clickhouse_type_name(array.data_type())?;

    // Write DynamicStructure
    writer.put_u64_le(DYNAMIC_FLATTENED_VERSION);

    // Number of variant types (1 for the actual type)
    writer.put_var_uint(1)?;
    // Write type name
    writer.put_string(&type_name)?;

    // Write DynamicData

    // Write indexes column (discriminator for each row)
    for i in 0..num_rows {
        if array.is_null(i) {
            writer.put_u8(NULL_DISCRIMINATOR);
        } else {
            writer.put_u8(0); // First (and only) variant type
        }
    }

    // Write variant column data (only non-null values)
    if num_rows > null_count {
        serialize_variant_values(writer, array, state)?;
    }

    Ok(())
}

/// Serializes only the non-null values of an Arrow array in ClickHouse native format.
async fn serialize_variant_values_async<W: ClickHouseWrite>(
    writer: &mut W,
    array: &ArrayRef,
    _state: &mut SerializerState,
) -> Result<()> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to BooleanArray".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_u8(u8::from(arr.value(i))).await?;
                }
            }
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int8Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_i8(arr.value(i)).await?;
                }
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int16Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_i16_le(arr.value(i)).await?;
                }
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int32Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_i32_le(arr.value(i)).await?;
                }
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int64Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_i64_le(arr.value(i)).await?;
                }
            }
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt8Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_u8(arr.value(i)).await?;
                }
            }
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt16Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_u16_le(arr.value(i)).await?;
                }
            }
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt32Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_u32_le(arr.value(i)).await?;
                }
            }
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt64Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_u64_le(arr.value(i)).await?;
                }
            }
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Float32Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_u32_le(arr.value(i).to_bits()).await?;
                }
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Float64Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.write_u64_le(arr.value(i).to_bits()).await?;
                }
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            // Handle as strings
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.write_string(arr.value(i)).await?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.write_string(arr.value(i)).await?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.write_string(arr.value(i)).await?;
                    }
                }
            } else {
                return Err(Error::ArrowSerialize(
                    "Failed to downcast to string array type".to_string(),
                ));
            }
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            // Handle as binary (written as strings in ClickHouse)
            if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.write_string(arr.value(i)).await?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.write_string(arr.value(i)).await?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<BinaryViewArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.write_string(arr.value(i)).await?;
                    }
                }
            } else {
                return Err(Error::ArrowSerialize(
                    "Failed to downcast to binary array type".to_string(),
                ));
            }
        }
        DataType::List(_) | DataType::LargeList(_) => {
            // For arrays, we need to write offsets then values
            // This is a simplified version - full implementation would need recursive serialization
            return Err(Error::ArrowSerialize(
                "Array types in Dynamic columns not yet fully implemented".to_string(),
            ));
        }
        dt => {
            return Err(Error::ArrowSerialize(format!(
                "Unsupported data type for Dynamic variant serialization: {dt:?}"
            )));
        }
    }
    Ok(())
}

/// Serializes only the non-null values of an Arrow array in ClickHouse native format (sync).
fn serialize_variant_values<W: ClickHouseBytesWrite>(
    writer: &mut W,
    array: &ArrayRef,
    _state: &mut SerializerState,
) -> Result<()> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to BooleanArray".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_u8(u8::from(arr.value(i)));
                }
            }
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int8Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_i8(arr.value(i));
                }
            }
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int16Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_i16_le(arr.value(i));
                }
            }
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int32Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_i32_le(arr.value(i));
                }
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int64Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_i64_le(arr.value(i));
                }
            }
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt8Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_u8(arr.value(i));
                }
            }
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt16Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_u16_le(arr.value(i));
                }
            }
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt32Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_u32_le(arr.value(i));
                }
            }
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt64Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_u64_le(arr.value(i));
                }
            }
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Float32Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_u32_le(arr.value(i).to_bits());
                }
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Float64Array".to_string())
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    writer.put_u64_le(arr.value(i).to_bits());
                }
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.put_string(arr.value(i))?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.put_string(arr.value(i))?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.put_string(arr.value(i))?;
                    }
                }
            } else {
                return Err(Error::ArrowSerialize(
                    "Failed to downcast to string array type".to_string(),
                ));
            }
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.put_string(arr.value(i))?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.put_string(arr.value(i))?;
                    }
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<BinaryViewArray>() {
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        writer.put_string(arr.value(i))?;
                    }
                }
            } else {
                return Err(Error::ArrowSerialize(
                    "Failed to downcast to binary array type".to_string(),
                ));
            }
        }
        DataType::List(_) | DataType::LargeList(_) => {
            return Err(Error::ArrowSerialize(
                "Array types in Dynamic columns not yet fully implemented".to_string(),
            ));
        }
        dt => {
            return Err(Error::ArrowSerialize(format!(
                "Unsupported data type for Dynamic variant serialization: {dt:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_arrow_to_clickhouse_type_name() {
        assert_eq!(arrow_to_clickhouse_type_name(&DataType::Int64).unwrap(), "Int64");
        assert_eq!(arrow_to_clickhouse_type_name(&DataType::Utf8).unwrap(), "String");
        assert_eq!(arrow_to_clickhouse_type_name(&DataType::Float64).unwrap(), "Float64");
        assert_eq!(arrow_to_clickhouse_type_name(&DataType::Boolean).unwrap(), "Bool");
    }

    #[tokio::test]
    async fn test_serialize_dynamic_int64() {
        let array = Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_dynamic_async(&mut buffer, &array, &mut state).await.unwrap();

        // Check DynamicStructure
        assert_eq!(&buffer[0..8], &3u64.to_le_bytes()); // Version = 2
        assert_eq!(buffer[8], 1); // 1 variant type
        assert_eq!(buffer[9], 5); // "Int64" length
        assert_eq!(&buffer[10..15], b"Int64");

        // Check indexes (all 0 = first variant)
        assert_eq!(&buffer[15..18], &[0, 0, 0]);

        // Check values (100, 200, 300 as little-endian Int64)
        assert_eq!(&buffer[18..26], &100i64.to_le_bytes());
        assert_eq!(&buffer[26..34], &200i64.to_le_bytes());
        assert_eq!(&buffer[34..42], &300i64.to_le_bytes());
    }

    #[tokio::test]
    async fn test_serialize_dynamic_string() {
        let array = Arc::new(StringArray::from(vec!["hello", "world"])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_dynamic_async(&mut buffer, &array, &mut state).await.unwrap();

        // Check DynamicStructure
        assert_eq!(&buffer[0..8], &3u64.to_le_bytes()); // Version = 2
        assert_eq!(buffer[8], 1); // 1 variant type
        assert_eq!(buffer[9], 6); // "String" length
        assert_eq!(&buffer[10..16], b"String");

        // Check indexes
        assert_eq!(&buffer[16..18], &[0, 0]);

        // Check values
        assert_eq!(buffer[18], 5); // "hello" length
        assert_eq!(&buffer[19..24], b"hello");
        assert_eq!(buffer[24], 5); // "world" length
        assert_eq!(&buffer[25..30], b"world");
    }

    #[tokio::test]
    async fn test_serialize_dynamic_with_nulls() {
        let array = Arc::new(Int64Array::from(vec![Some(100), None, Some(300)])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_dynamic_async(&mut buffer, &array, &mut state).await.unwrap();

        // Check DynamicStructure
        assert_eq!(&buffer[0..8], &3u64.to_le_bytes()); // Version = 2
        assert_eq!(buffer[8], 1); // 1 variant type
        assert_eq!(buffer[9], 5); // "Int64" length
        assert_eq!(&buffer[10..15], b"Int64");

        // Check indexes (0 = Int64, 1 = NULL, 0 = Int64)
        assert_eq!(&buffer[15..18], &[0, 1, 0]);

        // Check values (only non-null: 100, 300)
        assert_eq!(&buffer[18..26], &100i64.to_le_bytes());
        assert_eq!(&buffer[26..34], &300i64.to_le_bytes());
    }

    #[test]
    fn test_serialize_dynamic_sync() {
        let array = Arc::new(Int64Array::from(vec![42])) as ArrayRef;
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_dynamic(&mut buffer, &array, &mut state).unwrap();

        // Verify basic structure
        assert_eq!(&buffer[0..8], &3u64.to_le_bytes()); // Version = 2
    }
}
