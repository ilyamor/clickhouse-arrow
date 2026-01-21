//! FLATTENED Object serialization for schema-less JSON.
//!
//! This module handles serialization of Arrow Struct arrays as ClickHouse JSON columns
//! using the FLATTENED binary format (version 3). This format sends each JSON path as a
//! separate Dynamic column, avoiding JSON text parsing on the server.
//!
//! ## Wire Format (FLATTENED mode, version 3)
//!
//! ```text
//! ObjectStructure:
//!   UInt64 (LE): 3               // FLATTENED version
//!   VarUInt: num_paths           // number of paths
//!   For each path (sorted):
//!     String: path_name          // e.g., "user_id", "action"
//!
//! ObjectData:
//!   For each path (in same order as structure):
//!     [Dynamic column data]      // see dynamic.rs
//! ```

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use super::dynamic::{serialize_dynamic, serialize_dynamic_async};
use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result};

/// FLATTENED version for Object columns
const OBJECT_FLATTENED_VERSION: u64 = 3;

/// Represents a flattened JSON path with its corresponding Arrow array.
#[derive(Debug)]
pub(crate) struct FlattenedPath {
    /// The path name (e.g., "user_id", "nested.field")
    pub path: String,
    /// The Arrow array containing values for this path
    pub array: ArrayRef,
}

/// Serializes an Arrow Struct array as a ClickHouse JSON column in FLATTENED mode.
///
/// This writes the ObjectStructure header followed by ObjectData (each path as Dynamic).
pub(crate) async fn serialize_object_flattened_async<W: ClickHouseWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    // Write ObjectStructure
    writer.write_u64_le(OBJECT_FLATTENED_VERSION).await?;

    // Write number of paths
    writer.write_var_uint(paths.len() as u64).await?;

    // Write path names (must be sorted for consistency)
    for path in paths {
        writer.write_string(&path.path).await?;
    }

    // Write ObjectData - each path as a Dynamic column
    for path in paths {
        serialize_dynamic_async(writer, &path.array, state).await?;
    }

    Ok(())
}

/// Serializes an Arrow Struct array as a ClickHouse JSON column in FLATTENED mode (sync).
pub(crate) fn serialize_object_flattened<W: ClickHouseBytesWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    // Write ObjectStructure
    writer.put_u64_le(OBJECT_FLATTENED_VERSION);

    // Write number of paths
    writer.put_var_uint(paths.len() as u64)?;

    // Write path names (must be sorted for consistency)
    for path in paths {
        writer.put_string(&path.path)?;
    }

    // Write ObjectData - each path as a Dynamic column
    for path in paths {
        serialize_dynamic(writer, &path.array, state)?;
    }

    Ok(())
}

/// Flattens an Arrow StructArray into a list of (path, array) pairs.
///
/// Recursively handles nested structs, producing paths like "outer.inner.field".
pub(crate) fn flatten_struct_array(array: &StructArray, prefix: &str) -> Result<Vec<FlattenedPath>> {
    let mut paths = Vec::new();
    let fields = array.fields();
    let columns = array.columns();

    for (field, column) in fields.iter().zip(columns.iter()) {
        let field_name = field.name();
        let full_path = if prefix.is_empty() {
            field_name.clone()
        } else {
            format!("{prefix}.{field_name}")
        };

        // Check if this is a nested struct
        if let DataType::Struct(_) = column.data_type() {
            if let Some(nested_struct) = column.as_any().downcast_ref::<StructArray>() {
                // Recursively flatten nested struct
                let nested_paths = flatten_struct_array(nested_struct, &full_path)?;
                paths.extend(nested_paths);
            } else {
                return Err(Error::ArrowSerialize(format!(
                    "Failed to downcast nested struct at path '{full_path}'"
                )));
            }
        } else {
            // Leaf field - add to paths
            paths.push(FlattenedPath { path: full_path, array: Arc::clone(column) });
        }
    }

    // Sort paths for consistent serialization order
    paths.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(paths)
}

/// Serializes a StructArray directly as FLATTENED JSON.
pub(crate) async fn serialize_struct_as_flattened_json_async<W: ClickHouseWrite>(
    writer: &mut W,
    struct_array: &StructArray,
    state: &mut SerializerState,
) -> Result<()> {
    let paths = flatten_struct_array(struct_array, "")?;
    serialize_object_flattened_async(writer, &paths, state).await
}

/// Serializes a StructArray directly as FLATTENED JSON (sync).
pub(crate) fn serialize_struct_as_flattened_json<W: ClickHouseBytesWrite>(
    writer: &mut W,
    struct_array: &StructArray,
    state: &mut SerializerState,
) -> Result<()> {
    let paths = flatten_struct_array(struct_array, "")?;
    serialize_object_flattened(writer, &paths, state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields};

    fn create_simple_struct() -> StructArray {
        let user_id = Int64Array::from(vec![100, 200, 300]);
        let action = StringArray::from(vec!["click", "view", "buy"]);

        StructArray::from(vec![
            (
                Arc::new(Field::new("user_id", DataType::Int64, false)),
                Arc::new(user_id) as ArrayRef,
            ),
            (
                Arc::new(Field::new("action", DataType::Utf8, false)),
                Arc::new(action) as ArrayRef,
            ),
        ])
    }

    fn create_nested_struct() -> StructArray {
        // Inner struct: {name: String, value: Int64}
        let name = StringArray::from(vec!["test", "prod"]);
        let value = Int64Array::from(vec![42, 99]);
        let inner_struct = StructArray::from(vec![
            (Arc::new(Field::new("name", DataType::Utf8, false)), Arc::new(name) as ArrayRef),
            (Arc::new(Field::new("value", DataType::Int64, false)), Arc::new(value) as ArrayRef),
        ]);

        // Outer struct: {id: Int64, metadata: {name, value}}
        let id = Int64Array::from(vec![1, 2]);
        let inner_fields =
            Fields::from(vec![Field::new("name", DataType::Utf8, false), Field::new("value", DataType::Int64, false)]);

        StructArray::from(vec![
            (Arc::new(Field::new("id", DataType::Int64, false)), Arc::new(id) as ArrayRef),
            (
                Arc::new(Field::new("metadata", DataType::Struct(inner_fields), false)),
                Arc::new(inner_struct) as ArrayRef,
            ),
        ])
    }

    #[test]
    fn test_flatten_simple_struct() {
        let struct_array = create_simple_struct();
        let paths = flatten_struct_array(&struct_array, "").unwrap();

        assert_eq!(paths.len(), 2);
        // Paths should be sorted alphabetically
        assert_eq!(paths[0].path, "action");
        assert_eq!(paths[1].path, "user_id");
    }

    #[test]
    fn test_flatten_nested_struct() {
        let struct_array = create_nested_struct();
        let paths = flatten_struct_array(&struct_array, "").unwrap();

        assert_eq!(paths.len(), 3);
        // Paths should be sorted: id, metadata.name, metadata.value
        assert_eq!(paths[0].path, "id");
        assert_eq!(paths[1].path, "metadata.name");
        assert_eq!(paths[2].path, "metadata.value");
    }

    #[tokio::test]
    async fn test_serialize_object_flattened() {
        let struct_array = create_simple_struct();
        let paths = flatten_struct_array(&struct_array, "").unwrap();

        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_object_flattened_async(&mut buffer, &paths, &mut state).await.unwrap();

        // Check ObjectStructure
        assert_eq!(&buffer[0..8], &3u64.to_le_bytes()); // Version = 3 (FLATTENED)
        assert_eq!(buffer[8], 2); // 2 paths

        // Path 1: "action" (length 6)
        assert_eq!(buffer[9], 6);
        assert_eq!(&buffer[10..16], b"action");

        // Path 2: "user_id" (length 7)
        assert_eq!(buffer[16], 7);
        assert_eq!(&buffer[17..24], b"user_id");

        // After this comes the Dynamic column data for each path
        // Dynamic for "action" starts at byte 24
        // Check it has version 3 (FLATTENED Dynamic)
        assert_eq!(&buffer[24..32], &3u64.to_le_bytes());
    }

    #[tokio::test]
    async fn test_serialize_struct_as_flattened_json() {
        let struct_array = create_simple_struct();
        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_struct_as_flattened_json_async(&mut buffer, &struct_array, &mut state)
            .await
            .unwrap();

        // Check ObjectStructure version
        assert_eq!(&buffer[0..8], &3u64.to_le_bytes());
    }

    #[test]
    fn test_serialize_object_flattened_sync() {
        let struct_array = create_simple_struct();
        let paths = flatten_struct_array(&struct_array, "").unwrap();

        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_object_flattened(&mut buffer, &paths, &mut state).unwrap();

        // Check ObjectStructure
        assert_eq!(&buffer[0..8], &3u64.to_le_bytes()); // Version = 3
    }

    #[tokio::test]
    async fn test_complete_flattened_json_wire_format() {
        // Test the complete wire format as specified in the plan
        // Row 0: {"user_id": 100, "action": "click"}
        // Row 1: {"user_id": 200, "action": "view"}
        // Row 2: {"user_id": 300, "action": "buy"}

        let user_id = Int64Array::from(vec![100, 200, 300]);
        let action = StringArray::from(vec!["click", "view", "buy"]);

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("user_id", DataType::Int64, false)),
                Arc::new(user_id) as ArrayRef,
            ),
            (
                Arc::new(Field::new("action", DataType::Utf8, false)),
                Arc::new(action) as ArrayRef,
            ),
        ]);

        let mut buffer = Vec::new();
        let mut state = SerializerState::default();

        serialize_struct_as_flattened_json_async(&mut buffer, &struct_array, &mut state)
            .await
            .unwrap();

        // Verify the wire format structure
        let mut pos = 0;

        // ObjectStructure
        // Version = 3
        assert_eq!(&buffer[pos..pos + 8], &3u64.to_le_bytes());
        pos += 8;

        // num_paths = 2
        assert_eq!(buffer[pos], 2);
        pos += 1;

        // Path "action" (alphabetically first)
        assert_eq!(buffer[pos], 6); // length
        pos += 1;
        assert_eq!(&buffer[pos..pos + 6], b"action");
        pos += 6;

        // Path "user_id"
        assert_eq!(buffer[pos], 7); // length
        pos += 1;
        assert_eq!(&buffer[pos..pos + 7], b"user_id");
        pos += 7;

        // ObjectData for "action" - Dynamic column
        // DynamicStructure version = 3 (FLATTENED)
        assert_eq!(&buffer[pos..pos + 8], &3u64.to_le_bytes());
        pos += 8;

        // num_types = 1
        assert_eq!(buffer[pos], 1);
        pos += 1;

        // Type name "String"
        assert_eq!(buffer[pos], 6); // length
        pos += 1;
        assert_eq!(&buffer[pos..pos + 6], b"String");
        pos += 6;

        // Indexes (all 0 for String type)
        assert_eq!(&buffer[pos..pos + 3], &[0, 0, 0]);
        pos += 3;

        // String values: "click", "view", "buy"
        assert_eq!(buffer[pos], 5); // "click" length
        pos += 1;
        assert_eq!(&buffer[pos..pos + 5], b"click");
        pos += 5;

        assert_eq!(buffer[pos], 4); // "view" length
        pos += 1;
        assert_eq!(&buffer[pos..pos + 4], b"view");
        pos += 4;

        assert_eq!(buffer[pos], 3); // "buy" length
        pos += 1;
        assert_eq!(&buffer[pos..pos + 3], b"buy");
        pos += 3;

        // ObjectData for "user_id" - Dynamic column
        // DynamicStructure version = 3 (FLATTENED)
        assert_eq!(&buffer[pos..pos + 8], &3u64.to_le_bytes());
        pos += 8;

        // num_types = 1
        assert_eq!(buffer[pos], 1);
        pos += 1;

        // Type name "Int64"
        assert_eq!(buffer[pos], 5); // length
        pos += 1;
        assert_eq!(&buffer[pos..pos + 5], b"Int64");
        pos += 5;

        // Indexes (all 0 for Int64 type)
        assert_eq!(&buffer[pos..pos + 3], &[0, 0, 0]);
        pos += 3;

        // Int64 values: 100, 200, 300
        assert_eq!(&buffer[pos..pos + 8], &100i64.to_le_bytes());
        pos += 8;
        assert_eq!(&buffer[pos..pos + 8], &200i64.to_le_bytes());
        pos += 8;
        assert_eq!(&buffer[pos..pos + 8], &300i64.to_le_bytes());
    }
}
