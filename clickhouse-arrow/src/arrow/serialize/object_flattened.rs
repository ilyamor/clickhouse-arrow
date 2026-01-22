//! FLATTENED Object serialization for schema-less JSON.
//!
//! This module handles serialization of Arrow Struct arrays as ClickHouse JSON columns
//! using the FLATTENED binary format (version 3). This format sends each JSON path as a
//! separate Dynamic column, avoiding JSON text parsing on the server.
//!
//! ## Wire Format (FLATTENED mode, version 3)
//!
//! With typed paths (paths declared in schema with explicit types):
//! ```text
//! ObjectStructure:
//!   UInt64 (LE): 3               // FLATTENED version
//!   VarUInt: num_dynamic_paths   // number of DYNAMIC paths (excludes typed)
//!   For each dynamic path (sorted):
//!     String: path_name
//!   For each TYPED path (in schema order):
//!     [Native type prefix]       // e.g., nothing for primitives, version for LowCardinality
//!   For each DYNAMIC path:
//!     [Dynamic structure]        // version, type info
//!
//! ObjectData:
//!   For each TYPED path (in schema order):
//!     [Native type data]         // e.g., raw Int64 bytes
//!   For each DYNAMIC path:
//!     [Dynamic column data]      // indexes + values
//! ```
//!
//! Without typed paths (all dynamic):
//! ```text
//! ObjectStructure:
//!   UInt64 (LE): 3               // FLATTENED version
//!   VarUInt: num_paths           // number of paths
//!   For each path (sorted):
//!     String: path_name
//!   For each path:
//!     [Dynamic structure]
//!
//! ObjectData:
//!   For each path:
//!     [Dynamic column data]
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use tokio::io::AsyncWriteExt;

use super::dynamic::{
    serialize_dynamic_data, serialize_dynamic_data_async, serialize_dynamic_structure,
    serialize_dynamic_structure_async,
};
use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::{Error, Result, Type};

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

/// Represents a typed path (declared in schema with explicit type).
#[derive(Debug)]
pub(crate) struct TypedPath {
    /// The ClickHouse type for this path
    pub ch_type: Type,
    /// The Arrow array containing values for this path
    pub array: ArrayRef,
}

/// Categorizes struct array paths into typed and dynamic paths based on schema definition.
///
/// Typed paths are paths that are declared in the JSON schema with explicit types.
/// Dynamic paths are paths discovered at runtime (not in schema).
///
/// IMPORTANT: This function ensures ALL typed paths from the schema are included,
/// even if they're not present in the Arrow data. Missing typed paths get null arrays.
/// This matches the wire format ClickHouse expects for typed path serialization.
pub(crate) fn categorize_paths(
    struct_array: &StructArray,
    typed_path_defs: &[(String, Box<Type>)],
) -> Result<(Vec<TypedPath>, Vec<FlattenedPath>)> {
    // Build a set of typed path names for quick lookup
    let typed_path_names: HashSet<&str> = typed_path_defs.iter().map(|(name, _)| name.as_str()).collect();

    // Flatten the struct array to get all paths from Arrow
    let all_arrow_paths = flatten_struct_array(struct_array, "")?;

    // Build a map from path name to Arrow array for quick lookup
    let arrow_path_map: std::collections::HashMap<&str, ArrayRef> = all_arrow_paths
        .iter()
        .map(|p| (p.path.as_str(), Arc::clone(&p.array)))
        .collect();

    let num_rows = struct_array.len();

    // Iterate over ALL typed paths from schema (in schema order)
    // This ensures ClickHouse receives data for every typed path it expects
    let mut typed_paths = Vec::with_capacity(typed_path_defs.len());
    for (path_name, ch_type) in typed_path_defs {
        let array = if let Some(arr) = arrow_path_map.get(path_name.as_str()) {
            // Arrow has this typed path - use its array
            Arc::clone(arr)
        } else {
            // Arrow doesn't have this typed path - create null array
            // Use the appropriate null array based on the ClickHouse type
            create_null_array_for_type(ch_type, num_rows)
        };

        typed_paths.push(TypedPath { ch_type: (**ch_type).clone(), array });
    }

    // Dynamic paths: Arrow paths that are NOT in typed_path_defs
    let dynamic_paths: Vec<FlattenedPath> = all_arrow_paths
        .into_iter()
        .filter(|p| !typed_path_names.contains(p.path.as_str()))
        .collect();

    // Dynamic paths are already sorted by flatten_struct_array

    Ok((typed_paths, dynamic_paths))
}

/// Creates a null array of the appropriate Arrow type for a ClickHouse type.
fn create_null_array_for_type(ch_type: &Type, num_rows: usize) -> ArrayRef {
    match ch_type {
        Type::String => Arc::new(StringArray::from(vec![None::<&str>; num_rows])) as ArrayRef,
        Type::Int64 => Arc::new(Int64Array::from(vec![None::<i64>; num_rows])) as ArrayRef,
        Type::Int32 => Arc::new(Int32Array::from(vec![None::<i32>; num_rows])) as ArrayRef,
        Type::UInt64 => Arc::new(UInt64Array::from(vec![None::<u64>; num_rows])) as ArrayRef,
        Type::UInt32 => Arc::new(UInt32Array::from(vec![None::<u32>; num_rows])) as ArrayRef,
        Type::Float64 => Arc::new(Float64Array::from(vec![None::<f64>; num_rows])) as ArrayRef,
        Type::Float32 => Arc::new(Float32Array::from(vec![None::<f32>; num_rows])) as ArrayRef,
        // Default to nullable String for other types - this will be serialized as Dynamic
        _ => Arc::new(StringArray::from(vec![None::<&str>; num_rows])) as ArrayRef,
    }
}

/// Writes only the ObjectStructure header (version, paths, and Dynamic prefixes) for FLATTENED mode.
///
/// This is called from the prefix phase or before null bitmap for Nullable(Object).
/// The format is:
///   - Object version (3)
///   - num_paths
///   - path names
///   - For each path: Dynamic structure (version, type info)
#[allow(dead_code)]
pub(crate) async fn serialize_object_structure_async<W: ClickHouseWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
) -> Result<()> {
    // Write ObjectStructure
    writer.write_u64_le(OBJECT_FLATTENED_VERSION).await?;

    // Write number of paths
    writer.write_var_uint(paths.len() as u64).await?;

    // Write path names (must be sorted for consistency)
    for path in paths {
        writer.write_string(&path.path).await?;
    }

    // Write Dynamic structures for each path (this is part of the prefix in ClickHouse)
    for path in paths {
        serialize_dynamic_structure_async(writer, &path.array).await?;
    }

    Ok(())
}

/// Writes only the ObjectData (Dynamic data for each path) for FLATTENED mode.
///
/// This is called after the null bitmap for Nullable(Object).
/// Note: The Dynamic structures (version, type info) are written in serialize_object_structure_async.
#[allow(dead_code)]
pub(crate) async fn serialize_object_data_async<W: ClickHouseWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    // Write ObjectData - each path's Dynamic data (indexes + values only)
    for path in paths {
        serialize_dynamic_data_async(writer, &path.array, state).await?;
    }

    Ok(())
}

/// Serializes an Arrow Struct array as a ClickHouse JSON column in FLATTENED mode.
///
/// This writes the ObjectStructure header followed by ObjectData (each path as Dynamic).
/// Note: For Nullable(Object), use serialize_object_structure_async and serialize_object_data_async
/// separately to ensure correct ordering with the null bitmap.
#[allow(dead_code)]
pub(crate) async fn serialize_object_flattened_async<W: ClickHouseWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    serialize_object_structure_async(writer, paths).await?;
    serialize_object_data_async(writer, paths, state).await?;
    Ok(())
}

/// Writes only the ObjectStructure header (version, paths, and Dynamic prefixes) for FLATTENED mode (sync).
#[allow(dead_code)]
pub(crate) fn serialize_object_structure<W: ClickHouseBytesWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
) -> Result<()> {
    // Write ObjectStructure
    writer.put_u64_le(OBJECT_FLATTENED_VERSION);

    // Write number of paths
    writer.put_var_uint(paths.len() as u64)?;

    // Write path names (must be sorted for consistency)
    for path in paths {
        writer.put_string(&path.path)?;
    }

    // Write Dynamic structures for each path (this is part of the prefix in ClickHouse)
    for path in paths {
        serialize_dynamic_structure(writer, &path.array)?;
    }

    Ok(())
}

/// Writes only the ObjectData (Dynamic data for each path) for FLATTENED mode (sync).
#[allow(dead_code)]
pub(crate) fn serialize_object_data<W: ClickHouseBytesWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    // Write ObjectData - each path's Dynamic data (indexes + values only)
    for path in paths {
        serialize_dynamic_data(writer, &path.array, state)?;
    }

    Ok(())
}

/// Serializes an Arrow Struct array as a ClickHouse JSON column in FLATTENED mode (sync).
#[allow(dead_code)]
pub(crate) fn serialize_object_flattened<W: ClickHouseBytesWrite>(
    writer: &mut W,
    paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    serialize_object_structure(writer, paths)?;
    serialize_object_data(writer, paths, state)?;
    Ok(())
}

// =============================================================================
// Typed Path Serialization Functions
// =============================================================================

/// Writes ObjectStructure with typed paths support (async).
/// Typed paths are NOT included in the dynamic path count.
pub(crate) async fn serialize_object_structure_with_typed_paths_async<W: ClickHouseWrite>(
    writer: &mut W,
    _typed_paths: &[TypedPath],
    dynamic_paths: &[FlattenedPath],
) -> Result<()> {
    // Write ObjectStructure
    writer.write_u64_le(OBJECT_FLATTENED_VERSION).await?;

    // Write number of DYNAMIC paths only (typed paths are not counted here)
    writer.write_var_uint(dynamic_paths.len() as u64).await?;

    // Write dynamic path names (typed path names are implied by schema)
    for path in dynamic_paths {
        writer.write_string(&path.path).await?;
    }

    // Note: Typed paths (_typed_paths) would write their native type prefixes here
    // if needed (e.g., LowCardinality version). Currently only primitive types are
    // supported which don't require prefixes.

    // Write Dynamic structures for each dynamic path
    for path in dynamic_paths {
        serialize_dynamic_structure_async(writer, &path.array).await?;
    }

    Ok(())
}

/// Writes ObjectData with typed paths support (async).
/// Typed paths are serialized using native type serialization (not Dynamic).
pub(crate) async fn serialize_object_data_with_typed_paths_async<W: ClickHouseWrite>(
    writer: &mut W,
    typed_paths: &[TypedPath],
    dynamic_paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    // Write typed path data using native serialization (no Dynamic wrapper)
    for typed in typed_paths {
        serialize_typed_path_data_async(writer, typed).await?;
    }

    // Write dynamic path data using Dynamic serialization
    for path in dynamic_paths {
        serialize_dynamic_data_async(writer, &path.array, state).await?;
    }

    Ok(())
}

/// Serializes a typed path's data using native type serialization (async).
async fn serialize_typed_path_data_async<W: ClickHouseWrite>(
    writer: &mut W,
    typed: &TypedPath,
) -> Result<()> {
    // Use native type serialization based on the ClickHouse type
    match &typed.ch_type {
        Type::Int64 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<Int64Array>() {
                for i in 0..arr.len() {
                    writer.write_i64_le(arr.value(i)).await?;
                }
            }
        }
        Type::Int32 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<Int32Array>() {
                for i in 0..arr.len() {
                    writer.write_i32_le(arr.value(i)).await?;
                }
            }
        }
        Type::UInt64 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..arr.len() {
                    writer.write_u64_le(arr.value(i)).await?;
                }
            }
        }
        Type::UInt32 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..arr.len() {
                    writer.write_u32_le(arr.value(i)).await?;
                }
            }
        }
        Type::Float64 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..arr.len() {
                    let bytes = arr.value(i).to_le_bytes();
                    writer.write_all(&bytes).await?;
                }
            }
        }
        Type::String => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    writer.write_string(arr.value(i)).await?;
                }
            }
        }
        // For other types, fall back to Dynamic serialization for now
        _ => {
            // This shouldn't happen often with typed paths, but handle gracefully
            let mut dummy_state = SerializerState::default();
            super::dynamic::serialize_dynamic_async(writer, &typed.array, &mut dummy_state).await?;
        }
    }
    Ok(())
}

/// Writes ObjectStructure with typed paths support (sync).
pub(crate) fn serialize_object_structure_with_typed_paths<W: ClickHouseBytesWrite>(
    writer: &mut W,
    _typed_paths: &[TypedPath],
    dynamic_paths: &[FlattenedPath],
) -> Result<()> {
    // Write ObjectStructure
    writer.put_u64_le(OBJECT_FLATTENED_VERSION);

    // Write number of DYNAMIC paths only
    writer.put_var_uint(dynamic_paths.len() as u64)?;

    // Write dynamic path names
    for path in dynamic_paths {
        writer.put_string(&path.path)?;
    }

    // Note: Typed paths (_typed_paths) would write their native type prefixes here
    // if needed. Currently only primitive types are supported which don't require prefixes.

    // Write Dynamic structures for each dynamic path
    for path in dynamic_paths {
        serialize_dynamic_structure(writer, &path.array)?;
    }

    Ok(())
}

/// Writes ObjectData with typed paths support (sync).
pub(crate) fn serialize_object_data_with_typed_paths<W: ClickHouseBytesWrite>(
    writer: &mut W,
    typed_paths: &[TypedPath],
    dynamic_paths: &[FlattenedPath],
    state: &mut SerializerState,
) -> Result<()> {
    // Write typed path data using native serialization
    for typed in typed_paths {
        serialize_typed_path_data(writer, typed)?;
    }

    // Write dynamic path data using Dynamic serialization
    for path in dynamic_paths {
        serialize_dynamic_data(writer, &path.array, state)?;
    }

    Ok(())
}

/// Serializes a typed path's data using native type serialization (sync).
fn serialize_typed_path_data<W: ClickHouseBytesWrite>(
    writer: &mut W,
    typed: &TypedPath,
) -> Result<()> {
    match &typed.ch_type {
        Type::Int64 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<Int64Array>() {
                for i in 0..arr.len() {
                    writer.put_i64_le(arr.value(i));
                }
            }
        }
        Type::Int32 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<Int32Array>() {
                for i in 0..arr.len() {
                    writer.put_i32_le(arr.value(i));
                }
            }
        }
        Type::UInt64 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<UInt64Array>() {
                for i in 0..arr.len() {
                    writer.put_u64_le(arr.value(i));
                }
            }
        }
        Type::UInt32 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<UInt32Array>() {
                for i in 0..arr.len() {
                    writer.put_u32_le(arr.value(i));
                }
            }
        }
        Type::Float64 => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<Float64Array>() {
                for i in 0..arr.len() {
                    writer.put_f64_le(arr.value(i));
                }
            }
        }
        Type::String => {
            if let Some(arr) = typed.array.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    writer.put_string(arr.value(i))?;
                }
            }
        }
        _ => {
            let mut dummy_state = SerializerState::default();
            super::dynamic::serialize_dynamic(writer, &typed.array, &mut dummy_state)?;
        }
    }
    Ok(())
}

/// Serializes a StructArray as FLATTENED JSON with typed paths support (async).
pub(crate) async fn serialize_struct_as_flattened_json_with_typed_paths_async<W: ClickHouseWrite>(
    writer: &mut W,
    struct_array: &StructArray,
    typed_path_defs: &[(String, Box<Type>)],
    state: &mut SerializerState,
) -> Result<()> {
    let (typed_paths, dynamic_paths) = categorize_paths(struct_array, typed_path_defs)?;
    serialize_object_structure_with_typed_paths_async(writer, &typed_paths, &dynamic_paths).await?;
    serialize_object_data_with_typed_paths_async(writer, &typed_paths, &dynamic_paths, state).await?;
    Ok(())
}

/// Serializes a StructArray as FLATTENED JSON with typed paths support (sync).
pub(crate) fn serialize_struct_as_flattened_json_with_typed_paths<W: ClickHouseBytesWrite>(
    writer: &mut W,
    struct_array: &StructArray,
    typed_path_defs: &[(String, Box<Type>)],
    state: &mut SerializerState,
) -> Result<()> {
    let (typed_paths, dynamic_paths) = categorize_paths(struct_array, typed_path_defs)?;
    serialize_object_structure_with_typed_paths(writer, &typed_paths, &dynamic_paths)?;
    serialize_object_data_with_typed_paths(writer, &typed_paths, &dynamic_paths, state)?;
    Ok(())
}

// =============================================================================
// Path Flattening Functions
// =============================================================================

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
#[allow(dead_code)]
pub(crate) async fn serialize_struct_as_flattened_json_async<W: ClickHouseWrite>(
    writer: &mut W,
    struct_array: &StructArray,
    state: &mut SerializerState,
) -> Result<()> {
    let paths = flatten_struct_array(struct_array, "")?;
    serialize_object_flattened_async(writer, &paths, state).await
}

/// Serializes a StructArray directly as FLATTENED JSON (sync).
#[allow(dead_code)]
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
        // The format is:
        // 1. Object version (3)
        // 2. num_paths
        // 3. path names (sorted alphabetically)
        // 4. Dynamic structures for each path (version, type info)
        // 5. Dynamic data for each path (indexes, values)
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

        // Dynamic Structure for "action"
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

        // Dynamic Structure for "user_id"
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

        // Dynamic Data for "action"
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

        // Dynamic Data for "user_id"
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
