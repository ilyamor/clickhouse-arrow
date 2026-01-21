//! JSON flattening utilities for FLATTENED mode serialization.
//!
//! This module provides functionality to convert Arrow arrays (Struct, Map, or JSON strings)
//! into a flattened list of (path, array) pairs suitable for FLATTENED JSON serialization.
//!
//! ## Usage
//!
//! ```ignore
//! use clickhouse_arrow::arrow::json_flatten::{flatten_to_paths, FlattenedJsonPaths};
//!
//! // From a StructArray
//! let paths = flatten_to_paths(&struct_array)?;
//!
//! // Then serialize using object_flattened
//! serialize_object_flattened_async(writer, paths.as_flattened_paths(), state).await?;
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field};

use crate::{Error, Result};

/// A collection of flattened JSON paths with their corresponding Arrow arrays.
#[derive(Debug, Default)]
pub struct FlattenedJsonPaths {
    /// Map of path -> array, sorted by path name
    paths: BTreeMap<String, ArrayRef>,
    /// Number of rows
    num_rows: usize,
}

impl FlattenedJsonPaths {
    /// Creates a new empty `FlattenedJsonPaths`.
    #[must_use]
    pub fn new() -> Self {
        Self { paths: BTreeMap::new(), num_rows: 0 }
    }

    /// Creates a new `FlattenedJsonPaths` with the specified number of rows.
    #[must_use]
    pub fn with_rows(num_rows: usize) -> Self {
        Self { paths: BTreeMap::new(), num_rows }
    }

    /// Adds a path and its corresponding array.
    pub fn add_path(&mut self, path: String, array: ArrayRef) -> Result<()> {
        if self.num_rows == 0 {
            self.num_rows = array.len();
        } else if array.len() != self.num_rows {
            return Err(Error::ArrowSerialize(format!(
                "Path '{}' has {} rows, expected {}",
                path,
                array.len(),
                self.num_rows
            )));
        }
        let _prev = self.paths.insert(path, array);
        Ok(())
    }

    /// Returns an iterator over (path, array) pairs in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &ArrayRef)> {
        self.paths.iter()
    }

    /// Returns the number of paths.
    #[must_use]
    pub fn len(&self) -> usize {
        self.paths.len()
    }

    /// Returns true if there are no paths.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    /// Returns the number of rows.
    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Consumes self and returns a Vec of (path, array) pairs.
    #[must_use]
    pub fn into_vec(self) -> Vec<(String, ArrayRef)> {
        self.paths.into_iter().collect()
    }

    /// Converts to the format expected by `serialize_object_flattened`.
    #[must_use]
    #[allow(dead_code)] // Utility method for manual path construction
    pub(crate) fn as_flattened_paths(&self) -> Vec<crate::arrow::serialize::object_flattened::FlattenedPath> {
        self.paths
            .iter()
            .map(|(path, array)| crate::arrow::serialize::object_flattened::FlattenedPath {
                path: path.clone(),
                array: Arc::clone(array),
            })
            .collect()
    }
}

/// Flattens an Arrow array into JSON paths suitable for FLATTENED serialization.
///
/// Supports:
/// - `StructArray`: Flattens fields into paths (handles nested structs)
/// - `StringArray` with JSON: Parses JSON strings and extracts paths
///
/// # Arguments
/// * `array` - The Arrow array to flatten
///
/// # Returns
/// A `FlattenedJsonPaths` containing all extracted paths and their arrays.
pub fn flatten_to_paths(array: &dyn Array) -> Result<FlattenedJsonPaths> {
    match array.data_type() {
        DataType::Struct(_) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to StructArray".to_string())
            })?;
            flatten_struct_to_paths(struct_array, "")
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            // JSON string array - would need to parse JSON
            // For now, treat as a single path with the raw string
            Err(Error::ArrowSerialize(
                "JSON string parsing for FLATTENED mode not yet implemented. \
                 Use StructArray instead."
                    .to_string(),
            ))
        }
        dt => Err(Error::ArrowSerialize(format!(
            "Cannot flatten array of type {dt:?} to JSON paths. Expected Struct or JSON string."
        ))),
    }
}

/// Flattens a StructArray into JSON paths.
fn flatten_struct_to_paths(array: &StructArray, prefix: &str) -> Result<FlattenedJsonPaths> {
    let mut paths = FlattenedJsonPaths::with_rows(array.len());
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
                let nested_paths = flatten_struct_to_paths(nested_struct, &full_path)?;
                for (nested_path, nested_array) in nested_paths.paths {
                    paths.add_path(nested_path, nested_array)?;
                }
            } else {
                return Err(Error::ArrowSerialize(format!(
                    "Failed to downcast nested struct at path '{full_path}'"
                )));
            }
        } else {
            // Leaf field - add to paths
            paths.add_path(full_path, Arc::clone(column))?;
        }
    }

    Ok(paths)
}

/// Converts a MapArray to flattened JSON paths.
///
/// This is useful when the JSON data is represented as a Map<String, Dynamic>.
pub fn flatten_map_to_paths(_array: &MapArray) -> Result<FlattenedJsonPaths> {
    // MapArray is more complex - would need to collect all unique keys
    // and build arrays for each key
    Err(Error::ArrowSerialize(
        "MapArray flattening for FLATTENED JSON mode not yet implemented".to_string(),
    ))
}

/// Creates a FlattenedJsonPaths from explicit path-array pairs.
///
/// Useful for manually constructing FLATTENED JSON data.
pub fn create_flattened_paths(
    pairs: impl IntoIterator<Item = (impl Into<String>, ArrayRef)>,
) -> Result<FlattenedJsonPaths> {
    let pairs: Vec<_> = pairs.into_iter().collect();
    if pairs.is_empty() {
        return Ok(FlattenedJsonPaths::new());
    }

    let num_rows = pairs.first().map(|(_, arr)| arr.len()).unwrap_or(0);
    let mut paths = FlattenedJsonPaths::with_rows(num_rows);

    for (path, array) in pairs {
        paths.add_path(path.into(), array)?;
    }

    Ok(paths)
}

/// Builds a StructArray from flattened paths.
///
/// This is the inverse of `flatten_to_paths` - useful for constructing
/// Arrow data that will be serialized as FLATTENED JSON.
pub fn build_struct_from_paths(paths: &FlattenedJsonPaths) -> Result<StructArray> {
    if paths.is_empty() {
        return Err(Error::ArrowSerialize("Cannot build struct from empty paths".to_string()));
    }

    let mut fields = Vec::new();
    let mut arrays = Vec::new();

    for (path, array) in paths.iter() {
        // For simplicity, treat all paths as top-level fields
        // A full implementation would reconstruct nested structs
        fields.push(Arc::new(Field::new(path, array.data_type().clone(), array.null_count() > 0)));
        arrays.push(Arc::clone(array));
    }

    Ok(StructArray::new(fields.into(), arrays, None))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flatten_simple_struct() {
        let user_id = Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef;
        let action = Arc::new(StringArray::from(vec!["click", "view", "buy"])) as ArrayRef;

        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("user_id", DataType::Int64, false)), Arc::clone(&user_id)),
            (Arc::new(Field::new("action", DataType::Utf8, false)), Arc::clone(&action)),
        ]);

        let paths = flatten_to_paths(&struct_array).unwrap();

        assert_eq!(paths.len(), 2);
        assert_eq!(paths.num_rows(), 3);

        let path_names: Vec<_> = paths.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(path_names, vec!["action", "user_id"]); // sorted
    }

    #[test]
    fn test_flatten_nested_struct() {
        let name = Arc::new(StringArray::from(vec!["test", "prod"])) as ArrayRef;
        let value = Arc::new(Int64Array::from(vec![42, 99])) as ArrayRef;

        let inner_struct = StructArray::from(vec![
            (Arc::new(Field::new("name", DataType::Utf8, false)), name),
            (Arc::new(Field::new("value", DataType::Int64, false)), value),
        ]);

        let id = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
        let inner_fields = arrow::datatypes::Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]);

        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("id", DataType::Int64, false)), id),
            (
                Arc::new(Field::new("metadata", DataType::Struct(inner_fields), false)),
                Arc::new(inner_struct) as ArrayRef,
            ),
        ]);

        let paths = flatten_to_paths(&struct_array).unwrap();

        assert_eq!(paths.len(), 3);
        assert_eq!(paths.num_rows(), 2);

        let path_names: Vec<_> = paths.iter().map(|(p, _)| p.as_str()).collect();
        assert_eq!(path_names, vec!["id", "metadata.name", "metadata.value"]);
    }

    #[test]
    fn test_create_flattened_paths() {
        let user_id = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let action = Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef;

        let paths = create_flattened_paths([("user_id", user_id), ("action", action)]).unwrap();

        assert_eq!(paths.len(), 2);
        assert_eq!(paths.num_rows(), 3);
    }

    #[test]
    fn test_flattened_paths_row_mismatch() {
        let arr1 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let arr2 = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef; // Wrong length

        let result = create_flattened_paths([("a", arr1), ("b", arr2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_as_flattened_paths() {
        let arr = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let paths = create_flattened_paths([("test", arr)]).unwrap();

        let flattened = paths.as_flattened_paths();
        assert_eq!(flattened.len(), 1);
        assert_eq!(flattened[0].path, "test");
    }

    #[test]
    fn test_into_vec() {
        let arr = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let paths = create_flattened_paths([("test", arr)]).unwrap();

        let vec = paths.into_vec();
        assert_eq!(vec.len(), 1);
        assert_eq!(vec[0].0, "test");
    }
}