//! JSON subcolumn assembly for flattened Arrow columns.
//!
//! This module provides functionality to detect and assemble Arrow columns with dot notation
//! (e.g., `labels_json.id`, `labels_json.name`) into JSON strings that can be inserted into
//! `ClickHouse` JSON columns.
//!
//! `ClickHouse` does not support flattened JSON input natively (only output), so this module
//! performs client-side JSON assembly before sending data to `ClickHouse`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, StringArray, StructArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;

use crate::{Error, Result};

/// Represents a parsed JSON subcolumn path.
///
/// For example, the column name `labels_json.nested.field` would be parsed as:
/// - `base_name`: `"labels_json"`
/// - `subpath`: `["nested", "field"]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonPath {
    /// The base column name (before the first dot).
    pub base_name: String,
    /// The nested path components (after the first dot, split by dots).
    pub subpath: Vec<String>,
}

/// Parses a column name into a potential JSON path.
///
/// Returns `Some(JsonPath)` if the name contains a dot (indicating a subcolumn),
/// otherwise returns `None`.
///
/// # Examples
///
/// ```ignore
/// use clickhouse_arrow::arrow::json_assembly::parse_json_column_name;
///
/// let path = parse_json_column_name("labels_json.id").unwrap();
/// assert_eq!(path.base_name, "labels_json");
/// assert_eq!(path.subpath, vec!["id"]);
///
/// let nested = parse_json_column_name("data.nested.field").unwrap();
/// assert_eq!(nested.base_name, "data");
/// assert_eq!(nested.subpath, vec!["nested", "field"]);
///
/// assert!(parse_json_column_name("simple_column").is_none());
/// ```
#[must_use]
pub fn parse_json_column_name(name: &str) -> Option<JsonPath> {
    let (base_name, rest) = name.split_once('.')?;

    Some(JsonPath {
        base_name: base_name.to_string(),
        subpath: rest.split('.').map(String::from).collect(),
    })
}

/// A group of columns that share the same JSON base name.
#[derive(Debug, Clone)]
pub struct JsonColumnGroup {
    /// The base column name that will become the JSON column name.
    pub base_name: String,
    /// List of (subpath, `column_index`) tuples for all subcolumns in this group.
    pub subcolumns: Vec<(Vec<String>, usize)>,
}

/// Detects and groups JSON subcolumns in a [`RecordBatch`].
///
/// Scans all column names for dot notation and groups them by their base name.
/// Columns without dots are ignored.
///
/// # Returns
///
/// A vector of `JsonColumnGroup` structs, each representing a group of columns
/// that should be assembled into a single JSON column.
#[must_use]
pub fn detect_json_groups(batch: &RecordBatch) -> Vec<JsonColumnGroup> {
    let mut groups: HashMap<String, Vec<(Vec<String>, usize)>> = HashMap::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if let Some(path) = parse_json_column_name(field.name()) {
            groups.entry(path.base_name.clone()).or_default().push((path.subpath, idx));
        }
    }

    groups
        .into_iter()
        .map(|(base_name, subcolumns)| JsonColumnGroup { base_name, subcolumns })
        .collect()
}

/// A struct column that should be converted to JSON.
#[derive(Debug, Clone)]
pub struct StructColumnInfo {
    /// The column name.
    pub name: String,
    /// The column index in the [`RecordBatch`].
    pub column_index: usize,
    /// The struct fields.
    pub fields: Fields,
}

/// Detects Struct columns in a [`RecordBatch`] that can be converted to JSON.
///
/// Scans all columns and returns information about those with `DataType::Struct`.
///
/// # Returns
///
/// A vector of `StructColumnInfo` structs, each representing a Struct column
/// that can be serialized to JSON.
#[must_use]
pub fn detect_struct_columns(batch: &RecordBatch) -> Vec<StructColumnInfo> {
    let mut struct_columns = Vec::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if let DataType::Struct(fields) = field.data_type() {
            struct_columns.push(StructColumnInfo {
                name: field.name().clone(),
                column_index: idx,
                fields: fields.clone(),
            });
        }
    }

    struct_columns
}

/// Converts a [`StructArray`] to a [`StringArray`] containing JSON objects.
///
/// Each row in the [`StructArray`] becomes a JSON object string.
///
/// # Errors
///
/// Returns an error if any value cannot be converted to JSON.
pub fn struct_array_to_json(array: &StructArray) -> Result<StringArray> {
    let num_rows = array.len();
    let mut json_strings: Vec<Option<String>> = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        if array.is_null(row_idx) {
            json_strings.push(None);
        } else {
            let json = struct_value_to_json_string(array, row_idx)?;
            json_strings.push(Some(json));
        }
    }

    Ok(StringArray::from(json_strings))
}

/// Converts a single struct value at a row index to a JSON string.
fn struct_value_to_json_string(array: &StructArray, row_idx: usize) -> Result<String> {
    let mut obj = String::with_capacity(64);
    obj.push('{');

    let fields = array.fields();
    let columns = array.columns();

    for (i, (field, column)) in fields.iter().zip(columns.iter()).enumerate() {
        if i > 0 {
            obj.push(',');
        }
        obj.push('"');
        obj.push_str(field.name());
        obj.push_str("\":");

        let value = arrow_value_to_json_string(column.as_ref(), row_idx)?;
        obj.push_str(&value);
    }

    obj.push('}');
    Ok(obj)
}

/// Converts a single list value at a row index to a JSON array string.
fn list_value_to_json_string(array: &ListArray, row_idx: usize) -> Result<String> {
    let values = array.value(row_idx);
    let mut arr_str = String::with_capacity(32);
    arr_str.push('[');

    for i in 0..values.len() {
        if i > 0 {
            arr_str.push(',');
        }
        let value = arrow_value_to_json_string(values.as_ref(), i)?;
        arr_str.push_str(&value);
    }

    arr_str.push(']');
    Ok(arr_str)
}

/// Converts an Arrow array value at a specific row to a JSON-compatible string representation.
///
/// Handles null values and various Arrow data types, returning a JSON-formatted string.
#[allow(clippy::too_many_lines)]
fn arrow_value_to_json_string(array: &dyn Array, row_idx: usize) -> Result<String> {
    if array.is_null(row_idx) {
        return Ok("null".to_string());
    }

    let result = match array.data_type() {
        DataType::Null => "null".to_string(),
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to BooleanArray".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int8Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int16Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int32Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Int64Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt8Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt16Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt32Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to UInt64Array".to_string())
            })?;
            arr.value(row_idx).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Float32Array".to_string())
            })?;
            let val = arr.value(row_idx);
            if val.is_finite() { val.to_string() } else { "null".to_string() }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to Float64Array".to_string())
            })?;
            let val = arr.value(row_idx);
            if val.is_finite() { val.to_string() } else { "null".to_string() }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let s = array.as_string::<i32>().value(row_idx);
            escape_json_string(s)
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            // For binary data, we base64 encode or just escape as string
            let arr = array.as_binary::<i32>();
            let bytes = arr.value(row_idx);
            // Try to interpret as UTF-8 first
            if let Ok(s) = std::str::from_utf8(bytes) {
                escape_json_string(s)
            } else {
                // Fall back to hex encoding for non-UTF8 binary
                use std::fmt::Write;
                let mut hex = String::with_capacity(bytes.len() * 2 + 2);
                hex.push('"');
                for b in bytes {
                    let _ = write!(hex, "{b:02x}");
                }
                hex.push('"');
                hex
            }
        }
        DataType::Struct(_) => {
            let arr = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to StructArray".to_string())
            })?;
            struct_value_to_json_string(arr, row_idx)?
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                Error::ArrowSerialize("Failed to downcast to ListArray".to_string())
            })?;
            list_value_to_json_string(arr, row_idx)?
        }
        dt => {
            return Err(Error::ArrowSerialize(format!(
                "Unsupported data type for JSON assembly: {dt:?}"
            )));
        }
    };

    Ok(result)
}

/// Escapes a string for JSON output.
fn escape_json_string(s: &str) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(s.len() + 2);
    result.push('"');
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(result, "\\u{:04x}", c as u32);
            }
            c => result.push(c),
        }
    }
    result.push('"');
    result
}

/// Sets a value at a nested path in a JSON object string builder.
///
/// This function handles nested paths like `["nested", "field"]` by creating
/// intermediate objects as needed.
fn set_nested_value(obj: &mut String, path: &[String], value: &str, is_first: &mut bool) {
    if path.is_empty() {
        return;
    }

    if !*is_first {
        obj.push(',');
    }
    *is_first = false;

    if path.len() == 1 {
        // Simple case: just set the key
        obj.push('"');
        obj.push_str(&path[0]);
        obj.push_str("\":");
        obj.push_str(value);
    } else {
        // Nested case: we need to build nested objects
        // For simplicity in this implementation, we flatten nested paths
        // A more sophisticated implementation would build proper nested JSON
        let key = path.join(".");
        obj.push('"');
        obj.push_str(&key);
        obj.push_str("\":");
        obj.push_str(value);
    }
}

/// Assembles JSON strings from subcolumns for a given group.
///
/// Takes a [`RecordBatch`] and a group of subcolumns, and produces a [`StringArray`]
/// containing JSON objects assembled from the subcolumn values.
///
/// # Errors
///
/// Returns an error if any value cannot be converted to JSON.
pub fn assemble_json_column(batch: &RecordBatch, group: &JsonColumnGroup) -> Result<StringArray> {
    let num_rows = batch.num_rows();
    let mut json_strings: Vec<String> = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut obj = String::with_capacity(64);
        obj.push('{');
        let mut is_first = true;

        for (path, col_idx) in &group.subcolumns {
            let column = batch.column(*col_idx);
            let value = arrow_value_to_json_string(column.as_ref(), row_idx)?;
            set_nested_value(&mut obj, path, &value, &mut is_first);
        }

        obj.push('}');
        json_strings.push(obj);
    }

    Ok(StringArray::from(json_strings))
}

/// Preprocesses a [`RecordBatch`] to assemble JSON subcolumns.
///
/// This function detects columns with dot notation in their names, groups them
/// by base name, assembles them into JSON strings, and returns a new [`RecordBatch`]
/// with the assembled JSON columns replacing the original subcolumns.
///
/// # Arguments
///
/// * `batch` - The input [`RecordBatch`] containing potentially flattened JSON columns.
///
/// # Returns
///
/// A new [`RecordBatch`] where subcolumns have been assembled into JSON columns.
/// Returns the original batch unchanged if no JSON subcolumns are detected.
///
/// # Errors
///
/// Returns an error if JSON assembly fails for any column.
pub fn preprocess_json_subcolumns(batch: RecordBatch) -> Result<RecordBatch> {
    let groups = detect_json_groups(&batch);

    if groups.is_empty() {
        return Ok(batch);
    }

    // Collect indices of columns that are part of JSON groups
    let mut subcolumn_indices: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for group in &groups {
        for (_, idx) in &group.subcolumns {
            let _ = subcolumn_indices.insert(*idx);
        }
    }

    // Build new schema and columns
    let mut new_fields: Vec<Field> = Vec::new();
    let mut new_columns: Vec<ArrayRef> = Vec::new();

    // First, add columns that are not part of any JSON group
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if !subcolumn_indices.contains(&idx) {
            new_fields.push(field.as_ref().clone());
            new_columns.push(Arc::clone(batch.column(idx)));
        }
    }

    // Then, add the assembled JSON columns
    for group in &groups {
        let json_array = assemble_json_column(&batch, group)?;
        new_fields.push(Field::new(&group.base_name, DataType::Utf8, false));
        new_columns.push(Arc::new(json_array) as ArrayRef);
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| Error::ArrowSerialize(format!("Failed to create RecordBatch: {e}")))
}

/// Preprocesses a [`RecordBatch`] to convert Struct columns to JSON strings.
///
/// This function detects columns with `DataType::Struct`, converts them to JSON
/// strings, and returns a new [`RecordBatch`] with the converted columns.
///
/// # Arguments
///
/// * `batch` - The input [`RecordBatch`] containing Struct columns.
///
/// # Returns
///
/// A new [`RecordBatch`] where Struct columns have been converted to JSON string columns.
/// Returns the original batch unchanged if no Struct columns are detected.
///
/// # Errors
///
/// Returns an error if JSON conversion fails for any column.
pub fn preprocess_struct_columns(batch: RecordBatch) -> Result<RecordBatch> {
    let struct_cols = detect_struct_columns(&batch);

    if struct_cols.is_empty() {
        return Ok(batch);
    }

    // Build a map from column index to struct column info for O(1) lookup
    let struct_col_map: HashMap<usize, &StructColumnInfo> =
        struct_cols.iter().map(|c| (c.column_index, c)).collect();

    // Build new schema and columns
    let mut new_fields: Vec<Field> = Vec::new();
    let mut new_columns: Vec<ArrayRef> = Vec::new();

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        if let Some(struct_col) = struct_col_map.get(&idx) {
            // Convert struct column to JSON
            let column = batch.column(idx);
            let struct_array = column.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                Error::ArrowSerialize(format!(
                    "Failed to downcast column {} to StructArray",
                    struct_col.name
                ))
            })?;
            let json_array = struct_array_to_json(struct_array)?;
            new_fields.push(Field::new(&struct_col.name, DataType::Utf8, field.is_nullable()));
            new_columns.push(Arc::new(json_array) as ArrayRef);
        } else {
            // Keep non-struct columns as-is
            new_fields.push(field.as_ref().clone());
            new_columns.push(Arc::clone(batch.column(idx)));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| Error::ArrowSerialize(format!("Failed to create RecordBatch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn test_parse_json_column_name_simple() {
        let path = parse_json_column_name("labels_json.id").unwrap();
        assert_eq!(path.base_name, "labels_json");
        assert_eq!(path.subpath, vec!["id"]);
    }

    #[test]
    fn test_parse_json_column_name_nested() {
        let path = parse_json_column_name("data.nested.field").unwrap();
        assert_eq!(path.base_name, "data");
        assert_eq!(path.subpath, vec!["nested", "field"]);
    }

    #[test]
    fn test_parse_json_column_name_no_dot() {
        assert!(parse_json_column_name("simple_column").is_none());
    }

    #[test]
    fn test_parse_json_column_name_empty_subpath() {
        // Edge case: trailing dot
        let path = parse_json_column_name("base.").unwrap();
        assert_eq!(path.base_name, "base");
        assert_eq!(path.subpath, vec![""]);
    }

    #[test]
    fn test_detect_json_groups() {
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("labels_json.id", DataType::Int64, true),
            Field::new("labels_json.name", DataType::Utf8, true),
            Field::new("other.field", DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("test")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
            ],
        )
        .unwrap();

        let groups = detect_json_groups(&batch);
        assert_eq!(groups.len(), 2);

        // Find the labels_json group
        let labels_group = groups.iter().find(|g| g.base_name == "labels_json").unwrap();
        assert_eq!(labels_group.subcolumns.len(), 2);
    }

    #[test]
    fn test_escape_json_string() {
        assert_eq!(escape_json_string("hello"), "\"hello\"");
        assert_eq!(escape_json_string("hello\"world"), "\"hello\\\"world\"");
        assert_eq!(escape_json_string("line1\nline2"), "\"line1\\nline2\"");
        assert_eq!(escape_json_string("tab\there"), "\"tab\\there\"");
    }

    #[test]
    fn test_assemble_json_column() {
        let schema = Schema::new(vec![
            Field::new("labels_json.id", DataType::Int64, true),
            Field::new("labels_json.name", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("test"), Some("prod")])) as ArrayRef,
            ],
        )
        .unwrap();

        let groups = detect_json_groups(&batch);
        let group = groups.iter().find(|g| g.base_name == "labels_json").unwrap();

        let json_array = assemble_json_column(&batch, group).unwrap();
        assert_eq!(json_array.len(), 2);

        // Check that the JSON is valid (contains expected keys)
        let row0 = json_array.value(0);
        assert!(row0.contains("\"id\":1"));
        assert!(row0.contains("\"name\":\"test\""));

        let row1 = json_array.value(1);
        assert!(row1.contains("\"id\":2"));
        assert!(row1.contains("\"name\":\"prod\""));
    }

    #[test]
    fn test_assemble_json_column_with_nulls() {
        let schema = Schema::new(vec![
            Field::new("data.id", DataType::Int64, true),
            Field::new("data.value", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![None, Some("test")])) as ArrayRef,
            ],
        )
        .unwrap();

        let groups = detect_json_groups(&batch);
        let group = groups.iter().find(|g| g.base_name == "data").unwrap();

        let json_array = assemble_json_column(&batch, group).unwrap();

        let row0 = json_array.value(0);
        assert!(row0.contains("\"id\":1"));
        assert!(row0.contains("\"value\":null"));

        let row1 = json_array.value(1);
        assert!(row1.contains("\"id\":null"));
        assert!(row1.contains("\"value\":\"test\""));
    }

    #[test]
    fn test_preprocess_json_subcolumns() {
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("labels_json.id", DataType::Int64, true),
            Field::new("labels_json.name", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1000, 2000])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("test"), Some("prod")])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = preprocess_json_subcolumns(batch).unwrap();

        // Should have 2 columns: timestamp and labels_json
        assert_eq!(result.num_columns(), 2);

        // Check column names
        let schema = result.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"timestamp"));
        assert!(field_names.contains(&"labels_json"));

        // Verify the JSON column
        let json_col_idx = schema.index_of("labels_json").unwrap();
        let json_col = result.column(json_col_idx).as_string::<i32>();
        assert_eq!(json_col.len(), 2);
    }

    #[test]
    fn test_preprocess_no_json_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ],
        )
        .unwrap();

        let result = preprocess_json_subcolumns(batch.clone()).unwrap();

        // Should return the same batch unchanged
        assert_eq!(result.num_columns(), 2);
        assert_eq!(result.schema(), batch.schema());
    }

    #[test]
    fn test_detect_struct_columns() {
        let struct_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("metadata", DataType::Struct(struct_fields.clone()), true),
        ]);

        let name_array = StringArray::from(vec!["Alice", "Bob"]);
        let age_array = Int64Array::from(vec![30, 25]);
        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("name", DataType::Utf8, false)), Arc::new(name_array) as ArrayRef),
            (Arc::new(Field::new("age", DataType::Int64, false)), Arc::new(age_array) as ArrayRef),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(struct_array) as ArrayRef,
            ],
        )
        .unwrap();

        let struct_cols = detect_struct_columns(&batch);
        assert_eq!(struct_cols.len(), 1);
        assert_eq!(struct_cols[0].name, "metadata");
        assert_eq!(struct_cols[0].column_index, 1);
    }

    #[test]
    fn test_struct_array_to_json() {
        let name_array = StringArray::from(vec!["Alice", "Bob"]);
        let age_array = Int64Array::from(vec![30, 25]);
        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("name", DataType::Utf8, false)), Arc::new(name_array) as ArrayRef),
            (Arc::new(Field::new("age", DataType::Int64, false)), Arc::new(age_array) as ArrayRef),
        ]);

        let json_array = struct_array_to_json(&struct_array).unwrap();
        assert_eq!(json_array.len(), 2);

        let row0 = json_array.value(0);
        assert!(row0.contains("\"name\":\"Alice\""));
        assert!(row0.contains("\"age\":30"));

        let row1 = json_array.value(1);
        assert!(row1.contains("\"name\":\"Bob\""));
        assert!(row1.contains("\"age\":25"));
    }

    #[test]
    fn test_preprocess_struct_columns() {
        let struct_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, false),
        ]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Struct(struct_fields), true),
        ]);

        let name_array = StringArray::from(vec!["test", "prod"]);
        let count_array = Int64Array::from(vec![10, 20]);
        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("name", DataType::Utf8, false)), Arc::new(name_array) as ArrayRef),
            (
                Arc::new(Field::new("count", DataType::Int64, false)),
                Arc::new(count_array) as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(struct_array) as ArrayRef,
            ],
        )
        .unwrap();

        let result = preprocess_struct_columns(batch).unwrap();

        // Should have 2 columns: id and data (now as string)
        assert_eq!(result.num_columns(), 2);

        let schema = result.schema();
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "data");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        // Check the JSON content
        let json_col = result.column(1).as_string::<i32>();
        let row0 = json_col.value(0);
        assert!(row0.contains("\"name\":\"test\""));
        assert!(row0.contains("\"count\":10"));
    }

    #[test]
    fn test_nested_struct_to_json() {
        // Test nested struct: {inner: {value: 42}}
        let inner_fields = Fields::from(vec![Field::new("value", DataType::Int64, false)]);

        let value_array = Int64Array::from(vec![42]);
        let inner_struct = StructArray::from(vec![(
            Arc::new(Field::new("value", DataType::Int64, false)),
            Arc::new(value_array) as ArrayRef,
        )]);
        let outer_struct = StructArray::from(vec![(
            Arc::new(Field::new("inner", DataType::Struct(inner_fields), false)),
            Arc::new(inner_struct) as ArrayRef,
        )]);

        let json_array = struct_array_to_json(&outer_struct).unwrap();
        assert_eq!(json_array.len(), 1);

        let row0 = json_array.value(0);
        // Should contain nested JSON structure
        assert!(row0.contains("\"inner\":{\"value\":42}"));
    }
}
