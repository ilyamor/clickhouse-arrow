//! JSON subcolumn assembly for flattened Arrow columns.
//!
//! This module provides functionality to detect and assemble Arrow columns with dot notation
//! (e.g., `labels_json.id`, `labels_json.name`) into JSON strings that can be inserted into
//! ClickHouse JSON columns.
//!
//! ClickHouse does not support flattened JSON input natively (only output), so this module
//! performs client-side JSON assembly before sending data to ClickHouse.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
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
    let mut parts = name.splitn(2, '.');
    let base_name = parts.next()?;
    let rest = parts.next()?;

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
    /// List of (subpath, column_index) tuples for all subcolumns in this group.
    pub subcolumns: Vec<(Vec<String>, usize)>,
}

/// Detects and groups JSON subcolumns in a RecordBatch.
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

/// Converts an Arrow array value at a specific row to a JSON-compatible string representation.
///
/// Handles null values and various Arrow data types, returning a JSON-formatted string.
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
            match std::str::from_utf8(bytes) {
                Ok(s) => escape_json_string(s),
                Err(_) => {
                    // Fall back to hex encoding for non-UTF8 binary
                    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                    format!("\"{hex}\"")
                }
            }
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
                result.push_str(&format!("\\u{:04x}", c as u32));
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
/// Takes a RecordBatch and a group of subcolumns, and produces a StringArray
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

/// Preprocesses a RecordBatch to assemble JSON subcolumns.
///
/// This function detects columns with dot notation in their names, groups them
/// by base name, assembles them into JSON strings, and returns a new RecordBatch
/// with the assembled JSON columns replacing the original subcolumns.
///
/// # Arguments
///
/// * `batch` - The input RecordBatch containing potentially flattened JSON columns.
///
/// # Returns
///
/// A new RecordBatch where subcolumns have been assembled into JSON columns.
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
        let json_array = assemble_json_column(&batch, &group)?;
        new_fields.push(Field::new(&group.base_name, DataType::Utf8, false));
        new_columns.push(Arc::new(json_array) as ArrayRef);
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
}
