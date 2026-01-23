//! ## Logic for interfacing between Arrow and `ClickHouse`
pub mod block;
mod builder;
mod deserialize;
pub mod json_assembly;
pub mod json_flatten;
pub(crate) mod schema;
pub(crate) mod serialize;
pub(crate) mod types;
pub mod utils;

// Re-exports
pub use arrow;
pub use block::{SerializedBatch, serialize_record_batch, serialize_record_batch_compressed};
pub(crate) use deserialize::ArrowDeserializerState;
pub use json_assembly::{
    JsonColumnGroup, JsonPath, StructColumnInfo, assemble_json_column, detect_json_groups,
    detect_struct_columns, parse_json_column_name, preprocess_json_subcolumns,
    preprocess_struct_columns, struct_array_to_json,
};
pub use json_flatten::{FlattenedJsonPaths, create_flattened_paths, flatten_to_paths};
pub use types::ch_to_arrow_type;
