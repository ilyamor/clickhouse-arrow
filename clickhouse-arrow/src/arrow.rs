//! ## Logic for interfacing between Arrow and `ClickHouse`
pub mod block;
mod builder;
mod deserialize;
pub mod json_assembly;
pub(crate) mod schema;
mod serialize;
pub(crate) mod types;
pub mod utils;

// Re-exports
pub use arrow;
pub(crate) use deserialize::ArrowDeserializerState;
pub use json_assembly::{
    assemble_json_column, detect_json_groups, parse_json_column_name, preprocess_json_subcolumns,
    JsonColumnGroup, JsonPath,
};
pub use types::ch_to_arrow_type;
