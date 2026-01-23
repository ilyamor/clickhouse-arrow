#![allow(unused_crate_dependencies)]

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] = &[
    ("testcontainers", "debug"),
    // --
    ("arrow", "debug"),
];

// Test arrow e2e no compression
#[cfg(feature = "test-utils")]
e2e_test!(e2e_arrow_none, tests::arrow::test_round_trip_none, TRACING_DIRECTIVES, None);

// Test arrow e2e lz4
#[cfg(feature = "test-utils")]
e2e_test!(e2e_arrow_lz4, tests::arrow::test_round_trip_lz4, TRACING_DIRECTIVES, None);

// Test arrow e2e zstd
#[cfg(feature = "test-utils")]
e2e_test!(
    e2e_arrow_zstd_large_data,
    tests::arrow::test_round_trip_zstd_large_data,
    TRACING_DIRECTIVES,
    None
);

// Test arrow e2e no compression
#[cfg(feature = "test-utils")]
e2e_test!(
    e2e_arrow_none_large_data,
    tests::arrow::test_round_trip_none_large_data,
    TRACING_DIRECTIVES,
    None
);

// Test arrow e2e lz4
#[cfg(feature = "test-utils")]
e2e_test!(
    e2e_arrow_lz4_large_data,
    tests::arrow::test_round_trip_lz4_large_data,
    TRACING_DIRECTIVES,
    None
);

// Test arrow e2e zstd
#[cfg(feature = "test-utils")]
e2e_test!(e2e_arrow_zstd, tests::arrow::test_round_trip_zstd, TRACING_DIRECTIVES, None);

// Test arrow schema utils
#[cfg(feature = "test-utils")]
e2e_test!(e2e_arrow_schema, tests::arrow::test_schema_utils, TRACING_DIRECTIVES, None);

// Test arrow execute scalar/settings
#[cfg(feature = "test-utils")]
e2e_test!(e2e_arrow_execute, tests::arrow::test_execute_queries, TRACING_DIRECTIVES, None);

// Test ClickHouse nullable array support
#[cfg(feature = "test-utils")]
e2e_test!(
    e2e_arrow_nullable_array_support,
    tests::arrow::test_clickhouse_nullable_array_support,
    TRACING_DIRECTIVES,
    None
);

// Test nullable array serialization
#[cfg(feature = "test-utils")]
e2e_test!(
    e2e_arrow_nullable_array,
    tests::arrow::test_nullable_array_serialization,
    TRACING_DIRECTIVES,
    None
);

// Test named tuple field parsing (issue #85)
#[cfg(feature = "test-utils")]
e2e_test!(e2e_arrow_named_tuple, tests::arrow::test_named_tuple_schema, TRACING_DIRECTIVES, None);

// Test ClientFactory basic operations
#[cfg(feature = "test-utils")]
e2e_test!(
    e2e_factory_basic,
    tests::factory::test_factory_basic_operations,
    TRACING_DIRECTIVES,
    None
);

// Test ClientFactory insert operations
#[cfg(feature = "test-utils")]
e2e_test!(e2e_factory_insert, tests::factory::test_factory_insert, TRACING_DIRECTIVES, None);

// Test ClientFactory multiple connections
#[cfg(feature = "test-utils")]
e2e_test!(
    e2e_factory_multiple_connections,
    tests::factory::test_factory_multiple_connections,
    TRACING_DIRECTIVES,
    None
);

// Test oneshot query
#[cfg(feature = "test-utils")]
e2e_test!(e2e_oneshot_query, tests::oneshot::test_oneshot_query, TRACING_DIRECTIVES, None);

// Test oneshot scalar
#[cfg(feature = "test-utils")]
e2e_test!(e2e_oneshot_scalar, tests::oneshot::test_oneshot_scalar, TRACING_DIRECTIVES, None);

// Test oneshot insert
#[cfg(feature = "test-utils")]
e2e_test!(e2e_oneshot_insert, tests::oneshot::test_oneshot_insert, TRACING_DIRECTIVES, None);
