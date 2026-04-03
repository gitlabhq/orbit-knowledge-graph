pub const SCHEMA_DDL: &str = include_str!(concat!(env!("CONFIG_DIR"), "/graph_local.sql"));

pub const CODE_GRAPH_TABLES: &[&str] = &[
    "gl_directory",
    "gl_file",
    "gl_definition",
    "gl_imported_symbol",
    "gl_edge",
];
