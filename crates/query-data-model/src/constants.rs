pub const DEFAULT_PRIMARY_KEY: &str = "id";

pub const EDGE_RESERVED_COLUMNS: &[&str] = &[
    TRAVERSAL_PATH_COLUMN,
    RELATIONSHIP_KIND_COLUMN,
    SOURCE_ID_COLUMN,
    SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN,
    TARGET_KIND_COLUMN,
];

pub const RELATIONSHIP_KIND_COLUMN: &str = "relationship_kind";

pub const SOURCE_ID_COLUMN: &str = "source_id";

pub const SOURCE_KIND_COLUMN: &str = "source_kind";

pub const TARGET_ID_COLUMN: &str = "target_id";

pub const TARGET_KIND_COLUMN: &str = "target_kind";

pub const SOURCE_TAGS_COLUMN: &str = "source_tags";

pub const TARGET_TAGS_COLUMN: &str = "target_tags";

pub const VERSION_COLUMN: &str = "_version";

pub const DELETED_COLUMN: &str = "_deleted";

pub const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";
