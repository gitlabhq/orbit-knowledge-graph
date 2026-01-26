CREATE TABLE IF NOT EXISTS test.siphon_users (
    id Int64,
    username Nullable(String),
    email Nullable(String),
    name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    state Nullable(String),
    public_email Nullable(String),
    preferred_language Nullable(String),
    last_activity_on Nullable(String),
    private_profile Nullable(Bool),
    admin Nullable(Bool),
    auditor Nullable(Bool),
    external Nullable(Bool),
    user_type Nullable(Int16),
    created_at Nullable(String),
    updated_at Nullable(String),
    _siphon_replicated_at DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS test.users (
    id Int64,
    username Nullable(String),
    email Nullable(String),
    name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    state Nullable(String),
    public_email Nullable(String),
    preferred_language Nullable(String),
    last_activity_on Nullable(String),
    private_profile Nullable(Bool),
    is_admin Nullable(Bool),
    is_auditor Nullable(Bool),
    is_external Nullable(Bool),
    user_type Nullable(String),
    created_at Nullable(String),
    updated_at Nullable(String)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE IF NOT EXISTS test.user_indexing_watermark (
    watermark DateTime64(3),
    _version DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(_version) ORDER BY tuple();
