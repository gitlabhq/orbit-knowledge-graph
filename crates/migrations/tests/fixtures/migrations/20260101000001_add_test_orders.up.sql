CREATE TABLE test_orders (
    id UInt64,
    user_id UInt64,
    total Float64
) ENGINE = MergeTree
ORDER BY id;
