use std::time::Duration;

use indexing_engine::{ClusterPoolConfig, EngineConfig, PoolConfig};

#[test]
fn reads_lanes_and_pools_with_text_durations() {
    let config: EngineConfig = serde_json::from_str(
        r#"{
            "lanes": { "doc.index/default": { "slots": 16, "timeout": "15m", "on_timeout": "doc.index/big" } },
            "pools": {
                "parse": 10,
                "store_reads": { "cluster": true, "per_key": 8, "unknown_key": 4, "max_wait": "60s" }
            }
        }"#,
    )
    .unwrap();

    let lane = &config.lanes["doc.index/default"];
    assert_eq!(lane.timeout, Duration::from_secs(15 * 60));
    assert_eq!(lane.max_age, Duration::from_secs(24 * 60 * 60));
    assert_eq!(config.pools["parse"], PoolConfig::Pod(10));
    assert_eq!(
        config.pools["store_reads"],
        PoolConfig::Cluster(ClusterPoolConfig {
            cluster: true,
            per_key: 8,
            unknown_key: 4,
            max_wait: Duration::from_secs(60),
        })
    );
}

#[test]
fn rejects_a_duration_without_a_unit() {
    let config = serde_json::from_str::<EngineConfig>(
        r#"{ "lanes": { "a/default": { "slots": 1, "timeout": "15" } } }"#,
    );

    assert!(config.is_err());
}
