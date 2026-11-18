use std::time::Duration;

use indexing_engine::{ClusterPoolConfig, EngineConfig, LaneConfig, PoolConfig};

fn read_lane_timeout(timeout: &str) -> Result<Duration, String> {
    let json =
        format!(r#"{{ "lanes": {{ "a/default": {{ "slots": 1, "timeout": "{timeout}" }} }} }}"#);
    serde_json::from_str::<EngineConfig>(&json)
        .map(|config| config.lanes["a/default"].timeout)
        .map_err(|error| error.to_string())
}

#[test]
fn the_config_reads_lanes_and_pools_as_helm_writes_them() {
    let config: EngineConfig = serde_json::from_str(
        r#"{
            "lanes": {
                "code.index/default": { "slots": 16, "timeout": "15m", "max_age": "24h", "on_timeout": "code.index/big" }
            },
            "pools": {
                "parse": 10,
                "gitaly_archives": { "cluster": true, "per_key": 8, "unknown_key": 4, "max_wait": "60s" }
            }
        }"#,
    )
    .unwrap();

    let lane = LaneConfig {
        slots: 16,
        timeout: Duration::from_secs(15 * 60),
        max_age: Duration::from_secs(24 * 60 * 60),
        on_timeout: Some("code.index/big".to_owned()),
    };
    let archives = PoolConfig::Cluster(ClusterPoolConfig {
        cluster: true,
        per_key: 8,
        unknown_key: 4,
        max_wait: Duration::from_secs(60),
    });
    let expected = EngineConfig::default()
        .with_lane("code.index/default", lane)
        .with_pool("parse", PoolConfig::Pod(10))
        .with_pool("gitaly_archives", archives);
    assert_eq!(config, expected);
}

#[test]
fn a_duration_is_a_number_with_the_unit_ms_s_m_h_or_d() {
    assert_eq!(read_lane_timeout("250ms"), Ok(Duration::from_millis(250)));
    assert_eq!(read_lane_timeout("30s"), Ok(Duration::from_secs(30)));
    assert_eq!(read_lane_timeout("15m"), Ok(Duration::from_secs(15 * 60)));
    assert_eq!(
        read_lane_timeout("2h"),
        Ok(Duration::from_secs(2 * 60 * 60))
    );
    assert_eq!(
        read_lane_timeout("1d"),
        Ok(Duration::from_secs(24 * 60 * 60))
    );
}

#[test]
fn a_duration_without_a_unit_is_rejected() {
    let error = read_lane_timeout("15").unwrap_err();

    assert!(error.contains("duration \"15\" has no unit"), "{error}");
}

#[test]
fn a_duration_with_an_unknown_unit_is_rejected() {
    let error = read_lane_timeout("15w").unwrap_err();

    assert!(error.contains("must end in ms, s, m, h or d"), "{error}");
}
