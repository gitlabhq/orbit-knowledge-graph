use std::fs;
use std::path::Path;

use orbit_cli_graphql::ORBIT_SINCE;

const TAG: &str = "@gl_introduced(version: \"";

fn release_line(version: &str) -> (u64, u64) {
    let mut parts = version.split('.').map(|part| {
        part.parse::<u64>()
            .unwrap_or_else(|_| panic!("{version}: not a version"))
    });
    (parts.next().expect("major"), parts.next().expect("minor"))
}

#[test]
fn only_fields_newer_than_the_first_orbit_release_are_tagged() {
    let (major, minor, _) = ORBIT_SINCE[0];
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("queries");
    let mut tagged = 0;
    for entry in fs::read_dir(&dir).expect("queries dir") {
        let path = entry.expect("query file").path();
        let query = fs::read_to_string(&path).expect("query text");
        for (start, _) in query.match_indices(TAG) {
            let version = query[start + TAG.len()..]
                .split('"')
                .next()
                .expect("closing quote");
            tagged += 1;
            assert!(
                release_line(version) > (major, minor),
                "{}: a field tagged {version} is stripped on GitLab {major}.{minor}.x, \
                 which serves every untagged Orbit field",
                path.display(),
            );
        }
    }
    assert!(tagged > 0, "no tagged fields in {}", dir.display());
}
