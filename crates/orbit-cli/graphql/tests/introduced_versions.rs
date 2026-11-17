use std::fs;
use std::path::Path;

use orbit_cli_graphql::ORBIT_SINCE;

const TAG: &str = "@gl_introduced(version: \"";

#[test]
fn only_fields_newer_than_the_orbit_floor_are_tagged() {
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
            let tag = semver::Version::parse(version).expect("tag version");
            assert!(
                (tag.major, tag.minor) > (major, minor),
                "{}: tag {version} is not newer than the Orbit floor {major}.{minor}, \
                 which already serves the field; remove the tag",
                path.display(),
            );
        }
    }
    assert!(tagged > 0, "no tagged fields in {}", dir.display());
}
