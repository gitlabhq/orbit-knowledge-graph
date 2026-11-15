use std::fs;
use std::path::Path;

const FIRST_ORBIT_RELEASE: (u32, u32) = (19, 4);
const ORBIT_TAG: &str = "orbit @gl_introduced(version: \"";

fn minor(version: &str) -> (u32, u32) {
    let mut parts = version.split('.').map(|part| {
        part.parse::<u32>()
            .unwrap_or_else(|_| panic!("{version}: not a version"))
    });
    (parts.next().expect("major"), parts.next().expect("minor"))
}

#[test]
fn orbit_selections_are_kept_on_the_first_orbit_release() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("queries");
    let mut tagged = 0;
    for entry in fs::read_dir(&dir).expect("queries dir") {
        let path = entry.expect("query file").path();
        let query = fs::read_to_string(&path).expect("query text");
        for (start, _) in query.match_indices(ORBIT_TAG) {
            let version = query[start + ORBIT_TAG.len()..]
                .split('"')
                .next()
                .expect("closing quote");
            tagged += 1;
            assert!(
                minor(version) < FIRST_ORBIT_RELEASE,
                "{}: orbit is tagged {version}, which GitLab {}.{}.x strips",
                path.display(),
                FIRST_ORBIT_RELEASE.0,
                FIRST_ORBIT_RELEASE.1,
            );
        }
    }
    assert!(
        tagged > 0,
        "no tagged orbit selections in {}",
        dir.display()
    );
}
