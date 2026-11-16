use std::fs;
use std::path::{Path, PathBuf};

const FIRST_ORBIT_RELEASE: (u32, u32) = (19, 4);
const TAG: &str = "@gl_introduced(version: \"";

struct Tag {
    path: PathBuf,
    field: String,
    version: (u32, u32),
}

fn crate_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn major_minor(version: &str) -> (u32, u32) {
    let mut parts = version.split('.').map(|part| {
        part.parse::<u32>()
            .unwrap_or_else(|_| panic!("{version}: not a version"))
    });
    (parts.next().expect("major"), parts.next().expect("minor"))
}

fn query_tags() -> Vec<Tag> {
    let dir = crate_dir().join("queries");
    let mut tags = Vec::new();
    for entry in fs::read_dir(&dir).expect("queries dir") {
        let path = entry.expect("query file").path();
        let text = fs::read_to_string(&path).expect("query text");
        let query = text.split_whitespace().collect::<Vec<_>>().join(" ");
        for (start, _) in query.match_indices(TAG) {
            let field = query[..start]
                .split_whitespace()
                .next_back()
                .expect("tagged field");
            let version = query[start + TAG.len()..]
                .split('"')
                .next()
                .expect("closing quote");
            tags.push(Tag {
                path: path.clone(),
                field: field.to_string(),
                version: major_minor(version),
            });
        }
    }
    assert!(
        tags.iter().any(|tag| tag.field == "orbit"),
        "no tagged orbit selections in {}",
        dir.display()
    );
    tags
}

#[test]
fn orbit_selections_are_kept_on_the_first_orbit_release() {
    for tag in query_tags().iter().filter(|tag| tag.field == "orbit") {
        assert!(
            tag.version < FIRST_ORBIT_RELEASE,
            "{}: orbit is tagged {}.{}, which GitLab {}.{}.x strips",
            tag.path.display(),
            tag.version.0,
            tag.version.1,
            FIRST_ORBIT_RELEASE.0,
            FIRST_ORBIT_RELEASE.1,
        );
    }
}

#[test]
fn vendored_schema_fields_are_kept_on_its_gitlab_release() {
    let text =
        fs::read_to_string(crate_dir().join("schema/GITLAB_VERSION")).expect("GITLAB_VERSION");
    let schema = major_minor(text.trim());
    for tag in query_tags() {
        assert!(
            tag.version < schema,
            "{}: {} is tagged {}.{}, which GitLab {}.{}.x strips, but the vendored {}.{} schema serves it",
            tag.path.display(),
            tag.field,
            tag.version.0,
            tag.version.1,
            schema.0,
            schema.1,
            schema.0,
            schema.1,
        );
    }
}
