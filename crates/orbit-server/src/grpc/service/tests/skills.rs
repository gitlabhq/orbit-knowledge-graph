use super::*;
use sha2::{Digest, Sha256};

fn sha256_hex(content: &str) -> String {
    Sha256::digest(content.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[tokio::test]
async fn list_skills_returns_deployed_skill_metadata() {
    let response = test_service()
        .list_skills(authed_request(ListSkillsRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.skills.len(), 1);
    let skill = &response.skills[0];
    assert_eq!(skill.name, "orbit");
    assert_eq!(skill.version, "0.31.0");
    assert!(skill.description.contains("glab orbit"));
    assert!(skill.compatibility.contains("glab v1.117.0"));
    assert_eq!(response.server_version, orbit_utils::version::get());
}

#[tokio::test]
async fn get_skill_returns_sorted_tree_with_file_hashes() {
    let service = test_service();
    let listed = service
        .list_skills(authed_request(ListSkillsRequest {}))
        .await
        .unwrap()
        .into_inner();
    let response = service
        .get_skill(authed_request(GetSkillRequest {
            name: "orbit".into(),
            metadata_only: false,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.name, "orbit");
    assert_eq!(response.version, listed.skills[0].version);
    assert_eq!(response.compatibility, listed.skills[0].compatibility);
    assert_eq!(response.server_version, listed.server_version);
    assert_eq!(response.files.len(), 9);
    assert_eq!(response.files[0].path, "SKILL.md");
    assert!(
        response
            .files
            .windows(2)
            .all(|pair| pair[0].path < pair[1].path)
    );
    for file in response.files {
        assert_eq!(file.sha256, sha256_hex(&file.content), "{}", file.path);
    }
}

#[tokio::test]
async fn get_skill_metadata_only_omits_files() {
    let response = test_service()
        .get_skill(authed_request(GetSkillRequest {
            name: "orbit".into(),
            metadata_only: true,
        }))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.name, "orbit");
    assert_eq!(response.version, "0.31.0");
    assert!(response.compatibility.contains("glab v1.117.0"));
    assert_eq!(response.server_version, orbit_utils::version::get());
    assert!(response.files.is_empty());
}

#[tokio::test]
async fn get_skill_unknown_name_lists_sorted_known_names() {
    let error = test_service()
        .get_skill(authed_request(GetSkillRequest {
            name: "missing".into(),
            metadata_only: false,
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::NotFound);
    assert_eq!(
        error.message(),
        "Unknown skill \"missing\". Known skills: [\"orbit\"]"
    );
}
