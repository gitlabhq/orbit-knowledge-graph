use crate::config::GenerationConfig;
use synthetic_graph::ids::SeqIdAllocator;

#[derive(Clone)]
pub struct UserSeed {
    pub id: i64,
}

#[derive(Clone)]
pub struct GroupSeed {
    pub id: i64,
    pub namespace_id: i64,
    pub traversal_path: String,
    pub parent_namespace_id: Option<i64>,
    pub organization_id: i64,
}

#[derive(Clone)]
pub struct ProjectSeed {
    pub id: i64,
    pub namespace_id: i64,
    pub parent_namespace_id: i64,
    pub traversal_path: String,
    pub organization_id: i64,
}

#[derive(Clone)]
pub struct Foundation {
    pub users: Vec<UserSeed>,
    pub groups: Vec<GroupSeed>,
    pub projects: Vec<ProjectSeed>,
    pub root_group_namespace_ids: Vec<i64>,
    pub next_entity_id: i64,
    pub next_namespace_id: i64,
}

pub fn build_foundation(config: &GenerationConfig) -> Foundation {
    let mut entity_ids = SeqIdAllocator::new(1);
    let mut namespace_ids = SeqIdAllocator::new(1);
    let mut users = Vec::with_capacity(config.users);
    let mut groups = Vec::new();
    let mut projects = Vec::new();
    let mut root_group_namespace_ids = Vec::new();
    let organization_id = config.organizations as i64;

    for _ in 0..config.users {
        users.push(UserSeed {
            id: entity_ids.allocate(),
        });
    }

    for _ in 0..config.groups {
        let root_namespace_id = namespace_ids.allocate();
        let root_path = format!("{}/{}/", organization_id, root_namespace_id);
        groups.push(GroupSeed {
            id: entity_ids.allocate(),
            namespace_id: root_namespace_id,
            traversal_path: root_path.clone(),
            parent_namespace_id: None,
            organization_id,
        });
        root_group_namespace_ids.push(root_namespace_id);
        append_subgroups(
            &mut entity_ids,
            &mut namespace_ids,
            &mut groups,
            &root_path,
            root_namespace_id,
            organization_id,
            config.subgroups.max_depth,
            config.subgroups.per_group,
            1,
        );
    }

    for group in &groups {
        for _ in 0..config.per_group.projects {
            let project_namespace_id = namespace_ids.allocate();
            let project_id = entity_ids.allocate();
            projects.push(ProjectSeed {
                id: project_id,
                namespace_id: project_namespace_id,
                parent_namespace_id: group.namespace_id,
                traversal_path: format!("{}{}/", group.traversal_path, project_id),
                organization_id: group.organization_id,
            });
        }
    }

    Foundation {
        users,
        groups,
        projects,
        root_group_namespace_ids,
        next_entity_id: entity_ids.current(),
        next_namespace_id: namespace_ids.current(),
    }
}

#[allow(clippy::too_many_arguments)]
fn append_subgroups(
    entity_ids: &mut SeqIdAllocator,
    namespace_ids: &mut SeqIdAllocator,
    groups: &mut Vec<GroupSeed>,
    parent_path: &str,
    parent_namespace_id: i64,
    organization_id: i64,
    max_depth: usize,
    per_group: usize,
    depth: usize,
) {
    if depth > max_depth {
        return;
    }

    for _ in 0..per_group {
        let namespace_id = namespace_ids.allocate();
        let path = format!("{}{}/", parent_path, namespace_id);
        groups.push(GroupSeed {
            id: entity_ids.allocate(),
            namespace_id,
            traversal_path: path.clone(),
            parent_namespace_id: Some(parent_namespace_id),
            organization_id,
        });
        append_subgroups(
            entity_ids,
            namespace_ids,
            groups,
            &path,
            namespace_id,
            organization_id,
            max_depth,
            per_group,
            depth + 1,
        );
    }
}
