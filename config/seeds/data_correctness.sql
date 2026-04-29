-- Data correctness seed: known-good fixture data for integration tests.
--
-- Topology:
--
--   Users:
--     1 alice   (active,  human)
--     2 bob     (active,  human)
--     3 charlie (active,  human)
--     4 diana   (active,  project_bot)
--     5 eve     (blocked, service_account)
--     6 用户_émoji_🎉 (active, human) — unicode stress test
--     7 frank   (active,  human) — variable-length cliff reproducer
--
--   Groups:
--     100 Public Group   (public,   path 1/100/)
--     101 Private Group  (private,  path 1/101/)
--     102 Internal Group (internal, path 1/102/)
--     200 Deep Group A   (public,   path 1/100/200/)
--     300 Deep Group B   (public,   path 1/100/200/300/)
--
--   Projects:
--     1000 Public Project   (public,   path 1/100/1000/)
--     1001 Private Project  (private,  path 1/101/1001/)
--     1002 Internal Project (internal, path 1/100/1002/)
--     1003 Secret Project   (private,  path 1/101/1003/)
--     1004 Shared Project   (public,   path 1/102/1004/)
--     1010 Deep Project     (public,   path 1/100/200/1010/) — under Group 200, depth-2 from Group 100
--
--   MergeRequests:
--     2000 Add feature A (opened, path 1/100/1000/)
--     2001 Fix bug B     (opened, path 1/100/1000/)
--     2002 Refactor C    (merged, path 1/101/1001/)
--     2003 Update D      (closed, path 1/102/1004/)
--
--   MergeRequestDiffs:
--     5000 (MR 2000, collected)
--     5001 (MR 2000, collected)
--     5002 (MR 2001, collected)
--
--   Notes:
--     3000 Normal note           (MR 2000, not confidential, not internal)
--     3001 Confidential note     (MR 2001, confidential=true)
--     3002 Giant string note     (MR 2000, 10000 chars)
--     3003 SQL injection note    (MR 2000, DROP TABLE payload)
--
--   MEMBER_OF edges:
--     User 1 -> Group 100, User 1 -> Group 102
--     User 2 -> Group 100, User 3 -> Group 101
--     User 4 -> Group 101, User 4 -> Group 102, User 5 -> Group 101
--     User 6 -> Group 100, User 6 -> Group 101
--
--   CONTAINS edges:
--     Group 100 -> Project 1000, Group 100 -> Project 1002
--     Group 100 -> Group 200 (subgroup)
--     Group 200 -> Group 300 (subgroup depth 2)
--     Group 200 -> Project 1010 (enables depth>1 Group->Project paths)
--     Group 101 -> Project 1001, Group 101 -> Project 1003
--     Group 102 -> Project 1004
--
--   AUTHORED edges:
--     User 1 -> MR 2000, User 1 -> MR 2001
--     User 2 -> MR 2002, User 3 -> MR 2003
--     User 1 -> Note 3000
--     User 7 -> WI 4010 (reproducer for variable-length cliff)
--
--   HAS_NOTE edges:
--     MR 2000 -> Note 3000, MR 2000 -> Note 3002, MR 2000 -> Note 3003
--     MR 2001 -> Note 3001
--
--   HAS_DIFF edges:
--     MR 2000 -> MergeRequestDiff 5000, MR 2000 -> MergeRequestDiff 5001
--     MR 2001 -> MergeRequestDiff 5002
--
--   Milestones:
--     6000 Sprint 1 (active,  path 1/100/)
--     6001 Sprint 2 (closed,  path 1/101/)
--
--   Labels:
--     7000 bug      (#d73a4a, path 1/100/)
--     7001 feature  (#0075ca, path 1/100/)
--     7002 urgent   (#e4e669, path 1/101/)
--
--   WorkItems:
--     4000 Implement login page  (opened, issue,    not confidential, weight 3,  path 1/100/)
--     4001 Fix auth bug          (closed, incident, confidential,     weight 8,  path 1/100/)
--     4002 Write unit tests      (opened, task,     not confidential, no weight, path 1/101/)
--     4003 Q1 Objective          (opened, epic,     not confidential, weight 13, path 1/102/)
--     4010 Deep WI               (opened, issue,    not confidential, weight 5,  path 1/100/200/1010/) — User 7's WI in Project 1010
--
--   IN_PROJECT edges:
--     WI 4000 -> Project 1000, WI 4001 -> Project 1000
--     WI 4010 -> Project 1010 (depth-2 reproducer)
--
--   APPROVED edges:
--     User 2 -> MR 2000, User 3 -> MR 2000
--     User 1 -> MR 2002
--
--   ─── Organization 2 (cross-org isolation test data) ───
--
--   Groups:
--     900 Org2 Root Group (public, path 2/900/)
--
--   Projects:
--     9000 Org2 Project (public, path 2/900/9000/)
--
--   MergeRequests:
--     9100 Org2 MR (opened, path 2/900/9000/)
--
--   Edges:
--     User 1 -> Group 900 (MEMBER_OF, path 2/900/)
--     Group 900 -> Project 9000 (CONTAINS, path 2/900/9000/)
--     User 1 -> MR 9100 (AUTHORED, path 2/900/9000/)
--
--   WorkItem edges:
--     AUTHORED:      User 1 -> WI 4000, User 2 -> WI 4001, User 1 -> WI 4002, User 3 -> WI 4003
--     IN_GROUP:      WI 4000 -> Group 100, WI 4001 -> Group 100, WI 4002 -> Group 101, WI 4003 -> Group 102
--     IN_PROJECT:    WI 4000 -> Project 1000, WI 4001 -> Project 1000
--     CLOSED:        User 2 -> WI 4001
--     IN_MILESTONE:  WI 4000 -> Milestone 6000, WI 4001 -> Milestone 6000
--     ASSIGNED:      User 1 -> WI 4000, User 2 -> WI 4000, User 3 -> WI 4001
--     HAS_LABEL:     WI 4000 -> Label 7000, WI 4000 -> Label 7001, WI 4001 -> Label 7002
--
--   Code definitions:
--     Project 1000: compile -> helper -> run_query via CALLS
--     Project 1001: compile and run_query endpoints, plus a decoy CALLS edge
--                   sharing helper ID 12001 from a different traversal_path.
--                   This catches path-finding joins that connect only by node ID.

INSERT INTO gl_user (id, username, name, state, user_type, email) VALUES
    (1, 'alice', 'Alice Admin', 'active', 'human', 'alice@example.com'),
    (2, 'bob', 'Bob Builder', 'active', 'human', 'bob@example.com'),
    (3, 'charlie', 'Charlie Private', 'active', 'human', 'charlie@example.com'),
    (4, 'diana', 'Diana Developer', 'active', 'project_bot', 'diana@example.com'),
    (5, 'eve', 'Eve External', 'blocked', 'service_account', 'eve@example.com'),
    (6, '用户_émoji_🎉', 'Ünïcödé Üser', 'active', 'human', 'unicode@example.com'),
    (7, 'frank', 'Frank Deep', 'active', 'human', 'frank@example.com');

INSERT INTO gl_group (id, name, full_path, visibility_level, traversal_path) VALUES
    (100, 'Public Group', 'public-group', 'public', '1/100/'),
    (101, 'Private Group', 'private-group', 'private', '1/101/'),
    (102, 'Internal Group', 'internal-group', 'internal', '1/102/'),
    (200, 'Deep Group A', 'public-group/deep-a', 'public', '1/100/200/'),
    (300, 'Deep Group B', 'public-group/deep-a/deep-b', 'public', '1/100/200/300/');

INSERT INTO gl_project (id, name, full_path, visibility_level, traversal_path) VALUES
    (1000, 'Public Project', 'public-group/public-project', 'public', '1/100/1000/'),
    (1001, 'Private Project', 'private-group/private-project', 'private', '1/101/1001/'),
    (1002, 'Internal Project', 'public-group/internal-project', 'internal', '1/100/1002/'),
    (1003, 'Secret Project', 'private-group/secret-project', 'private', '1/101/1003/'),
    (1004, 'Shared Project', 'internal-group/shared-project', 'public', '1/102/1004/'),
    (1010, 'Deep Project', 'public-group/deep-a/deep-project', 'public', '1/100/200/1010/');

INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, merged_at, traversal_path) VALUES
    (2000, 1, 'Add feature A', 'opened',  'feature-a',  'main', NULL,                          '1/100/1000/'),
    (2001, 2, 'Fix bug B',     'opened',  'fix-b',      'main', NULL,                          '1/100/1000/'),
    (2002, 3, 'Refactor C',    'merged',  'refactor-c', 'main', toDateTime64('2024-03-15 12:00:00.000000', 6, 'UTC'), '1/101/1001/'),
    (2003, 4, 'Update D',      'closed',  'update-d',   'main', NULL,                          '1/102/1004/'),
    -- Two extra merged MRs let the data-correctness test pin a specific
    -- result for `merged_at >= 2024-06-01` (2004 + 2005, deterministic order).
    (2004, 5, 'Ship feature E','merged',  'ship-e',     'main', toDateTime64('2024-06-10 09:00:00.000000', 6, 'UTC'), '1/100/1000/'),
    (2005, 6, 'Ship feature F','merged',  'ship-f',     'main', toDateTime64('2024-08-20 09:00:00.000000', 6, 'UTC'), '1/100/1000/');

INSERT INTO gl_note (id, note, noteable_type, noteable_id, confidential, internal, created_at, traversal_path) VALUES
    (3000, 'Normal note on feature A', 'MergeRequest', 2000, false, false, '2024-01-15 10:30:00', '1/100/1000/'),
    (3001, 'Confidential feedback on bug B', 'MergeRequest', 2001, true, false, '2024-02-20 14:45:00', '1/100/1000/'),
    (3002, repeat('x', 10000), 'MergeRequest', 2000, false, false, NULL, '1/100/1000/'),
    (3003, 'Robert''); DROP TABLE gl_note;--', 'MergeRequest', 2000, false, false, NULL, '1/100/1000/');

INSERT INTO gl_merge_request_diff (id, merge_request_id, state, traversal_path) VALUES
    (5000, 2000, 'collected', '1/100/1000/'),
    (5001, 2000, 'collected', '1/100/1000/'),
    (5002, 2001, 'collected', '1/100/1000/');

INSERT INTO gl_milestone (id, iid, title, state, traversal_path) VALUES
    (6000, 1, 'Sprint 1', 'active', '1/100/'),
    (6001, 2, 'Sprint 2', 'closed', '1/101/');

INSERT INTO gl_label (id, title, color, traversal_path) VALUES
    (7000, 'bug', '#d73a4a', '1/100/'),
    (7001, 'feature', '#0075ca', '1/100/'),
    (7002, 'urgent', '#e4e669', '1/101/');

-- Vulnerabilities for role-scoped aggregation tests.
-- Each vuln lives under a project's traversal path so the compiler's
-- security pass filters them exactly like any other namespaced entity.
-- read_vulnerability is granted at Security Manager+, which is why Vulnerability's
-- ontology declares `required_role: security_manager`.
INSERT INTO gl_vulnerability (id, title, state, severity, report_type, resolved_on_default_branch, present_on_default_branch, traversal_path) VALUES
    (8000, 'SQLi in login', 'detected', 'critical', 'sast', false, true, '1/100/1000/'),
    (8001, 'XSS in comments', 'detected', 'high', 'sast', false, true, '1/101/1001/'),
    (8002, 'Exposed secret in CI', 'detected', 'critical', 'secret_detection', false, true, '1/102/1004/');

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/100/1000/', 8000, 'Vulnerability', 'IN_PROJECT', 1000, 'Project', ['severity:critical', 'state:detected'], []),
    ('1/101/1001/', 8001, 'Vulnerability', 'IN_PROJECT', 1001, 'Project', ['severity:high', 'state:detected'], []),
    ('1/102/1004/', 8002, 'Vulnerability', 'IN_PROJECT', 1004, 'Project', ['severity:critical', 'state:detected'], []);

INSERT INTO gl_definition (
    id, traversal_path, project_id, branch, commit_sha, file_path, fqn, name,
    definition_type, start_line, end_line, start_byte, end_byte
) VALUES
    (12000, '1/100/1000/', 1000, 'main', 'abc123', 'crates/compiler/src/lib.rs', 'compiler::compile', 'compile', 'Function', 10, 20, 100, 200),
    (12001, '1/100/1000/', 1000, 'main', 'abc123', 'crates/compiler/src/lib.rs', 'compiler::helper', 'helper', 'Function', 22, 30, 220, 300),
    (12002, '1/100/1000/', 1000, 'main', 'abc123', 'crates/orbit/src/main.rs', 'orbit::run_query', 'run_query', 'Function', 40, 55, 400, 550),
    (12100, '1/101/1001/', 1001, 'main', 'def456', 'crates/compiler/src/lib.rs', 'compiler::compile', 'compile', 'Function', 10, 20, 100, 200),
    (12102, '1/101/1001/', 1001, 'main', 'def456', 'crates/orbit/src/main.rs', 'orbit::run_query', 'run_query', 'Function', 40, 55, 400, 550);

INSERT INTO gl_code_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
    ('1/100/1000/', 12000, 'Definition', 'CALLS', 12001, 'Definition'),
    ('1/100/1000/', 12001, 'Definition', 'CALLS', 12002, 'Definition'),
    ('1/101/1001/', 12001, 'Definition', 'CALLS', 12102, 'Definition');

INSERT INTO gl_work_item (id, iid, title, state, work_item_type, confidential, weight, created_at, updated_at, closed_at, traversal_path) VALUES
    (4000, 1, 'Implement login page', 'opened', 'issue', false, 3, '2024-03-01 09:00:00', '2024-03-10 14:00:00', NULL, '1/100/'),
    (4001, 2, 'Fix auth bug', 'closed', 'incident', true, 8, '2024-03-05 11:30:00', '2024-03-15 16:00:00', '2024-03-15 16:00:00', '1/100/'),
    (4002, 3, 'Write unit tests', 'opened', 'task', false, NULL, '2024-04-01 08:00:00', '2024-04-01 08:00:00', NULL, '1/101/'),
    (4003, 4, 'Q1 Objective', 'opened', 'epic', false, 13, '2024-01-02 10:00:00', '2024-03-30 12:00:00', NULL, '1/102/'),
    (4010, 5, 'Deep WI', 'opened', 'issue', false, 5, '2024-04-15 09:00:00', '2024-04-15 09:00:00', NULL, '1/100/200/1010/');

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group', [], []),
    ('1/102/', 1, 'User', 'MEMBER_OF', 102, 'Group', [], []),
    ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group', [], []),
    ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group', [], []),
    ('1/101/', 4, 'User', 'MEMBER_OF', 101, 'Group', [], []),
    ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group', [], []),
    ('1/101/', 5, 'User', 'MEMBER_OF', 101, 'Group', [], []),
    ('1/100/', 6, 'User', 'MEMBER_OF', 100, 'Group', [], []),
    ('1/101/', 6, 'User', 'MEMBER_OF', 101, 'Group', [], []),
    ('1/100/200/', 100, 'Group', 'CONTAINS', 200, 'Group', [], []),
    ('1/100/200/300/', 200, 'Group', 'CONTAINS', 300, 'Group', [], []),
    ('1/100/1000/', 100, 'Group', 'CONTAINS', 1000, 'Project', [], []),
    ('1/100/1002/', 100, 'Group', 'CONTAINS', 1002, 'Project', [], []),
    ('1/101/1001/', 101, 'Group', 'CONTAINS', 1001, 'Project', [], []),
    ('1/101/1003/', 101, 'Group', 'CONTAINS', 1003, 'Project', [], []),
    ('1/102/1004/', 102, 'Group', 'CONTAINS', 1004, 'Project', [], []),
    ('1/100/200/1010/', 200, 'Group', 'CONTAINS', 1010, 'Project', [], []),
    ('1/100/1000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest', [], ['state:opened']),
    ('1/100/1000/', 1, 'User', 'AUTHORED', 2001, 'MergeRequest', [], ['state:opened']),
    ('1/101/1001/', 2, 'User', 'AUTHORED', 2002, 'MergeRequest', [], ['state:merged']),
    ('1/102/1004/', 3, 'User', 'AUTHORED', 2003, 'MergeRequest', [], ['state:closed']),
    ('1/100/1000/', 1, 'User', 'AUTHORED', 3000, 'Note', [], []),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3000, 'Note', ['state:opened'], []),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3002, 'Note', ['state:opened'], []),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3003, 'Note', ['state:opened'], []),
    ('1/100/1000/', 2001, 'MergeRequest', 'HAS_NOTE', 3001, 'Note', ['state:opened'], []),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_DIFF', 5000, 'MergeRequestDiff', ['state:opened'], []),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_DIFF', 5001, 'MergeRequestDiff', ['state:opened'], []),
    ('1/100/1000/', 2001, 'MergeRequest', 'HAS_DIFF', 5002, 'MergeRequestDiff', ['state:opened'], []),
    ('1/100/', 1, 'User', 'AUTHORED', 4000, 'WorkItem', [], ['state:opened', 'wi_type:issue']),
    ('1/100/', 2, 'User', 'AUTHORED', 4001, 'WorkItem', [], ['state:closed', 'wi_type:incident']),
    ('1/101/', 1, 'User', 'AUTHORED', 4002, 'WorkItem', [], ['state:opened', 'wi_type:task']),
    ('1/102/', 3, 'User', 'AUTHORED', 4003, 'WorkItem', [], ['state:opened', 'wi_type:epic']),
    ('1/100/', 4000, 'WorkItem', 'IN_GROUP', 100, 'Group', ['state:opened', 'wi_type:issue'], []),
    ('1/100/', 4001, 'WorkItem', 'IN_GROUP', 100, 'Group', ['state:closed', 'wi_type:incident'], []),
    ('1/101/', 4002, 'WorkItem', 'IN_GROUP', 101, 'Group', ['state:opened', 'wi_type:task'], []),
    ('1/102/', 4003, 'WorkItem', 'IN_GROUP', 102, 'Group', ['state:opened', 'wi_type:epic'], []),
    ('1/100/', 4000, 'WorkItem', 'IN_MILESTONE', 6000, 'Milestone', ['state:opened', 'wi_type:issue'], []),
    ('1/100/', 4001, 'WorkItem', 'IN_MILESTONE', 6000, 'Milestone', ['state:closed', 'wi_type:incident'], []),
    ('1/100/', 1, 'User', 'ASSIGNED', 4000, 'WorkItem', [], ['state:opened', 'wi_type:issue']),
    ('1/100/', 2, 'User', 'ASSIGNED', 4000, 'WorkItem', [], ['state:opened', 'wi_type:issue']),
    ('1/100/', 3, 'User', 'ASSIGNED', 4001, 'WorkItem', [], ['state:closed', 'wi_type:incident']),
    ('1/100/', 4000, 'WorkItem', 'HAS_LABEL', 7000, 'Label', ['state:opened', 'wi_type:issue'], []),
    ('1/100/', 4000, 'WorkItem', 'HAS_LABEL', 7001, 'Label', ['state:opened', 'wi_type:issue'], []),
    ('1/100/', 4001, 'WorkItem', 'HAS_LABEL', 7002, 'Label', ['state:closed', 'wi_type:incident'], []);

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/100/', 4000, 'WorkItem', 'IN_PROJECT', 1000, 'Project', ['state:opened', 'wi_type:issue'], []),
    ('1/100/', 4001, 'WorkItem', 'IN_PROJECT', 1000, 'Project', ['state:closed', 'wi_type:incident'], []),
    ('1/100/200/1010/', 4010, 'WorkItem', 'IN_PROJECT', 1010, 'Project', ['state:opened', 'wi_type:issue'], []),
    ('1/100/200/1010/', 7, 'User', 'AUTHORED', 4010, 'WorkItem', [], ['state:opened', 'wi_type:issue']),
    ('1/100/', 2, 'User', 'CLOSED', 4001, 'WorkItem', [], ['state:closed', 'wi_type:incident']);

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/100/1000/', 2, 'User', 'APPROVED', 2000, 'MergeRequest', [], ['state:opened']),
    ('1/100/1000/', 3, 'User', 'APPROVED', 2000, 'MergeRequest', [], ['state:opened']),
    ('1/101/1001/', 1, 'User', 'APPROVED', 2002, 'MergeRequest', [], ['state:merged']);

-- Organization 2: cross-org isolation test data.
-- User 1 (alice) exists in both orgs — her User row is global (gl_user has
-- no traversal_path), but her edges and the resources below are in org 2.

INSERT INTO gl_group (id, name, full_path, visibility_level, traversal_path) VALUES
    (900, 'Org2 Root Group', 'org2-root', 'public', '2/900/');

INSERT INTO gl_project (id, name, full_path, visibility_level, traversal_path) VALUES
    (9000, 'Org2 Project', 'org2-root/org2-project', 'public', '2/900/9000/');

INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
    (9100, 1, 'Org2 MR', 'opened', 'org2-feature', 'main', '2/900/9000/');

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('2/900/', 1, 'User', 'MEMBER_OF', 900, 'Group', [], []),
    ('2/900/9000/', 900, 'Group', 'CONTAINS', 9000, 'Project', [], []),
    ('2/900/9000/', 1, 'User', 'AUTHORED', 9100, 'MergeRequest', [], ['state:opened']);
