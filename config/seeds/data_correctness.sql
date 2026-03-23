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
--     Group 101 -> Project 1001, Group 101 -> Project 1003
--     Group 102 -> Project 1004
--
--   AUTHORED edges:
--     User 1 -> MR 2000, User 1 -> MR 2001
--     User 2 -> MR 2002, User 3 -> MR 2003
--     User 1 -> Note 3000
--
--   HAS_NOTE edges:
--     MR 2000 -> Note 3000, MR 2000 -> Note 3002, MR 2000 -> Note 3003
--     MR 2001 -> Note 3001
--
--   HAS_DIFF edges:
--     MR 2000 -> MergeRequestDiff 5000, MR 2000 -> MergeRequestDiff 5001
--     MR 2001 -> MergeRequestDiff 5002

INSERT INTO gl_user (id, username, name, state, user_type) VALUES
    (1, 'alice', 'Alice Admin', 'active', 'human'),
    (2, 'bob', 'Bob Builder', 'active', 'human'),
    (3, 'charlie', 'Charlie Private', 'active', 'human'),
    (4, 'diana', 'Diana Developer', 'active', 'project_bot'),
    (5, 'eve', 'Eve External', 'blocked', 'service_account'),
    (6, '用户_émoji_🎉', 'Ünïcödé Üser', 'active', 'human');

INSERT INTO gl_group (id, name, visibility_level, traversal_path) VALUES
    (100, 'Public Group', 'public', '1/100/'),
    (101, 'Private Group', 'private', '1/101/'),
    (102, 'Internal Group', 'internal', '1/102/'),
    (200, 'Deep Group A', 'public', '1/100/200/'),
    (300, 'Deep Group B', 'public', '1/100/200/300/');

INSERT INTO gl_project (id, name, visibility_level, traversal_path) VALUES
    (1000, 'Public Project', 'public', '1/100/1000/'),
    (1001, 'Private Project', 'private', '1/101/1001/'),
    (1002, 'Internal Project', 'internal', '1/100/1002/'),
    (1003, 'Secret Project', 'private', '1/101/1003/'),
    (1004, 'Shared Project', 'public', '1/102/1004/');

INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
    (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
    (2001, 2, 'Fix bug B', 'opened', 'fix-b', 'main', '1/100/1000/'),
    (2002, 3, 'Refactor C', 'merged', 'refactor-c', 'main', '1/101/1001/'),
    (2003, 4, 'Update D', 'closed', 'update-d', 'main', '1/102/1004/');

INSERT INTO gl_note (id, note, noteable_type, noteable_id, confidential, internal, created_at, traversal_path) VALUES
    (3000, 'Normal note on feature A', 'MergeRequest', 2000, false, false, '2024-01-15 10:30:00', '1/100/1000/'),
    (3001, 'Confidential feedback on bug B', 'MergeRequest', 2001, true, false, '2024-02-20 14:45:00', '1/100/1000/'),
    (3002, repeat('x', 10000), 'MergeRequest', 2000, false, false, NULL, '1/100/1000/'),
    (3003, 'Robert''); DROP TABLE gl_note;--', 'MergeRequest', 2000, false, false, NULL, '1/100/1000/');

INSERT INTO gl_merge_request_diff (id, merge_request_id, state, traversal_path) VALUES
    (5000, 2000, 'collected', '1/100/1000/'),
    (5001, 2000, 'collected', '1/100/1000/'),
    (5002, 2001, 'collected', '1/100/1000/');

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
    ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
    ('1/102/', 1, 'User', 'MEMBER_OF', 102, 'Group'),
    ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group'),
    ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
    ('1/101/', 4, 'User', 'MEMBER_OF', 101, 'Group'),
    ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group'),
    ('1/101/', 5, 'User', 'MEMBER_OF', 101, 'Group'),
    ('1/100/', 6, 'User', 'MEMBER_OF', 100, 'Group'),
    ('1/101/', 6, 'User', 'MEMBER_OF', 101, 'Group'),
    ('1/100/200/', 100, 'Group', 'CONTAINS', 200, 'Group'),
    ('1/100/200/300/', 200, 'Group', 'CONTAINS', 300, 'Group'),
    ('1/100/1000/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
    ('1/100/1002/', 100, 'Group', 'CONTAINS', 1002, 'Project'),
    ('1/101/1001/', 101, 'Group', 'CONTAINS', 1001, 'Project'),
    ('1/101/1003/', 101, 'Group', 'CONTAINS', 1003, 'Project'),
    ('1/102/1004/', 102, 'Group', 'CONTAINS', 1004, 'Project'),
    ('1/100/1000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest'),
    ('1/100/1000/', 1, 'User', 'AUTHORED', 2001, 'MergeRequest'),
    ('1/101/1001/', 2, 'User', 'AUTHORED', 2002, 'MergeRequest'),
    ('1/102/1004/', 3, 'User', 'AUTHORED', 2003, 'MergeRequest'),
    ('1/100/1000/', 1, 'User', 'AUTHORED', 3000, 'Note'),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3000, 'Note'),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3002, 'Note'),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3003, 'Note'),
    ('1/100/1000/', 2001, 'MergeRequest', 'HAS_NOTE', 3001, 'Note'),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_DIFF', 5000, 'MergeRequestDiff'),
    ('1/100/1000/', 2000, 'MergeRequest', 'HAS_DIFF', 5001, 'MergeRequestDiff'),
    ('1/100/1000/', 2001, 'MergeRequest', 'HAS_DIFF', 5002, 'MergeRequestDiff');
