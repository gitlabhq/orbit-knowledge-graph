-- Default seed data for KDL integration tests.
-- Inserts 5 users, 3 groups, 5 projects, 4 merge requests, and 12 edges.

INSERT INTO gl_user (id, username, name, state, user_type) VALUES
    (1, 'alice', 'Alice Admin', 'active', 'human'),
    (2, 'bob', 'Bob Builder', 'active', 'human'),
    (3, 'charlie', 'Charlie Private', 'active', 'human'),
    (4, 'diana', 'Diana Developer', 'active', 'project_bot'),
    (5, 'eve', 'Eve External', 'blocked', 'service_account');

INSERT INTO gl_group (id, name, visibility_level, traversal_path) VALUES
    (100, 'Public Group', 'public', '1/100/'),
    (101, 'Private Group', 'private', '1/101/'),
    (102, 'Internal Group', 'internal', '1/102/');

INSERT INTO gl_project (id, name, visibility_level, traversal_path) VALUES
    (1000, 'Public Project', 'public', '1/100/1000/'),
    (1001, 'Private Project', 'private', '1/101/1001/'),
    (1002, 'Internal Project', 'internal', '1/100/1002/'),
    (1003, 'Secret Project', 'private', '1/101/1003/'),
    (1004, 'Shared Project', 'public', '1/102/1004/');

INSERT INTO gl_merge_request (id, title, state, source_branch, target_branch, traversal_path) VALUES
    (2000, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
    (2001, 'Fix bug B', 'opened', 'fix-b', 'main', '1/100/1000/'),
    (2002, 'Refactor C', 'merged', 'refactor-c', 'main', '1/101/1001/'),
    (2003, 'Update D', 'closed', 'update-d', 'main', '1/102/1004/');

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
    ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
    ('1/102/', 1, 'User', 'MEMBER_OF', 102, 'Group'),
    ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group'),
    ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
    ('1/101/', 4, 'User', 'MEMBER_OF', 101, 'Group'),
    ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group'),
    ('1/101/', 5, 'User', 'MEMBER_OF', 101, 'Group'),
    ('1/100/1000/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
    ('1/100/1002/', 100, 'Group', 'CONTAINS', 1002, 'Project'),
    ('1/101/1001/', 101, 'Group', 'CONTAINS', 1001, 'Project'),
    ('1/101/1003/', 101, 'Group', 'CONTAINS', 1003, 'Project'),
    ('1/102/1004/', 102, 'Group', 'CONTAINS', 1004, 'Project');
