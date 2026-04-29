-- Denormalization correctness seed data.
-- Minimal dataset to verify that denormalized tag arrays on edge rows
-- produce correct query results when the compiler rewrites _nf_ CTEs
-- to hasToken/hasAllTokens filters.

-- Nodes: MergeRequests with different states
INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
    (100, 1, 'Open MR 1',   'opened', 'feature-1', 'main', '1/10/20/'),
    (101, 2, 'Open MR 2',   'opened', 'feature-2', 'main', '1/10/20/'),
    (102, 3, 'Merged MR',   'merged', 'feature-3', 'main', '1/10/20/'),
    (103, 4, 'Closed MR',   'closed', 'feature-4', 'main', '1/10/20/');

-- Nodes: Vulnerabilities with different state + severity combos
INSERT INTO gl_vulnerability (id, title, state, severity, report_type, resolved_on_default_branch, present_on_default_branch, traversal_path) VALUES
    (200, 'SQL Injection',  'detected',  'critical', 'sast',  false, true,  '1/10/20/'),
    (201, 'XSS',            'detected',  'high',     'sast',  false, true,  '1/10/20/'),
    (202, 'CSRF',           'resolved',  'critical', 'dast',  true,  false, '1/10/20/'),
    (203, 'Open Redirect',  'confirmed', 'medium',   'dast',  false, true,  '1/10/20/');

-- Nodes: WorkItems with different state + type combos
INSERT INTO gl_work_item (id, iid, title, state, work_item_type, confidential, weight, created_at, updated_at, closed_at, traversal_path) VALUES
    (300, 1, 'Open Issue',     'opened', 'issue',    false, 3,    '2024-01-01 00:00:00', '2024-01-01 00:00:00', NULL, '1/10/'),
    (301, 2, 'Closed Incident','closed', 'incident', false, 8,    '2024-01-01 00:00:00', '2024-01-15 00:00:00', '2024-01-15 00:00:00', '1/10/'),
    (302, 3, 'Open Epic',      'opened', 'epic',     false, NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:00', NULL, '1/10/');

-- Nodes: Pipelines with different statuses
INSERT INTO gl_pipeline (id, iid, status, ref, source, traversal_path) VALUES
    (400, 1, 'failed',  'main',      'push',    '1/10/20/'),
    (401, 2, 'success', 'main',      'push',    '1/10/20/'),
    (402, 3, 'failed',  'feature-1', 'push',    '1/10/20/');

-- Container nodes
INSERT INTO gl_group (id, name, full_path, visibility_level, traversal_path) VALUES
    (10, 'Test Group', 'test-group', 'public', '1/10/');

INSERT INTO gl_project (id, name, full_path, visibility_level, traversal_path) VALUES
    (20, 'Test Project', 'test-group/test-project', 'public', '1/10/20/');

INSERT INTO gl_user (id, username, name, state, user_type, email) VALUES
    (1, 'alice', 'Alice', 'active', 'human', 'alice@example.com');

-- Edges: MR → IN_PROJECT → Project (source_tags carry MR state)
INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/10/20/', 100, 'MergeRequest', 'IN_PROJECT', 20, 'Project', ['state:opened'], []),
    ('1/10/20/', 101, 'MergeRequest', 'IN_PROJECT', 20, 'Project', ['state:opened'], []),
    ('1/10/20/', 102, 'MergeRequest', 'IN_PROJECT', 20, 'Project', ['state:merged'], []),
    ('1/10/20/', 103, 'MergeRequest', 'IN_PROJECT', 20, 'Project', ['state:closed'], []);

-- Edges: Vulnerability → IN_PROJECT → Project (source_tags carry severity + state)
INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/10/20/', 200, 'Vulnerability', 'IN_PROJECT', 20, 'Project', ['severity:critical', 'state:detected'], []),
    ('1/10/20/', 201, 'Vulnerability', 'IN_PROJECT', 20, 'Project', ['severity:high', 'state:detected'],     []),
    ('1/10/20/', 202, 'Vulnerability', 'IN_PROJECT', 20, 'Project', ['severity:critical', 'state:resolved'], []),
    ('1/10/20/', 203, 'Vulnerability', 'IN_PROJECT', 20, 'Project', ['severity:medium', 'state:confirmed'],  []);

-- Edges: WorkItem → IN_GROUP → Group (source_tags carry state + wi_type)
INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/10/', 300, 'WorkItem', 'IN_GROUP', 10, 'Group', ['state:opened', 'wi_type:issue'],    []),
    ('1/10/', 301, 'WorkItem', 'IN_GROUP', 10, 'Group', ['state:closed', 'wi_type:incident'], []),
    ('1/10/', 302, 'WorkItem', 'IN_GROUP', 10, 'Group', ['state:opened', 'wi_type:epic'],     []);

-- Edges: Pipeline → IN_PROJECT → Project (source_tags carry status)
INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/10/20/', 400, 'Pipeline', 'IN_PROJECT', 20, 'Project', ['status:failed'],  []),
    ('1/10/20/', 401, 'Pipeline', 'IN_PROJECT', 20, 'Project', ['status:success'], []),
    ('1/10/20/', 402, 'Pipeline', 'IN_PROJECT', 20, 'Project', ['status:failed'],  []);

-- Edges: User → AUTHORED → MR (target_tags carry MR state)
INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/10/20/', 1, 'User', 'AUTHORED', 100, 'MergeRequest', [], ['state:opened']),
    ('1/10/20/', 1, 'User', 'AUTHORED', 101, 'MergeRequest', [], ['state:opened']),
    ('1/10/20/', 1, 'User', 'AUTHORED', 102, 'MergeRequest', [], ['state:merged']),
    ('1/10/20/', 1, 'User', 'AUTHORED', 103, 'MergeRequest', [], ['state:closed']);

-- Edges: structural (no denorm tags)
INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
    ('1/10/',    1,  'User',  'MEMBER_OF', 10, 'Group',   [], []),
    ('1/10/20/', 10, 'Group', 'CONTAINS',  20, 'Project', [], []);
