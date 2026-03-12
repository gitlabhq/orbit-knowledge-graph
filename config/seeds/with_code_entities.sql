-- Additional seed data for code entity (indirect auth) tests.
-- Requires default.sql to be loaded first.
-- Inserts files, definitions, and DEFINES edges across two projects.

INSERT INTO gl_file (id, traversal_path, project_id, branch, path, name, extension, language) VALUES
    (3000, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'lib.rs', 'rs', 'Rust'),
    (3001, '1/100/1000/', 1000, 'main', 'src/main.rs', 'main.rs', 'rs', 'Rust'),
    (3002, '1/101/1001/', 1001, 'main', 'src/secret.rs', 'secret.rs', 'rs', 'Rust');

INSERT INTO gl_definition (id, traversal_path, project_id, branch, file_path, fqn, name, definition_type, start_line, end_line, start_byte, end_byte) VALUES
    (5000, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'crate::MyStruct', 'MyStruct', 'class', 10, 50, 100, 500),
    (5001, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'crate::my_func', 'my_func', 'function', 60, 80, 600, 900),
    (5002, '1/100/1000/', 1000, 'main', 'src/main.rs', 'crate::main', 'main', 'function', 1, 20, 0, 200),
    (5003, '1/101/1001/', 1001, 'main', 'src/secret.rs', 'crate::Secret', 'Secret', 'class', 1, 30, 0, 300);

INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
    ('1/100/1000/', 3000, 'File', 'DEFINES', 5000, 'Definition'),
    ('1/100/1000/', 3000, 'File', 'DEFINES', 5001, 'Definition'),
    ('1/100/1000/', 3001, 'File', 'DEFINES', 5002, 'Definition'),
    ('1/101/1001/', 3002, 'File', 'DEFINES', 5003, 'Definition');
