-- Teardown for the GKG ClickHouse setup contract; DROP DATABASE destroys the graph.
DROP USER IF EXISTS gkg_writer;
DROP USER IF EXISTS gkg_reader;
DROP USER IF EXISTS gkg_siphon_reader;
DROP ROLE IF EXISTS gkg_app;
DROP ROLE IF EXISTS gkg_reader_app;
DROP ROLE IF EXISTS gkg_siphon_reader_app;
DROP DATABASE IF EXISTS gkg;
