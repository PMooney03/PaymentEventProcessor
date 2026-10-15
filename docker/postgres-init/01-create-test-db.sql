-- Runs once when the Postgres volume is first initialised. Creates a separate
-- database for integration tests so they never share tables with the running
-- application (whose worker would otherwise claim test events).
CREATE DATABASE payments_test;
