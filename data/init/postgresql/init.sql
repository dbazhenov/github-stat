\c postgres;
CREATE EXTENSION pg_stat_monitor;

CREATE DATABASE dataset;
\c dataset;

CREATE EXTENSION pg_stat_monitor;

CREATE SCHEMA IF NOT EXISTS github;

CREATE TABLE IF NOT EXISTS github.repositories (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE TABLE IF NOT EXISTS github.repositories_test (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE TABLE IF NOT EXISTS github.pulls (
    id BIGINT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo)
);

CREATE TABLE IF NOT EXISTS github.pulls_test (
    id BIGINT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo)
);

CREATE INDEX idx_id_pulls ON github.pulls (id);
CREATE INDEX idx_repo_pulls ON github.pulls (repo);
CREATE INDEX idx_id_pulls_test ON github.pulls_test (id);
CREATE INDEX idx_repo_pulls_test ON github.pulls_test (repo);

CREATE TABLE IF NOT EXISTS github.reports_runs (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE TABLE IF NOT EXISTS github.reports_databases (
    id SERIAL PRIMARY KEY,
    data JSONB
);
