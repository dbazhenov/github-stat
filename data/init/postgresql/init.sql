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
    PRIMARY KEY (id, repo),
    INDEX idx_id (id),
    INDEX idx_repo (repo)
);

CREATE TABLE IF NOT EXISTS github.pulls_test (
    id BIGINT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo),
    INDEX idx_id (id),
    INDEX idx_repo (repo)
);

CREATE TABLE IF NOT EXISTS github.reports_runs (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE TABLE IF NOT EXISTS github.reports_databases (
    id SERIAL PRIMARY KEY,
    data JSONB
);
