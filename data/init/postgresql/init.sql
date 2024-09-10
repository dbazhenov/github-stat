CREATE DATABASE github;
\c github;

CREATE SCHEMA IF NOT EXISTS github;

CREATE TABLE IF NOT EXISTS github.repositories (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE TABLE IF NOT EXISTS github.pulls (
    id INT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo)
);

CREATE TABLE IF NOT EXISTS github.reports_runs (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE TABLE IF NOT EXISTS github.reports_databases (
    id SERIAL PRIMARY KEY,
    data JSONB
);
