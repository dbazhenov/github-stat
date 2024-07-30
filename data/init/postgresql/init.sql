CREATE DATABASE github;
\c github;

CREATE SCHEMA IF NOT EXISTS github;

CREATE TABLE IF NOT EXISTS github.repositories (
    id SERIAL PRIMARY KEY,
    data JSONB
);

CREATE TABLE IF NOT EXISTS github.report (
    id SERIAL PRIMARY KEY,
    data JSONB
);