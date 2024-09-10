CREATE DATABASE IF NOT EXISTS github;
USE github;
CREATE TABLE IF NOT EXISTS repositories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);

CREATE TABLE IF NOT EXISTS pulls (
    id INT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo)
);

ALTER TABLE github.pulls 
ADD COLUMN updated_at_virtual VARCHAR(255) 
GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.updated_at'))) VIRTUAL;
CREATE INDEX idx_updated_at ON github.pulls (updated_at_virtual);

CREATE TABLE IF NOT EXISTS reports_runs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);

CREATE TABLE IF NOT EXISTS reports_databases (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);