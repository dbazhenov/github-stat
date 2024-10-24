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

CREATE TABLE IF NOT EXISTS pullsTest (
    id INT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo)
);

CREATE TABLE IF NOT EXISTS reports_runs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);

CREATE TABLE IF NOT EXISTS reports_databases (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);