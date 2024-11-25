CREATE DATABASE IF NOT EXISTS dataset;
USE dataset;
CREATE TABLE IF NOT EXISTS repositories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);

CREATE TABLE IF NOT EXISTS repositoriesTest (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);

CREATE TABLE IF NOT EXISTS pulls (
    id BIGINT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo),
    INDEX idx_repo (repo),
    INDEX idx_id (id)
);

CREATE TABLE IF NOT EXISTS pullsTest (
    id BIGINT NOT NULL,
    repo VARCHAR(255) NOT NULL,
    data JSON,
    PRIMARY KEY (id, repo),
    INDEX idx_repo (repo),
    INDEX idx_id (id)
);

CREATE TABLE IF NOT EXISTS reports_dataset (
    id INT AUTO_INCREMENT PRIMARY KEY,
    data JSON
);