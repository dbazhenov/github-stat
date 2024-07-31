# Project for testing database connections

This Golang application performs the following functions:
1. Gets data from GitHub API. Gets the list of the organization's repositories specified in the settings.
2. Stores data in JSON format in MySQL, MongoDB, and PostgreSQL.

The script allows you to write JSON documents to three databases in parallel. Documents of the same type and the script algorithm is the same for each database, as a result you can compare speed, data volume and convenience of working with each database. 

This is a great opportunity to learn about Go and how it works with MySQL, PostgreSQL, MongoDB databases in JSON format.

The application requires Docker and Go on your machine.

## Quick start

1. Clone the project repository

`git clone git@github.com:dbazhenov/github-stat.git`

2. Run the environment, this will run the three databases locally.

`docker compose up -d`

3. Run the script

`go run cmd/main/main.go`

You will see in the terminal the process of writing identical JSON documents to databases in parallel.

Also, the results of each run will be written to a table or collection of 'reports' in each database.

```
2024/07/31 18:16:34 PostgreSQL: Insert data, row: 67, repo: percona/mongo-rocks
2024/07/31 18:16:34 MySQL: Insert data, row: 137, repo: percona/portal-doc
2024/07/31 18:16:34 Finish MongoDB: {"type":"GitHub Repositories","started_at":"2024-07-31T18:16:33+04:00","finished_at":"2024-07-31T18:16:34+04:00","count":193,"api_requests":19}
2024/07/31 18:16:34 MySQL: Insert data, row: 138, repo: percona/orchestrator
2024/07/31 18:16:34 PostgreSQL: Insert data, row: 68, repo: percona/qan-api
....
2024/07/31 18:16:34 PostgreSQL: Insert data, row: 116, repo: percona/proxysql-admin-tool-doc
2024/07/31 18:16:34 Finish MySQL: {"type":"GitHub Repositories","started_at":"2024-07-31T18:16:33+04:00","finished_at":"2024-07-31T18:16:34+04:00","count":193,"api_requests":19}
2024/07/31 18:16:34 PostgreSQL: Insert data, row: 117, repo: percona/grafana
2024/07/31 18:16:34 PostgreSQL: Insert data, row: 118, repo: percona/pdmysql-docs
....
2024/07/31 18:16:34 PostgreSQL: Insert data, row: 192, repo: percona/valkey-packaging
2024/07/31 18:16:34 PostgreSQL: Finish Insert
2024/07/31 18:16:34 Finish PostgreSQL: {"type":"GitHub Repositories","started_at":"2024-07-31T18:16:33+04:00","finished_at":"2024-07-31T18:16:34+04:00","count":193,"api_requests":19}
```


## Installation and launch.

1. Clone the repository.

2. Start the environment with Docker using docker-compose.yaml in the main project directory.

`docker compose up -d`

This starts three databases:
- MySQL. Percona Server for MySQL 8
- PostgreSQL. Percona Distribution for PostgreSQL 16.2
- MongoDB. Percona Server for MongoDB 7

3. (Optional) Connect to databases using GUI
- MySQL Workbench for MySQL
- pgAdmin for Postgres
- MongoDB Compass for MongoDB

This will allow you to explore the data in a user-friendly interface.

4. Copy or rename `.env.example` to `.env`

Set the parameters in the .env file

- `GITHUB_ORG` - GitHub organization whose repositories will be investigated. For example, percona contains 193 repositories. 

- `GITHUB_TOKEN` - Your personalized GitHub Token. If left empty, the limit is 60 API requests per hour, which is enough for a test run. If you add token then 5000 requests per hour. You can get a Token in your GitHub profile settings, it's free, instructions at [the link](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)

5. Run the script in the terminal

`go run cmd/main/main.go`

The main script will run parallel writes to the three databases.

You can also write to each database individually by running the scripts:

- MySQL: `go run cmd/mysql/main.go`

- PostgreSQL: `go run cmd/postgres/main.go`

- MongoDB: `go run cmd/mongodb/main.go`

The scripts will retrieve the data and write it to each database. A github table database or a collection of repositories will be used. 
It will also write the result to the report table/collection. 

Using MongoDB Compass, pgAdmib, MySQL WorkBench you can explore data and compare databases.

## Coming soon

1. Run by one script and parallel or asynchronous saving to different databases.
2. Get GitHub PRs, Issues and Contributors for each repository 
3. Web interface to view saved data and reports.

You are invited to make a contribution:
1. Suggest improvements and create Issues
2. Improve code or do a review.
