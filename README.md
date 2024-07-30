# Project for testing database connections

This Golang application performs the following functions:
1. Gets data from GitHub API. Gets the list of the organization's repositories specified in the settings.
2. Stores data in JSON format in MySQL, MongoDB, and PostgreSQL.

The application requires Docker and Go on your machine.

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

5. Run the scripts in the terminal for each database.

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













