# Golang application to demonstrate work with MySQL, PostgreSQL, MongoDB databases.

This is a great opportunity to learn about Go and how it works with MySQL, PostgreSQL, MongoDB databases in JSON format.

The script allows you to write JSON documents to three databases in parallel. Documents of the same type and the script algorithm is the same for each database, as a result you can compare speed, data volume and convenience of working with each database. 

The application requires Docker and Go on your machine.

The application performs the following functions:
1. Receives data from GitHub API. The list of repositories and Pull Requests for the selected organization will be retrieved. 
2. Asynchronously writes data to three databases Postgres, MySQL, MongoDB. The algorithm and the write data itself is the same for all the databases, thus we can compare the queries, their query performance and execution speed.
3. Simulation of database load in different scenarios (in development). Dataset in databases will be used for different types of queries and parameters.
4. Dashboards with analytical reports on the dataset (in development). A web server and web pages with information about Pull Requests and Repositories will be launched. Such as numbers (PRs, comments, stars, contributors, forks), statuses, average response and close times. In this way we can assess the health of the open source organization on GitHub.

The default configuration uses:
1. Docker containers with Percona Server for MySQL, Percona Distribution for PostgreSQL and Percona Server for MySQL. 
2. GitHub organization Percona, as it has about 200 public repositories and more than 40k Pull Requests.
You can use any other configuration or databases. 

Algorithm of the data fetching and storing application:
1. The database connection configuration is set in environment variables or .env file. See .env.example
2.The databases for the test can be run using Docker, docker-compose.yaml file is located in the main directory.
3. The script reads the configuration.
4. Runs an infinite loop that runs the main process of fetching and writing data. 
5. Runs http requests to the Github API to get JSON documents with repository information and Pull Requests. On the first run, all Pull Requests are retrieved. On subsequent runs, the application checks for the latest updates in the databases and retrieves only new Pull Requests from the API.
6. Runs asynchronously write to three databases using the same algorithm for the same data. 
7. Reports are written to separate tables and collections for each run and each database.
8. A log of the operations performed is output to the terminal. 
9. The script is repeated in a loop with a period set in the configuration.

## Quick start

1. Clone the project repository

`git clone git@github.com:dbazhenov/github-stat.git`

2. Run the environment, this will run the three databases locally.

`docker compose up -d`

3. Copy or rename `.env.example` to `.env`

4. Run the script

`go run cmd/main/main.go`

You will see in the terminal the process of writing identical JSON documents to databases in parallel.

Also, the results of each run will be written to a table or collection of 'reports_*' in each database.

## Installation and launch.

1. Clone the repository.

2. Start the environment with Docker using docker-compose.yaml in the main project directory.

`docker compose up -d`

This starts three databases:
- MySQL. Percona Server for MySQL 8
- PostgreSQL. Percona Distribution for PostgreSQL 16.2
- MongoDB. Percona Server for MongoDB 7

For Postgres and MySQL, data schemas will be created based on the data/init/* files

3. (Optional) Connect to databases using GUI
- MySQL Workbench for MySQL
- pgAdmin for Postgres
- MongoDB Compass for MongoDB

This will allow you to explore the data in a user-friendly interface.

4. Copy or rename `.env.example` to `.env`

Set the parameters in the .env file

- `GITHUB_ORG` - GitHub organization whose repositories will be investigated. For example, percona contains 193 repositories. 

- `GITHUB_TOKEN` - Your personalized GitHub Token. If left empty, the limit is 60 API requests per hour, which is enough for a test run. If you add token then 5000 requests per hour. You can get a Token in your GitHub profile settings, it's free, instructions at [the link](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)

If you don't want to use any database, leave HOST empty.

5. Run the script in the terminal

`go run cmd/main/main.go`

The main script will run parallel writes to the three databases.

Using MongoDB Compass, pgAdmib, MySQL WorkBench you can explore data and compare databases.

## Launching with Docker

1. Build the Docker Image Use the following command to build the Docker image:

`docker build -t github_app .`

This command will create an image named `github_app` . The Dockerfile in the main directory of the repository will be used

2. Run the Docker Container Use the following command to run the Docker container:

`docker run --name app --env-file .env.docker.example --rm -d  --network=databases_default github_app`

This command will start a container named `app`, use the environment variables specified in the `.env.docker.example` file, remove the container when it stops, run it in detached mode, and connect it to the `databases_default` network (network name will be available after running docker-compose.yaml with databases.)

## Launching in Kubernetes

1. Edit the deployment.yaml file. Edit the Secret and ConfigMap sections. Add databases access data and GitHub Token. 

2. Connect to your MySQL and PostgreSQL databases and run the commands from the data/init folder to create the databases and tables. If you don't want to use any database, leave MONGODB_HOST, PG_HOST, MySQL_HOST empty.

3. Launch the application

`kubectl apply -f deployment.yaml -n [namespace]`

Tested with [the Percona Everest](https://docs.percona.com/everest/index.html)

### Creating your own Docker image to run in Kubernetes

A [dockerhub](https://hub.docker.com/) account is required. The build will use the Dockerfile in the main directory, edit it if necessary.

1. Run a Docker image build and push it to DockerHub

`docker buildx build --platform linux/amd64 -t [dockerhub_login]/github_app:[version] --push .`

2. Edit deployment.yaml by adding your image. Launch the application

`kubectl apply -f deployment.yaml -n [namespace]`

### Useful commands

kubectl get pods -n github

kubectl describe pod github-app-857958c877-qp6lz -n github

kubectl logs github-app-6499787d79-sdcdx -n github

## Coming soon

1. Get GitHub Issues and Contributors for each repository 
2. Web interface to view saved data and reports.

You are invited to make a contribution:
1. Suggest improvements and create Issues
2. Improve code or do a review.