# Demo application for MySQL, PostgreSQL, and MongoDB databases, with monitoring and deployment in Kubernetes.

This demo application is designed to showcase the usage of **MySQL**, **PostgreSQL**, and **MongoDB** databases, along with database monitoring and deployment in **Kubernetes** environments. It provides an opportunity to explore how these databases can be tested and monitored, using Go applications and [Percona Monitoring and Management](https://docs.percona.com/percona-monitoring-and-management/index.html) (PMM) tools.

![Demo Control Panel](./assets/readme-contol-panel.png)

## Overview

The application consists of three main components:

1. **Control Panel**: A web-based application used for managing database load and configurations.

2. **Dataset Loader**: A Go application that fetches data from GitHub via API and loads it into the databases for testing and load simulation.

3. **Load Generator**: A Go application that generates SQL and NoSQL queries based on control panel settings.

## Usage

The application connects to and generates load on MySQL, PostgreSQL, and MongoDB databases in the cloud or Kubernetes. You can start the databases with:

1. **[Docker Compose](https://docs.docker.com/compose/)**: Configuration is available in the repository.
2. **[Percona Everest](https://docs.percona.com/everest/index.html) or [Percona Operators](https://docs.percona.com/percona-operators) in Kubernetes**: If the databases are not externally accessible, run the application in the same cluster.
3. **Custom Methods**: Connection parameters can be set in environment variables or through the Settings tab of the Control Panel.

### Usage Scenario:

1. Start the **Control Panel** in your browser (e.g., iPad).
2. Open **PMM** in the browser (e.g., screen or laptop).
3. Install **Percona Everest**, run it in the browser, and create MySQL, PostgreSQL, and MongoDB databases.
4. Connect the databases in the **Control Panel Settings**.
5. Adjust the load on the **Control Panel** and monitor the changes in PMM.

## How It Works Technically

1. **Control Panel**: A web application that stores settings in the [Valkey](https://valkey.io/) database when adjustments are made.
2. **Dataset Loader**: A continuously running script that checks settings in Valkey every `5` seconds, connects to the databases, and loads the data.
3. **Load Generator**: Another continuously running script that works on one or all databases. Every 5 seconds, it checks the load settings in Valkey and generates SQL and NoSQL queries accordingly. These queries are defined in `internal/load/load.go`.

## Running locally with Docker Compose

1. Clone the project repository:

```bash
git clone git@github.com:dbazhenov/github-stat.git
```

2. Copy or rename `.env.example` (already provided in this repo) to `.env`. Set the parameters in the `.env` file.

   > **Note:** The `.env` file contains essential configuration settings for the application. Adjust these settings based on your environment to ensure proper functionality.

3. Run the environment:

```bash
docker compose up -d
```

4. Launch the application at `localhost:3000` in your browser.

![Demo App](./assets/demo-app.png)

5. Open the Settings tab and create connections to the databases you want to load.

   - If you don't databases, start them using docker compose:

   ```bash
   docker compose -f docker-compose-dbs.yaml up -d
   ```

   > **Note:** This `docker-compose.yml` file sets up a multi-container environment with PostgreSQL, MongoDB, and MySQL databases, along with a Percona Monitoring and Management (PMM) server and its client containers for monitoring each database.

   ![Demo App](./assets/docker-compose-dbs.png)

   Connection options are available in docker-compose-dbs.yaml

   - **MySQL**: `root:password@tcp(mysql:3306)/dataset`

   - **Postgres**: `user=postgres password='password' dbname=dataset host=postgres port=5432 sslmode=disable`

   - **MongoDB**: `mongodb://databaseAdmin:password@mongodb:27017/`

6. In the **Settings** tab, load the test dataset for each database by clicking `Create Schema` and `Import Dataset` buttons. A small dataset from a CSV file (26 repos and 4600 PRs) will be imported by default.

   > **Note:** To import the full dataset, add a GitHub Token to the .env file and change the import type to GitHub.

7. Turn on the `Enable Load` setting option and open the `Load Generator Control Panel` tab.

8. Adjust load settings and check the results in PMM at `localhost:8081`.

## Development Environment

0. Run the environment:

```bash
docker compose -f docker-compose-dev.yaml up -d
```

1. Run the Control Panel script:

```go
go run cmd/web/main.go
```

Launch the control panel at localhost:3000.

2. Run the Dataset loader script

```go
go run cmd/dataset/main.go
```

3. Run the Dataset Loader script:

```go
go run cmd/load/main.go
```

7. Optionally, uncomment the configuration for PMM and PMM Client in docker-compose.yaml and run PMM:

```bash
docker compose up -d
```

Start PMM in your browser at localhost.

## Launching in Kubernetes

1. Create a Kubernetes cluster (e.g., Minikube or GKE). For GKE:

```bash
gcloud container clusters create demo-app --project percona-product --zone us-central1-a --cluster-version 1.30 --machine-type n1-standard-16 --num-nodes=1
```

2. Install **Percona Everest** or **Percona Operators** in the Kubernetes cluster to create databases.
   Percona Everest documentation:

   - [Install Everest CLI](https://github.com/edithturn/github-stat/tree/main#:~:text=Percona%20Everest%20documentation%3A-,Install%20Everest%20CLI,-Install%20Everest)
   - [Install Everest](https://github.com/edithturn/github-stat/tree/main#:~:text=Install%20Everest%20CLI-,Install%20Everest,-Create%20databases%20if)

   Create databases if you don't have any.

3. Install PMM using Helm:

```bash
helm repo add percona https://percona.github.io/percona-helm-charts/

helm install pmm -n demo \
--set service.type="LoadBalancer" \
--set pmmResources.limits.memory="4Gi" \
--set pmmResources.limits.cpu="2" \
percona/pmm
```

4. Get the PMM administrator password:

```bash
kubectl get secret pmm-secret -n demo -o jsonpath='{.data.PMM_ADMIN_PASSWORD}' | base64 --decode
```

5. Get a public IP for PMM:

```bash
kubectl get svc -n demo monitoring-service -o jsonpath="{.status.loadBalancer.ingress[0].ip}"

```

### Running the Demo Application Using Helm

1. Set the HELM parameters in the `./k8s/helm/values.yaml` file:

2. Launch the application:

```bash
helm install demo-app ./k8s/helm -n demo
```

3. Get the public IP of the demo app and launch the control panel in your browser.
   Run this command to get the Public IP

```bash
kubectl -n demo get svc
```

4. Open the Settings tab on the control panel and set the parameters for connecting to the databases you created in Percona Everest or with Percona Operators.

5. You may need to restart the dataset pod to speed up the process of loading the dataset into the databases.

```bash
kubectl -n demo delete pod [DATASET_POD]
```

6. You can change the allocated resources or the number of replicas by editing the `values.yaml` file and issuing the command

```bash
helm upgrade demo-app ./k8s/helm -n demo
```

Demo App HELM parameters (./k8s/helm/values.yaml):

- `githubToken` - is required to properly load the dataset from the GitHub API. You can create a personal Token at [https://github.com/settings/tokens](https://github.com/settings/tokens).

- `separateLoads` - If true, separate pods for each database will be started for the load.

- `useResourceLimits` - if true, resource limits will be set for the resource consumption

- `controlPanelService.type` - LoadBalancer for the public address of the dashboard. NodePort for developing locally.

### Running Demo Application Manually

1. Create the necessary Secrets and ConfigMap:

```bash
kubectl apply -f k8s/manual/config.yaml -n demo
```

Check the k8s/config.yaml file. Be sure to set `GITHUB_TOKEN`, which is required to properly load the dataset from the GitHub API. You can create a personal Token at [https://github.com/settings/tokens](https://github.com/settings/tokens).

2. Run Valkey database:

```bash
kubectl apply -f k8s/manual/valkey.yaml -n demo
```

3. Deploy the Control Panel:

```bash
kubectl apply -f k8s/manual/web-deployment.yaml -n demo
```

4. Run `kubectl -n demo get svc` to get the public IP. Launch the control panel in your browser.

5. Open the control panel in your browser. Open the Settings tab. Set the connection string to the databases created in Percona Everest. Click the Connect button.

The first time you connect to MySQL and Postgres, you will need to create a schema and tables. You will see the buttons on the Settings tab.

6. Deploy the Dataset Loader:

```bash
kubectl apply -f k8s/manual/dataset-deployment.yaml -n demo
```

5. Deploy the Load Generator:

```bash
kubectl apply -f k8s/manual/load-deployment.yaml -n demo
```

8. For separate database load generators, apply these commands:

   - MySQL:

   ```bash
    kubectl apply -f k8s/manual/load-mysql-deployment.yaml -n demo
   ```

   - Postgres:

   ```bash
   kubectl apply -f k8s/manual/load-postgres-deployment.yaml -n demo
   ```

   - MongoDB:

   ```bash
   kubectl apply -f k8s/manual/load-mongodb-deployment.yaml -n demo
   ```

You can set the environment variable to determine which database the script will load.

6. Control the load in the control panel. Change queries using the switches. Track the result on PMM dashboards. Scale or change database parameters with Percona Everest.

Have fun experimenting.

### Useful Commands

- Get Pods:

```bash
kubectl get pods -n demo
```

- View logs:

```bash
kubectl logs [pod_name] -n demo
```

- Describe Pods:

```bash
kubectl describe pod [pod_name] -n demo
```

## Contributing

1. Clone the repository and run locally using Docker Compose.

2. Make changes to the code and run scripts for tests.

3. The repository contains Workflow to build and publish to Docker Hub. You can publish your own versions of containers and run them in Kubernetes.

4. Send your changes to the project using Pull Request.

We welcome contributions:

1. Suggest improvements and create Issues
2. Improve code or do a review.
