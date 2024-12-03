# Demo application to demonstrate MySQL, Postgres, MongoDB databases, monitoring and deployment in Kubernetes.

This is a great opportunity to learn about Go and how it works with MySQL, PostgreSQL, MongoDB databases.

The application is a web-based control panel through which you can manage the load on the databases. In combination with Percona Monitoring and Management (PMM) tools, you will have an excellent tool for demonstrating databases and their capabilities.

The application consists of three components:
1. Control Panel - a web application that you can run in your browser. The control panel contains controls and load switches for databases MySQL, Postgres, MongoDB. The control panel also contains Settings tabs for configuring the connection to databases and Dataset tab for tracking the dataset state.
2. Dataset Loader - Go application that fetches data from GitHub via API and loads it into databases. It is used to provide databases with data for testing and load simulation. The component loads data into an empty database and periodically loads new data to keep the dataset up to date.
3. Load Generator - Go application that connects to databases and generates SQL and NoSQL queries based on control panel settings (component 1)

All three components work as a single application and allow you to experiment with databases. The application can be run locally using Docker Compose or in the cloud using Kubernetes.

![Demo Contol Panel](./assets/readme-contol-panel.png)

The application can connect to and generate load on MySQL, Postgres and MongoDB databases in the cloud or Kubernetes. You can start the databases with:
1. Docker compose - configuration is available in the repository. 
2. Percona Everest or Percona Operators in a Kubernetes cluster. If the databases are not externally accessible, then run the application in the same cluster. 
3. In another way that is convenient for you. 
Connection parameters can be set in environment variables or in the Settings tab of the Control Panel of the application.

Usage Scenario:
1. Start the control panel in your browser (for example on iPad)
2. Start PMM in the browser (for example on a screen or laptop)
3. Install Percona Everest, run it in the browser and create MySQL, Postgres, MongoDB databases. 
4. Connect the databases in the control panel settings
5. Change the load on the control panel and monitor the changes on PMM.

How it works technically:
1. Control panel is a web application, when you adjust or switch, the settings are stored in the Valkey database. 
2. Dataset loader - a constantly running script that checks the settings in Valkey every 5 seconds, connects to the databases and loads the data.
3. Load Generator - a constantly running script that can work on one database or on all three databases. Every 5 seconds it checks the load settings in Valkey. Based on the settings, it creates separate go routines (parallel threads) of load, each of which performs a connection to the database and runs SQL and NoSQL queries, which can be switched in the Control Panel. Queries are available in the file internal/load/load.go 

## Running locally with Docker 

1. Clone the project repository

```
git clone git@github.com:dbazhenov/github-stat.git
```

2. Copy or rename `.env.example` to `.env`. Set the parameters in the .env file. 

3. Run the environment. 

```
docker compose up -d
```

4. Launch the application at `localhost:3000` in your browser.

5. Open the Settings tab and create connections to the databases you want to load. 

You can use any of your databases, such as those running with Percona Everest or Docker. 

If you don't have databases, start them using the command:

```
docker compose -f docker-compose-dbs.yaml up -d
```

docker-compose launches three databases (MySQL, Postgres, MongoDB) and PMM with pmm-client.

Connection options are available in docker-compose-dbs.yaml

    - MySQL: `root:password@tcp(mysql:3306)/dataset`

    - Postgres: `user=postgres password='password' dbname=dataset host=postgres port=5432 sslmode=disable`

    - MongoDB: `mongodb://databaseAdmin:password@mongodb:27017/`

6. On the Settings tab, for each database, load the test dataset by clicking the “Create schema” and “Import Dataset” buttons. By default a small dataset from a CSV file (26 repos and 4600 PRs) will be imported, to import the full dataset you need to add a GitHub Token to the .env file and change the import type to github.

7. Turn on the Enable Load setting and open the Load Generator Control Panel tab.

8. Change load adjustments and check the results in PMM at `localhost:8081`

## Development environment

0. Run the environment. 

```
docker compose -f docker-compose-dev.yaml up -d
```

1. Run the Control Panel script

`go run cmd/web/main.go`

Launch the control panel in your browser (localhost:3000).

2. Run the Dataset loader script

`go run cmd/dataset/main.go`

3. Run the Load Generator script

`go run cmd/load/main.go`

7. Optional. Uncomment the configuration for PMM and PMM Client in docker-compose.yaml. Run PMM

`docker compose up -d`

Start PMM in your browser (localhost)

## Launching in Kubernetes

1. Create a Kubernetes cluster, for example in minikube or GKE. For GKE I use the command 

`gcloud container clusters create demo-app --project percona-product --zone us-central1-a --cluster-version 1.30 --machine-type n1-standard-16 --num-nodes=1`

Doc: [Create Kubernetes cluster on Google Kubernetes Engine (GKE)](https://docs.percona.com/everest/quickstart-guide/gke.html#environment-setup)

2. Install [Percona Everest](https://docs.percona.com/everest/index.html) to create databases or [Percona Operators](https://docs.percona.com/percona-operators/).

Percona Everest documentation:
- [Install Everest CLI](https://docs.percona.com/everest/install/installEverestCLI.html)
- [Install Everest](https://docs.percona.com/everest/install/installEverest.html)

Create databases if you don't have any.

3. Install the PMM, e.g. with HELM

```
helm repo add percona https://percona.github.io/percona-helm-charts/
```

```
helm install pmm -n demo \
--set service.type="LoadBalancer" \
--set pmmResources.limits.memory="4Gi" \
--set pmmResources.limits.cpu="2" \
percona/pmm
```

Get the administrator password (admin user)

```
kubectl get secret pmm-secret -n demo -o jsonpath='{.data.PMM_ADMIN_PASSWORD}' | base64 --decode
```

Get a public IP to open PMM in a browser

```
kubectl get svc -n demo monitoring-service -o jsonpath="{.status.loadBalancer.ingress[0].ip}"
```

Next, you can run the Demo application, either manually or with Helm.

### Running Demo application using HELM

1. Set the HELM parameters in the ./k8s/demo-app/values.yaml file:

`githubToken` -  is required to properly load the dataset from the GitHub API. You can create a personal Token at [https://github.com/settings/tokens](https://github.com/settings/tokens).

`separateLoads` - If true, separate pods for each database will be started for the load.

`useResourceLimits` - if true, resource limits will be set for the resource consumption

`controlPanelService.type` - LoadBalancer for the public address of the dashboard. NodePort for developing locally.

2. Launch the application

```
helm install demo-app ./k8s/demo-app -n demo
```

3. Run `kubectl -n demo get svc` to get the public IP for demo-app-web-service. Launch the control panel in your browser.

4. Open the Settings tab on the control panel and set the parameters for connecting to the databases you created in Percona Everest or with Percona Operators.

5. You may need to restart the dataset pod to speed up the process of loading the dataset into the databases.

```
kubectl -n demo delete pod [DATASET_POD]
```

6. You can change the allocated resources or the number of replicas by editing the `values.yaml` file and issuing the command 

```
helm upgrade demo-app ./k8s/demo-app -n demo
```

### Running Demo application manually

1. Create Secrets and ConfigMap for the application.

```
kubectl apply -f k8s/config.yaml -n demo
```

Check the k8s/config.yaml file. Be sure to set `GITHUB_TOKEN`, which is required to properly load the dataset from the GitHub API. You can create a personal Token at [https://github.com/settings/tokens](https://github.com/settings/tokens).

2. Run Valkey database

```
kubectl apply -f k8s/valkey.yaml -n demo
```

3. Run the Control Panel script

```
kubectl apply -f k8s/web-deployment.yaml -n demo
```

Run `kubectl -n demo get svc` to get the public IP. Launch the control panel in your browser.

Open the control panel in your browser. Open the Settings tab. Set the connection string to the databases created in Percona Everest. Click the Connect button. 

The first time you connect to MySQL and Postgres, you will need to create a schema and tables. You will see the buttons on the Settings tab. 

4. Run the Dataset loader script

```
kubectl apply -f k8s/dataset-deployment.yaml -n demo
```

5. Run the Load Generator script.

If one script for all databases. 

```
kubectl apply -f k8s/load-deployment.yaml -n demo
```

You can run a separate load generator for each database. To distribute resources or scale the load.

- MySQL: `kubectl apply -f k8s/load-mysql-deployment.yaml -n demo`
- Postgres: `kubectl apply -f k8s/load-postgres-deployment.yaml -n demo`
- MongoDB `kubectl apply -f k8s/load-mongodb-deployment.yaml -n demo`

You can set the environment variable to determine which database the script will load.

6. Control the load in the control panel. Change queries using the switches. Track the result on PMM dashboards. Scale or change database parameters with Percona Everest. 

Have fun experimenting. 

### Useful commands

`kubectl get pods -n demo`

`kubectl logs [pod_name] -n demo`

`kubectl describe pod [pod_name] -n demo`

## How to make changes to the code.

1. Clone the repository 

2. Run locally using Docker Compose. 

3. Make changes to the code and run scripts for tests. 

4. The repository contains Workflow to build and publish to Docker Hub. You can publish your own versions of containers and run them in Kubernetes. 

5. Send your changes to the project using Pull Request. 

You are invited to make a contribution:
1. Suggest improvements and create Issues
2. Improve code or do a review.
