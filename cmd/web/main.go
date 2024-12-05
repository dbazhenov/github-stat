package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	app "github-stat/internal"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"

	"github-stat/internal/databases/valkey"

	"go.mongodb.org/mongo-driver/bson"
)

func main() {

	// Initialization of global variables:
	// - app.Config - environment variables for connecting to the database
	// - valkey.Valkey - Valkey client for saving and retrieving load settings
	initConfig()
	// Close the connection with Valkey at the end of the program execution
	defer valkey.Valkey.Close()

	// Starting a web server
	handleRequest()
}

func initConfig() {

	// Initializing app.Config with environment variables
	app.InitConfig("web")

	// Valkey client (valkey.Valkey) initialization
	valkey.InitValkey(app.Config)

}

func handleRequest() {

	http.HandleFunc("/", index)
	http.HandleFunc("/dataset", dataset)
	http.HandleFunc("/create_db", createDatabase)
	http.HandleFunc("/database_list", databaseList)
	http.HandleFunc("/update_db/", updateDatabase)
	http.HandleFunc("/delete_db", deleteDatabase)
	http.HandleFunc("/load_db", loadDatabase)
	http.HandleFunc("/manage-dataset/", manageDataset)

	port := app.Config.ControlPanel.Port
	if port == "" {
		port = "8080" // standard port, if not specified
	}

	fmt.Printf("\nYou can open the control panel in your browser at http://localhost:%s\n\n", port)
	http.ListenAndServe(":"+port, nil)

}

func manageDataset(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/manage-dataset/")

	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	action := r.FormValue("action")

	log.Printf("manageDataset: Action: %s, id: %s", action, id)

	fields := make(map[string]string)

	if action == "stop" {
		fields["datasetStatus"] = ""
	} else {
		fields["datasetStatus"] = "Waiting"
	}

	err := valkey.AddDatabase(id, fields)
	if err != nil {
		log.Printf("Error: Updating database: %v", err)
		http.Error(w, "Error updating database", http.StatusInternalServerError)
		return
	}

	data := map[string]string{
		"status":        "success",
		"datasetStatus": fields["datasetStatus"],
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(data)
}

func prepareIndexData() app.IndexData {
	// Fetch database configurations from Redis
	databases, err := valkey.GetDatabases()
	if err != nil {
		log.Printf("Error: Getting databases: %v", err)
	}

	// Sort databases by position
	sort.Slice(databases, func(i, j int) bool {
		pos1, _ := strconv.Atoi(databases[i]["position"])
		pos2, _ := strconv.Atoi(databases[j]["position"])
		return pos1 < pos2
	})

	// Filter databases with loadSwitch set to true
	var databasesLoad []map[string]string
	for _, db := range databases {
		if db["loadSwitch"] == "true" {
			databasesLoad = append(databasesLoad, db)
		}
	}

	// Initialize IndexData with actual values from Valkey
	data := app.IndexData{
		Databases:        databases,
		DatabasesLoad:    databasesLoad,
		DatabasesDataset: fetchDatabasesDataset(databases),
		DatasetState:     fetchDatasetState(),
	}

	return data
}

// fetchDatasetState retrieves the dataset state from Valkey and returns it as an app.DatasetState.
func fetchDatasetState() app.DatasetState {
	// Get data from Valkey
	data := valkey.GetDatasetState()

	// If no data is available, set default values
	if data == nil {
		return app.DatasetState{
			Status:     "Not Started",
			Type:       "",
			ReposCount: 0,
			PullsCount: 0,
		}
	}

	// Convert data to DatasetState
	datasetState := app.DatasetState{
		Status:     data["status"].(string),
		Type:       data["type"].(string),
		ReposCount: int(data["repos_count"].(float64)), // assuming the numbers come back as float64
		PullsCount: int(data["pulls_count"].(float64)), // assuming the numbers come back as float64
	}

	// Get the latest report from Valkey
	report, err := valkey.GetLatestDatasetReport()
	if err != nil {
		log.Printf("Error getting latest report: %v", err)
		datasetState.LastUpdate = ""
	} else {
		// Extract the last update date
		if finishedAt, ok := report["FinishedAt"]; ok {
			datasetState.LastUpdate = finishedAt.(string)
		} else {
			datasetState.LastUpdate = ""
		}
	}

	return datasetState
}

func fetchDatabasesDataset(databases []map[string]string) []app.DatabaseInfo {
	// Channel to collect results from go-routines
	results := make(chan struct {
		db  app.DatabaseInfo
		err error
	}, len(databases))

	// Fetch dataset information from each database
	for _, db := range databases {
		go func(db map[string]string) {
			dbData, err := getDataFromDatabase(db)
			results <- struct {
				db  app.DatabaseInfo
				err error
			}{
				db:  dbData,
				err: err,
			}
		}(db)
	}

	// Collect results
	var databasesDataset []app.DatabaseInfo
	for i := 0; i < len(databases); i++ {
		result := <-results
		if result.err == nil {
			databasesDataset = append(databasesDataset, result.db)
		}
	}

	close(results)

	return databasesDataset
}

// getDataFromDatabase retrieves the dataset information from a database based on its type and connection string.
func getDataFromDatabase(db map[string]string) (app.DatabaseInfo, error) {
	id := db["id"]
	dbType := db["dbType"]
	connectionString := db["connectionString"]
	datasetStatus := db["datasetStatus"]
	var dbData app.DatasetInfo
	var err error

	switch dbType {
	case "mysql":
		dbData, err = getMySQLData(connectionString)
	case "postgres":
		dbData, err = getPostgresData(connectionString)
	case "mongodb":
		dbData, err = getMongoDBData(db)
	default:
		return app.DatabaseInfo{}, fmt.Errorf("unsupported database type: %s", dbType)
	}

	if err != nil {
		return app.DatabaseInfo{}, err
	}

	return app.DatabaseInfo{
		ID:           id,
		DBType:       dbType,
		DBName:       dbData.DBName,
		Repositories: dbData.Repositories,
		PullRequests: dbData.PullRequests,
		LastUpdate:   dbData.LastUpdate,
		Status:       datasetStatus,
	}, nil
}

// getMySQLData retrieves the dataset information from a MySQL database.
func getMySQLData(connectionString string) (app.DatasetInfo, error) {
	my, err := mysql.ConnectByString(connectionString)
	if err != nil {
		log.Printf("Error: Dataset: MySQL: Connect: %s", err)
		return app.DatasetInfo{}, err
	}
	defer my.Close()

	dbName := ""
	err = my.QueryRow(`SELECT DATABASE();`).Scan(&dbName)
	if err != nil {
		log.Printf("Error: Dataset: MySQL: Getting MySQL DB name: %v", err)
		dbName, err = mysql.GetDbName(connectionString)
		if err != nil {
			log.Printf("Error: Dataset: MySQL: Getting PostgreSQL DB name: %v", err)
		}
		return app.DatasetInfo{
			DBName: dbName,
		}, nil
	} else {
		mysql_pulls, err := mysql.SelectInt(my, `SELECT COUNT(*) FROM pulls;`)
		if err != nil {
			log.Printf("Error: Dataset: MySQL: %s: Pulls: %v", dbName, err)
		}
		mysql_repositories, err := mysql.SelectInt(my, `SELECT COUNT(*) FROM repositories;`)
		if err != nil {
			log.Printf("Error: Dataset: MySQL: %s: Repos: %v", dbName, err)
		}

		// Get the latest update from the reports_dataset table
		var lastUpdate string
		err = my.QueryRow(`
			SELECT JSON_UNQUOTE(JSON_EXTRACT(data, '$.finished_at'))
			FROM reports_dataset
			ORDER BY JSON_UNQUOTE(JSON_EXTRACT(data, '$.finished_at')) DESC
			LIMIT 1
		`).Scan(&lastUpdate)
		if err != nil {
			log.Printf("Error: Dataset: MySQL: %s: Last update: %v", dbName, err)
		}

		return app.DatasetInfo{
			DBName:       dbName,
			Repositories: mysql_repositories,
			PullRequests: mysql_pulls,
			LastUpdate:   lastUpdate,
		}, nil
	}
}

// getPostgresData retrieves the dataset information from a PostgreSQL database.
func getPostgresData(connectionString string) (app.DatasetInfo, error) {
	pg, err := postgres.ConnectByString(connectionString)
	if err != nil {
		log.Printf("Error: Dataset: Postgres: Connect: %s", err)

	}
	defer pg.Close()

	dbName := ""
	err = pg.QueryRow(`SELECT current_database();`).Scan(&dbName)
	if err != nil {
		log.Printf("Error: Dataset: Postgres: Getting PostgreSQL DB name: %v", err)
		dbName, err = postgres.GetDbName(connectionString)
		if err != nil {
			log.Printf("Error: Dataset: Postgres: Getting PostgreSQL DB name: %v", err)
		}
		return app.DatasetInfo{
			DBName: dbName,
		}, nil
	} else {

		pg_pulls, err := postgres.SelectInt(pg, `SELECT COUNT(*) FROM github.pulls;`)
		if err != nil {
			log.Printf("Error: Dataset: Postgres: %s: Pulls: %v", dbName, err)
		}
		pg_repositories, err := postgres.SelectInt(pg, `SELECT COUNT(*) FROM github.repositories;`)
		if err != nil {
			log.Printf("Error: Dataset: Postgres: %s: Repos: %v", dbName, err)
		}

		// Get the latest update from the reports_dataset table
		var lastUpdate string
		err = pg.QueryRow(`
			SELECT data->>'finished_at'
			FROM github.reports_dataset
			ORDER BY data->>'finished_at' DESC
			LIMIT 1
		`).Scan(&lastUpdate)
		if err != nil {
			log.Printf("Error: Dataset: Postgres: %s: Last update: %v", dbName, err)
		}

		return app.DatasetInfo{
			DBName:       dbName,
			Repositories: pg_repositories,
			PullRequests: pg_pulls,
			LastUpdate:   lastUpdate,
		}, nil
	}

}

// getMongoDBData retrieves the dataset information from a MongoDB database.
func getMongoDBData(db map[string]string) (app.DatasetInfo, error) {
	connectionString := db["connectionString"]
	dbName := db["database"] // Use the database field from db map

	mongo_ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	mongo, err := mongodb.ConnectByString(connectionString, mongo_ctx)
	if err != nil {
		log.Printf("Error: Dataset: MongoDB: Connect: %s", err)
		return app.DatasetInfo{
			DBName: dbName,
		}, nil
	}
	defer mongo.Disconnect(mongo_ctx)

	mongo_pulls, err := mongodb.CountDocuments(mongo, dbName, "pulls", bson.D{})
	if err != nil {
		log.Printf("Error: Dataset: MongoDB: Count Pulls: %s", err)
		mongo_pulls = 0
	}

	mongo_repositories, err := mongodb.CountDocuments(mongo, dbName, "repositories", bson.D{})
	if err != nil {
		log.Printf("Error: Dataset: MongoDB: Count Repos: %s", err)
		mongo_repositories = 0
	}

	// Use the new function to get the latest update from the reports_dataset collection
	lastUpdate, err := mongodb.GetDatasetLatestUpdates(db)
	if err != nil {
		log.Printf("Error: Dataset: MongoDB: Last update: %v", err)
	}

	return app.DatasetInfo{
		DBName:       dbName,
		Repositories: int(mongo_repositories),
		PullRequests: int(mongo_pulls),
		LastUpdate:   lastUpdate,
	}, nil
}

func createDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		dbType := r.FormValue("dbType")
		connectionString := r.FormValue("connectionString")
		mongodbDatabase := r.FormValue("mongodbDatabase")

		maxID, err := valkey.GetMaxID(dbType)
		if err != nil {
			log.Printf("Error: Getting max ID: %v", err)
			http.Error(w, "Error getting max ID", http.StatusInternalServerError)
			return
		}

		newID := maxID + 1
		id := fmt.Sprintf("%s-%d", dbType, newID)

		fields := map[string]string{
			"id":               id,
			"dbType":           dbType,
			"connectionString": connectionString,
			"loadSwitch":       "false",
			"position":         "0",
			"sleep":            "100",
			"connections":      "0",
			"switch1":          "false",
			"switch2":          "false",
			"switch3":          "false",
			"switch4":          "false",
		}

		if dbType == "mongodb" {
			fields["database"] = mongodbDatabase
		}

		textMessage := ""
		if dbType == "mysql" {
			fields["connectionStatus"] = mysql.CheckMySQL(connectionString)

			if fields["connectionStatus"] == "Connected" {
				fields["schemaStatus"] = "true"

				textMessage = fmt.Sprintf(
					`Database connection (ID: <a href="#formDatabases-%s">%s</a>) has been successfully created. To add to the Load Generator Control Panel enable <a href="#formDatabases-%s">the Enable Load</a> switch.`,
					id, id, id,
				)
			} else if strings.Contains(fields["connectionStatus"], "Unknown database") {

				fields["updateStatus"] = fmt.Sprintf("Database connection (id: %s) has been successfully created, but the database schema is missing. To create it, click the Create Schema button below.", id)
				fields["schemaStatus"] = "false"
				textMessage = fmt.Sprintf(
					`Database connection (ID: <a href="#formDatabases-%s">%s</a>) has been successfully created, but the database schema is missing. To create it, click the <a href="#formDatabases-%s">Create Schema</a> button below.`,
					id, id, id,
				)
			} else {
				textMessage = fmt.Sprintf(
					`Connection (ID: <a href="#formDatabases-%s">%s</a>) to the database was created, but an error occurred while trying to connect. Please check the connection string in the list below. Error: %s`,
					id, id, fields["connectionStatus"],
				)
			}

		} else if dbType == "postgres" {
			fields["connectionStatus"] = postgres.CheckPostgreSQL(connectionString)

			if fields["connectionStatus"] == "Connected" {
				fields["schemaStatus"] = "true"

				textMessage = fmt.Sprintf(
					`Database connection (ID: <a href="#formDatabases-%s">%s</a>) has been successfully created. To add to the Load Generator Control Panel enable <a href="#formDatabases-%s">the Enable Load</a> switch.`,
					id, id, id,
				)

			} else if strings.Contains(fields["connectionStatus"], "does not exist") || strings.Contains(fields["connectionStatus"], "server login has been failing") {

				fields["updateStatus"] = fmt.Sprintf("Database connection (id: %s) has been successfully created, but the database schema is missing. To create it, click the Create Schema button below.", id)
				fields["schemaStatus"] = "false"
				textMessage = fmt.Sprintf(
					`Database connection (ID: <a href="#formDatabases-%s">%s</a>) has been successfully created, but the database schema is missing. To create it, click the <a href="#formDatabases-%s">Create Schema</a> button below.`,
					id, id, id,
				)

			} else {
				textMessage = fmt.Sprintf(
					`Connection (ID: <a href="#formDatabases-%s">%s</a>) to the database was created, but an error occurred while trying to connect. Please check the connection string in the list below. Error: %s`,
					id, id, fields["connectionStatus"],
				)
			}

		} else if dbType == "mongodb" {

			fields["connectionStatus"] = mongodb.CheckMongoDB(connectionString)
			if fields["connectionStatus"] == "Connected" {
				textMessage = fmt.Sprintf(
					`Database connection (ID: <a href="#formDatabases-%s">%s</a>) has been successfully created. To add to the Load Generator Control Panel enable <a href="#formDatabases-%s">the Enable Load</a> switch.`,
					id, id, id,
				)
			} else {
				textMessage = fmt.Sprintf(
					`Connection (ID: <a href="#formDatabases-%s">%s</a>) to the database was created, but an error occurred while trying to connect. Please check the connection string in the list below. Error: %s`,
					id, id, fields["connectionStatus"],
				)
			}
		}

		err = valkey.AddDatabase(id, fields)
		if err != nil {
			log.Printf("Error: Creating database: %v", err)
			http.Error(w, "Error creating database", http.StatusInternalServerError)
			return
		}

		data := map[string]string{
			"status":       "success",
			"id":           id,
			"updateStatus": fields["updateStatus"],
			"textMessage":  textMessage,
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(data)
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func loadDatabase(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id")
	connections := r.FormValue("connections")
	switch1 := convertSwitch(r.FormValue("switch1"))
	switch2 := convertSwitch(r.FormValue("switch2"))
	switch3 := convertSwitch(r.FormValue("switch3"))
	switch4 := convertSwitch(r.FormValue("switch4"))

	fieldsToUpdate := map[string]string{
		"connections": connections,
		"switch1":     switch1,
		"switch2":     switch2,
		"switch3":     switch3,
		"switch4":     switch4,
	}

	err := valkey.AddDatabase(id, fieldsToUpdate)
	if err != nil {
		log.Printf("Error: Updating database load settings: %v", err)
		http.Error(w, "Error updating database load settings", http.StatusInternalServerError)
		return
	}

	response := fieldsToUpdate
	response["id"] = id

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func convertSwitch(value string) string {
	if value == "on" {
		return "true"
	}
	return "false"
}

func updateDatabase(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/update_db/")
	connectionString := r.FormValue("connectionString")
	database := r.FormValue("database")
	dbType := r.FormValue("dbType")
	loadSwitch := convertSwitch(r.FormValue("loadSwitch"))
	position := r.FormValue("position")
	sleep := r.FormValue("sleep")

	init_schema := r.FormValue("init_schema")
	delete_schema := r.FormValue("delete_schema")

	updateStatus := ""
	if position == "" {
		position = "0"
	}
	if sleep == "" {
		sleep = "0"
	}

	fieldsToUpdate := map[string]string{
		"connectionString": connectionString,
		"database":         database,
		"loadSwitch":       loadSwitch,
		"position":         position,
		"sleep":            sleep,
	}

	currentDB, err := valkey.GetDatabase(id)
	if err != nil {
		log.Printf("Error: Getting database: %v", err)
		http.Error(w, "Error getting database", http.StatusInternalServerError)
		return
	}

	for key, value := range fieldsToUpdate {
		currentDB[key] = value
	}

	if delete_schema != "" {
		switch dbType {
		case "mysql":
			err = mysql.DeleteSchema(connectionString)
		case "postgres":
			err = postgres.DeleteSchema(connectionString)
		case "mongodb":
			err = mongodb.DeleteSchema(connectionString, database)
		}
		if err != nil {
			log.Printf("Error: Deleting schema: %v", err)
			http.Error(w, "Error deleting schema", http.StatusInternalServerError)
			return
		} else {
			updateStatus = "Schema deletion successful."
			currentDB["datasetStatus"] = ""
			currentDB["schemaStatus"] = "false"
		}
	}

	if dbType == "mysql" {
		currentDB["connectionStatus"] = mysql.CheckMySQL(connectionString)

		if currentDB["connectionStatus"] == "Connected" {
			currentDB["schemaStatus"] = "true"
			currentDB["updateStatus"] = ""
		} else if strings.Contains(currentDB["connectionStatus"], "Unknown database") {
			if init_schema != "" {
				err := mysql.InitSchema(connectionString)
				if err != nil {
					currentDB["connectionStatus"] = fmt.Sprintf("Error: MySQL Database creation error: %v", err)
					currentDB["schemaStatus"] = "false"
				} else {

					currentDB["connectionStatus"] = mysql.CheckMySQL(connectionString)
					if currentDB["connectionStatus"] == "Connected" {
						updateStatus = "The database and schema have been created. Connection is successful."
						currentDB["schemaStatus"] = "true"
						currentDB["updateStatus"] = ""
					}
				}
			} else {
				updateStatus = "You need to create a test database and a schema. Click the Create schema button."
				currentDB["updateStatus"] = updateStatus
				currentDB["schemaStatus"] = "false"
			}
		}

	} else if dbType == "postgres" {
		currentDB["connectionStatus"] = postgres.CheckPostgreSQL(connectionString)

		log.Printf("Connect 1: %v", currentDB["connectionStatus"])
		if currentDB["connectionStatus"] == "Connected" {
			currentDB["schemaStatus"] = "true"
			currentDB["updateStatus"] = ""
		} else if strings.Contains(currentDB["connectionStatus"], "does not exist") || strings.Contains(currentDB["connectionStatus"], "server login has been failing") {

			if init_schema != "" {
				err := postgres.InitSchema(connectionString)
				if err != nil {
					currentDB["connectionStatus"] = fmt.Sprintf("Error: Postgres Database creation error: %v", err)
					currentDB["schemaStatus"] = "false"
				} else {

					currentDB["connectionStatus"] = postgres.CheckPostgreSQL(connectionString)

					if currentDB["connectionStatus"] == "Connected" {
						updateStatus = "The database and schema have been created. Connection is successful."
						currentDB["schemaStatus"] = "true"
						currentDB["updateStatus"] = ""
					}
				}
			} else {
				updateStatus = "You need to create a test database and a schema. Click the Create schema button."
				currentDB["updateStatus"] = updateStatus
				currentDB["schemaStatus"] = "false"
			}
		}

	} else if dbType == "mongodb" {

		currentDB["connectionStatus"] = mongodb.CheckMongoDB(connectionString)

		log.Printf("MongoDB Status: %s", currentDB["connectionStatus"])

		if currentDB["connectionStatus"] != "Connected" {
			currentDB["datasetStatus"] = ""
		} else {
			if delete_schema == "" {
				err := mongodb.InitProfileOptions(connectionString, database)
				if err != nil {
					log.Printf("Error: MongoDB: %s: InitProfileOptions: %v", id, err)
				}
			}
		}
	}

	log.Printf("Update: ID: %s, Fields: %v", id, currentDB)

	err = valkey.AddDatabase(id, currentDB)
	if err != nil {
		log.Printf("Error: Updating database: %v", err)
		http.Error(w, "Error updating database", http.StatusInternalServerError)
		return
	}

	if updateStatus == "" {
		updateStatus = "Update successful."
	}

	data := map[string]string{
		"status":           "success",
		"connectionStatus": currentDB["connectionStatus"],
		"schemaStatus":     currentDB["schemaStatus"],
		"updateStatus":     updateStatus,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(data)
}

func deleteDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		id := r.FormValue("id")

		if id == "" {
			log.Printf("Error: No ID provided for deletion")
			http.Error(w, "No ID provided", http.StatusBadRequest)
			return
		}

		err := valkey.DeleteDatabase(id)
		if err != nil {
			log.Printf("Error: Deleting database: %v", err)
			http.Error(w, "Error deleting database", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func databaseList(w http.ResponseWriter, r *http.Request) {
	databases, err := valkey.GetDatabases()
	if err != nil {
		log.Printf("Error: Getting databases: %v", err)
		http.Error(w, "Error getting databases", http.StatusInternalServerError)
		return
	}

	sort.Slice(databases, func(i, j int) bool {
		pos1, _ := strconv.Atoi(databases[i]["position"])
		pos2, _ := strconv.Atoi(databases[j]["position"])
		return pos1 < pos2
	})

	data := map[string]interface{}{
		"Databases": databases,
	}

	tmpl, err := template.ParseFiles("templates/settings_databases.html")
	if err != nil {
		log.Printf("Error: Parsing template: %v", err)
		http.Error(w, "Error parsing template", http.StatusInternalServerError)
		return
	}

	err = tmpl.ExecuteTemplate(w, "settings_databases", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}

func index(w http.ResponseWriter, r *http.Request) {

	t, err := template.ParseFiles("templates/index.html",
		"templates/header.html",
		"templates/footer.html",
		"templates/settings.html",
		"templates/settings_databases.html",
		"templates/dataset.html",
		"templates/control.html")
	if err != nil {
		log.Fatal("Errors: Index: Templates: ", err)
	}

	data := prepareIndexData()

	t.ExecuteTemplate(w, "index", data)
}

func dataset(w http.ResponseWriter, r *http.Request) {
	data := prepareIndexData()

	tmpl, err := template.ParseFiles("templates/dataset.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = tmpl.ExecuteTemplate(w, "dataset", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
