package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	app "github-stat/internal"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"

	"github-stat/internal/databases/valkey"
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
	app.InitConfig()

	// Valkey client (valkey.Valkey) initialization
	valkey.InitValkey(app.Config)

}

func handleRequest() {

	http.HandleFunc("/", index)
	http.HandleFunc("/dataset", dataset)
	http.HandleFunc("/create_db", createDatabase)
	http.HandleFunc("/database_list", databaseList)
	http.HandleFunc("/update_db/", updateDatabase)
	http.HandleFunc("/settings_load", settingsLoad)
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

	databases, err := valkey.GetDatabases()
	if err != nil {
		log.Printf("Error: Getting databases: %v", err)
	}

	sort.Slice(databases, func(i, j int) bool {
		pos1, _ := strconv.Atoi(databases[i]["position"])
		pos2, _ := strconv.Atoi(databases[j]["position"])
		return pos1 < pos2
	})

	var databasesLoad []map[string]string
	for _, db := range databases {
		if db["loadSwitch"] == "true" {
			databasesLoad = append(databasesLoad, db)
		}
	}

	data := app.IndexData{
		Databases:     databases,
		DatabasesLoad: databasesLoad,
	}

	return data
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
			"connections":      "0",
			"switch1":          "false",
			"switch2":          "false",
			"switch3":          "false",
			"switch4":          "false",
		}

		if dbType == "mongodb" {
			fields["database"] = mongodbDatabase
		}

		if dbType == "mysql" {
			fields["connectionStatus"] = mysql.CheckMySQL(connectionString)

			if fields["connectionStatus"] == "Connected" {
				fields["schemaStatus"] = "true"

			} else if strings.Contains(fields["connectionStatus"], "Unknown database") {

				fields["updateStatus"] = "Need to create a database and schema. Click the Create Schema button."
				fields["schemaStatus"] = "false"

			}

		} else if dbType == "postgres" {
			fields["connectionStatus"] = postgres.CheckPostgreSQL(connectionString)

			if fields["connectionStatus"] == "Connected" {
				fields["schemaStatus"] = "true"

			} else if strings.Contains(fields["connectionStatus"], "does not exist") || strings.Contains(fields["connectionStatus"], "server login has been failing") {

				fields["updateStatus"] = "Need to create a database and schema. Click the Create Schema button."
				fields["schemaStatus"] = "false"

			}

		} else if dbType == "mongodb" {

			fields["connectionStatus"] = mongodb.CheckMongoDB(connectionString)

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

	currentDB, err := valkey.GetDatabase(id)
	if err != nil {
		log.Printf("Error: Getting database: %v", err)
		http.Error(w, "Error getting database", http.StatusInternalServerError)
		return
	}

	log.Printf("Load settings update: ID: %s, currentDB: %v", id, currentDB)

	for key, value := range fieldsToUpdate {
		currentDB[key] = value
	}

	log.Printf("Load settings update: ID: %s, new fields: %v", id, currentDB)

	err = valkey.AddDatabase(id, currentDB)
	if err != nil {
		log.Printf("Error: Updating database load settings: %v", err)
		http.Error(w, "Error updating database load settings", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(currentDB)
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

	init_schema := r.FormValue("init_schema")
	delete_schema := r.FormValue("delete_schema")

	updateStatus := ""
	if position == "" {
		position = "0"
	}

	fieldsToUpdate := map[string]string{
		"connectionString": connectionString,
		"database":         database,
		"loadSwitch":       loadSwitch,
		"position":         position,
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
				updateStatus = "Need to create a database and schema. Click the Create Schema button."
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
				updateStatus = "Need to create a database and schema. Click the Create Schema button."
				currentDB["updateStatus"] = updateStatus
				currentDB["schemaStatus"] = "false"
			}
		}

	} else if dbType == "mongodb" {

		currentDB["connectionStatus"] = mongodb.CheckMongoDB(connectionString)

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

		log.Printf("Delete db: %s", id)

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
		"templates/settings_load.html",
		"templates/dataset.html",
		"templates/control.html")
	if err != nil {
		log.Fatal("Errors: Index: Templates: ", err)
	}

	data := prepareIndexData()

	t.ExecuteTemplate(w, "index", data)
}

func settingsLoad(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		err := r.ParseForm()
		if err != nil {
			http.Error(w, "Error parsing form", http.StatusBadRequest)
			return
		}

		for id, sleepStr := range r.Form {
			if sleepStr[0] == "" {
				sleepStr[0] = "0"
			}

			sleep, err := strconv.Atoi(sleepStr[0])
			if err != nil {
				http.Error(w, "Invalid value for sleep for id: "+id, http.StatusBadRequest)
				return
			}

			currentDB, err := valkey.GetDatabase(id)
			if err != nil {
				log.Printf("Error: Getting database: %v", err)
				http.Error(w, "Error getting database for id: "+id, http.StatusInternalServerError)
				return
			}

			currentDB["sleep"] = strconv.Itoa(sleep)

			err = valkey.AddDatabase(id, currentDB)
			if err != nil {
				log.Printf("Error: Updating database: %v", err)
				http.Error(w, "Error updating database for id: "+id, http.StatusInternalServerError)
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func dataset(w http.ResponseWriter, r *http.Request) {
	var wg sync.WaitGroup
	results := make(chan struct {
		dbType string
		data   app.Database
	}, 3)
	// MySQL
	// if app.Config.MySQL.ConnectionStatus == "Connected" {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		my, err := mysql.ConnectByString(app.Config.MySQL.ConnectionString)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: MySQL: Connect: %s", err)
	// 			return
	// 		}
	// 		defer my.Close()

	// 		dbName := ""
	// 		err = my.QueryRow(`SELECT DATABASE();`).Scan(&dbName)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: MySQL: Getting MySQL DB name: %v", err)
	// 		}

	// 		mysql_pulls, err := mysql.SelectInt(my, `SELECT COUNT(*) FROM pulls;`)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: MySQL: Pulls: %v", err)
	// 		}
	// 		mysql_repositories, err := mysql.SelectInt(my, `SELECT COUNT(*) FROM repositories;`)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: MySQL: Repos: %v", err)
	// 		}
	// 		results <- struct {
	// 			dbType string
	// 			data   app.Database
	// 		}{
	// 			dbType: "mysql",
	// 			data: app.Database{
	// 				DBName:       dbName,
	// 				Repositories: mysql_repositories,
	// 				PullRequests: mysql_pulls,
	// 			},
	// 		}
	// 	}()
	// } else {
	// 	log.Printf("Error: Dataset: MongoDB: Config: %v", app.Config.MySQL)
	// }

	// // PostgreSQL
	// if app.Config.Postgres.ConnectionStatus == "Connected" {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		pg, err := postgres.ConnectByString(app.Config.Postgres.ConnectionString)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: Postgres: Connect: %s", err)
	// 			return
	// 		}
	// 		defer pg.Close()

	// 		dbName := ""
	// 		err = pg.QueryRow(`SELECT current_database();`).Scan(&dbName)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: Postgres: Getting PostgreSQL DB name: %v", err)
	// 		}

	// 		pg_pulls, err := postgres.SelectInt(pg, `SELECT COUNT(*) FROM github.pulls;`)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: Postgres: Pulls: %v", err)
	// 		}
	// 		pg_repositories, err := postgres.SelectInt(pg, `SELECT COUNT(*) FROM github.repositories;`)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: Postgres: Repos: %v", err)
	// 		}
	// 		results <- struct {
	// 			dbType string
	// 			data   app.Database
	// 		}{
	// 			dbType: "postgresql",
	// 			data: app.Database{
	// 				DBName:       dbName,
	// 				Repositories: pg_repositories,
	// 				PullRequests: pg_pulls,
	// 			},
	// 		}
	// 	}()
	// } else {
	// 	log.Printf("Error: Dataset: MongoDB: Config: %v", app.Config.Postgres)
	// }
	// // MongoDB
	// if app.Config.MongoDB.ConnectionStatus == "Connected" {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		mongo_ctx := context.Background()
	// 		mongo, err := mongodb.ConnectByString(app.Config.MongoDB.ConnectionString, mongo_ctx)
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: MongoDB: Connect: %s", err)
	// 			return
	// 		}
	// 		defer mongo.Disconnect(mongo_ctx)

	// 		dbName := app.Config.MongoDB.DB

	// 		mongo_pulls, err := mongodb.CountDocuments(mongo, dbName, "pulls", bson.D{})
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: MongoDB: Count Pulls: %s", err)
	// 			mongo_pulls = 0
	// 		}

	// 		mongo_repositories, err := mongodb.CountDocuments(mongo, dbName, "repositories", bson.D{})
	// 		if err != nil {
	// 			log.Printf("Error: Dataset: MongoDB: Count Repos: %s", err)
	// 			mongo_repositories = 0
	// 		}

	// 		results <- struct {
	// 			dbType string
	// 			data   app.Database
	// 		}{
	// 			dbType: "mongodb",
	// 			data: app.Database{
	// 				DBName:       dbName,
	// 				Repositories: int(mongo_repositories),
	// 				PullRequests: int(mongo_pulls),
	// 			},
	// 		}
	// 	}()
	// } else {
	// 	log.Printf("Error: Dataset: MongoDB: Config: %v", app.Config.MongoDB)
	// }

	go func() {
		wg.Wait()
		close(results)
	}()

	data := app.IndexData{}
	for result := range results {
		switch result.dbType {
		case "mysql":
			data.Dataset.MySQL = result.data
		case "postgresql":
			data.Dataset.PostgreSQL = result.data
		case "mongodb":
			data.Dataset.MongoDB = result.data
		}
	}

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
