package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"sync"

	app "github-stat/internal"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"github-stat/internal/databases/valkey"

	"go.mongodb.org/mongo-driver/bson"
)

// Global variable to store the configuration. Default values can be set.
var LoadConfig = app.Load{
	MySQLConnections:      0,
	PostgreSQLConnections: 0,
	MongoDBConnections:    0,
	MySQLSwitch1:          false,
	PostgresSwitch1:       false,
	MongoDBSwitch1:        false,
}

func main() {

	// Initialization of global variables:
	// - LoadConfig - load parameters
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

	// Getting settings from Valkey
	loadConfigFromValkey, err := valkey.LoadControlPanelConfigFromValkey()
	if err != nil {
		log.Print("Valkey: Load Config Empty")
	} else {
		LoadConfig = loadConfigFromValkey
		log.Printf("Valkey: Load Control Panel Settings: %v", LoadConfig)
	}

	getDBConnectSettings()

}

func handleRequest() {

	http.HandleFunc("/", index)
	http.HandleFunc("/config", config)
	http.HandleFunc("/start", start)
	http.HandleFunc("/settings", settings)
	http.HandleFunc("/dataset", dataset)

	port := app.Config.ControlPanel.Port
	if port == "" {
		port = "8080" // standard port, if not specified
	}

	fmt.Printf("\nYou can open the control panel in your browser at http://localhost:%s\n\n", port)
	http.ListenAndServe(":"+port, nil)

}

func index(w http.ResponseWriter, r *http.Request) {

	t, err := template.ParseFiles("templates/index.html",
		"templates/header.html",
		"templates/footer.html",
		"templates/settings.html",
		"templates/dataset.html",
		"templates/control.html")
	if err != nil {
		log.Fatal("Errors: Index: Templates: ", err)
	}

	data := prepareIndexData()

	t.ExecuteTemplate(w, "index", data)
}

func getDBConnectSettings() {

	settings, err := valkey.LoadFromValkey("db_connections")
	if err != nil {
		log.Printf("Error: Valkey: Get connections to databases: %s", err)
	} else {
		log.Printf("Valkey: Сonnections to databases: Ok")
	}

	if settings.MongoDBConnectionString != "" {
		app.Config.MongoDB.ConnectionString = settings.MongoDBConnectionString
	} else {
		app.Config.MongoDB.ConnectionString = mongodb.GetConnectionString(app.Config)
	}

	if app.Config.MongoDB.ConnectionString != "" {
		app.Config.MongoDB.ConnectionStatus = mongodb.CheckMongoDB(app.Config.MongoDB.ConnectionString)
	}

	if settings.MySQLConnectionString != "" {
		app.Config.MySQL.ConnectionString = settings.MySQLConnectionString
	} else {
		app.Config.MySQL.ConnectionString = mysql.GetConnectionString(app.Config)
	}

	if app.Config.MySQL.ConnectionString != "" {
		app.Config.MySQL.ConnectionStatus = mysql.CheckMySQL(app.Config.MySQL.ConnectionString)
	}

	if settings.PostgresConnectionString != "" {
		app.Config.Postgres.ConnectionString = settings.PostgresConnectionString
	} else {
		app.Config.Postgres.ConnectionString = postgres.GetConnectionString(app.Config)
	}

	if app.Config.Postgres.ConnectionString != "" {
		app.Config.Postgres.ConnectionStatus = postgres.CheckPostgreSQL(app.Config.Postgres.ConnectionString)
	}

}

func prepareIndexData() app.IndexData {

	var Settings app.Connections

	Settings.MongoDBConnectionString = app.Config.MongoDB.ConnectionString
	Settings.MongoDBStatus = app.Config.MongoDB.ConnectionStatus

	Settings.MySQLConnectionString = app.Config.MySQL.ConnectionString
	Settings.MySQLStatus = app.Config.MySQL.ConnectionStatus

	Settings.PostgresConnectionString = app.Config.Postgres.ConnectionString
	Settings.PostgresStatus = app.Config.Postgres.ConnectionStatus

	data := app.IndexData{
		LoadConfig: LoadConfig,
		Settings:   Settings,
	}

	return data
}

func config(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {

		var load app.Load

		err := json.NewDecoder(r.Body).Decode(&load)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		LoadConfig = load

		err = valkey.SaveConfigToValkey(load)
		if err != nil {
			log.Printf("Error: Valkey: Save Config: %s", err)
		} else {
			log.Printf("Valkey: Save Config: Success: %v", load)
		}

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(LoadConfig)

	} else {
		http.Error(w, "API: Invalid request method", http.StatusMethodNotAllowed)
	}
}

// Optional fetching of settings from Valkey after page load.
func start(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {

		LoadConfig, err := valkey.LoadControlPanelConfigFromValkey()
		if err != nil {
			log.Printf("Error: Valkey: Start: Get connections to databases: %s", err)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(LoadConfig)
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func settings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {

		mysqlConnectionString := r.FormValue("mysqlConnectionString")
		mongodbConnectionString := r.FormValue("mongodbConnectionString")
		postgresqlConnectionString := r.FormValue("postgresqlConnectionString")

		create_db_mysql := r.FormValue("create_db_mysql")
		create_db_postgres := r.FormValue("create_db_postgres")

		valkeySettings := app.Connections{}

		mysqlStatus := mysql.CheckMySQL(mysqlConnectionString)
		mysqlCreateSchema := false
		if mysqlStatus == "Connected" {
			valkeySettings.MySQLConnectionString = mysqlConnectionString
			app.Config.MySQL.ConnectionString = mysqlConnectionString
		} else if strings.Contains(mysqlStatus, "Unknown database") {

			if create_db_mysql != "" {
				err := mysql.InitDB(mysqlConnectionString)
				if err != nil {
					mysqlStatus = fmt.Sprintf("Error: MySQL Database creation error: %v", err)
					mysqlCreateSchema = true
				} else {

					mysqlStatus = mysql.CheckMySQL(mysqlConnectionString)
					if mysqlStatus == "Connected" {
						mysqlStatus = "The database and schema have been created. Connection is successful."
						valkeySettings.MySQLConnectionString = mysqlConnectionString
						app.Config.MySQL.ConnectionString = mysqlConnectionString
					}
				}
			} else {
				mysqlStatus = "Need to create a database and schema. Click the Create MySQL Database button."
				mysqlCreateSchema = true
			}
		}
		valkeySettings.MySQLStatus = mysqlStatus

		mongodbStatus := mongodb.CheckMongoDB(mongodbConnectionString)
		if mongodbStatus == "Connected" {
			valkeySettings.MongoDBConnectionString = mongodbConnectionString
			app.Config.MongoDB.ConnectionString = mongodbConnectionString
		}
		valkeySettings.MongoDBStatus = mongodbStatus

		postgresqlStatus := postgres.CheckPostgreSQL(postgresqlConnectionString)
		postgresCreateSchema := false
		if postgresqlStatus == "Connected" {
			valkeySettings.PostgresConnectionString = postgresqlConnectionString
			app.Config.Postgres.ConnectionString = postgresqlConnectionString
		} else if strings.Contains(postgresqlStatus, "does not exist") {

			if create_db_postgres != "" {
				err := postgres.InitDB(postgresqlConnectionString)
				if err != nil {
					postgresqlStatus = fmt.Sprintf("Error: Postgres Database creation error: %v", err)
					postgresCreateSchema = true
				} else {

					postgresqlStatus = postgres.CheckPostgreSQL(postgresqlConnectionString)
					if postgresqlStatus == "Connected" {
						postgresqlStatus = "The database and schema have been created. Connection is successful."
						valkeySettings.PostgresConnectionString = postgresqlConnectionString
						app.Config.Postgres.ConnectionString = postgresqlConnectionString
					}
				}
			} else {
				postgresqlStatus = "Need to create a database and schema. Click the Create Postgres Database button."
				postgresCreateSchema = true
			}
		}
		valkeySettings.PostgresStatus = postgresqlStatus

		err := valkey.SaveToValkey("db_connections", valkeySettings)
		if err != nil {
			log.Printf("Error: Valkey: Settings: Save Connections: %s", err)
		} else {
			log.Printf("Valkey: Settings: Save Connections: Success")
		}

		data := map[string]interface{}{
			"mysql_status":    mysqlStatus,
			"mongodb_status":  mongodbStatus,
			"postgres_status": postgresqlStatus,
		}

		if postgresCreateSchema {
			data["postgres_create_schema"] = postgresCreateSchema
		}
		if mysqlCreateSchema {
			data["mysql_create_schema"] = mysqlCreateSchema
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		my, err := mysql.ConnectByString(app.Config.MySQL.ConnectionString)
		if err != nil {
			log.Printf("Error: Dataset: MySQL: Connect: %s", err)
			return
		}
		defer my.Close()

		mysql_pulls, _ := mysql.SelectInt(my, `SELECT COUNT(*) FROM pulls;`)
		mysql_repositories, _ := mysql.SelectInt(my, `SELECT COUNT(*) FROM repositories;`)

		results <- struct {
			dbType string
			data   app.Database
		}{
			dbType: "mysql",
			data: app.Database{
				Repositories: mysql_repositories,
				PullRequests: mysql_pulls,
			},
		}
	}()

	// PostgreSQL
	wg.Add(1)
	go func() {
		defer wg.Done()
		pg, err := postgres.ConnectByString(app.Config.Postgres.ConnectionString)
		if err != nil {
			log.Printf("Error: Dataset: Postgres: Connect: %s", err)
			return
		}
		defer pg.Close()

		pg_pulls, _ := postgres.SelectInt(pg, `SELECT COUNT(*) FROM github.pulls;`)
		pg_repositories, _ := postgres.SelectInt(pg, `SELECT COUNT(*) FROM github.repositories;`)

		results <- struct {
			dbType string
			data   app.Database
		}{
			dbType: "postgresql",
			data: app.Database{
				Repositories: pg_repositories,
				PullRequests: pg_pulls,
			},
		}
	}()

	// MongoDB
	wg.Add(1)
	go func() {
		defer wg.Done()
		mongo_ctx := context.Background()
		mongo, err := mongodb.ConnectByString(app.Config.MongoDB.ConnectionString, mongo_ctx)
		if err != nil {
			log.Printf("Error: Dataset: MongoDB: Connect: %s", err)
			return
		}
		defer mongo.Disconnect(mongo_ctx)

		mongo_pulls, err := mongodb.CountDocuments(mongo, app.Config.MongoDB.DB, "pulls", bson.D{})
		if err != nil {
			log.Fatal(err)
		}

		mongo_repositories, err := mongodb.CountDocuments(mongo, app.Config.MongoDB.DB, "repositories", bson.D{})
		if err != nil {
			log.Fatal(err)
		}

		results <- struct {
			dbType string
			data   app.Database
		}{
			dbType: "mongodb",
			data: app.Database{
				Repositories: int(mongo_repositories),
				PullRequests: int(mongo_pulls),
			},
		}
	}()

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
