package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
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
	PostgresSwitch1:       true,
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
	loadConfigFromValkey, err := valkey.LoadConfigFromValkey()
	if err != nil {
		log.Print("Valkey: Load Config Empty")
	} else {
		LoadConfig = loadConfigFromValkey
		log.Printf("Valkey: Load Config: %v", LoadConfig)
	}

}

func handleRequest() {

	// http.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir("./assets/"))))
	http.HandleFunc("/", index)
	http.HandleFunc("/config", config)
	http.HandleFunc("/start", start)
	http.HandleFunc("/settings", settings)
	http.HandleFunc("/dataset", dataset)
	host := fmt.Sprintf("%s:%s", app.Config.ControlPanel.Host, app.Config.ControlPanel.Port)
	fmt.Printf("\nYou can open the control panel in your browser at http://%s\n\n", host)
	http.ListenAndServe(host, nil)

}

func index(w http.ResponseWriter, r *http.Request) {
	log.Printf("Index start LoadConfig: %v", LoadConfig)
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

func prepareIndexData() app.IndexData {

	var Settings app.Connections

	loadConnectionsSettings, err := valkey.LoadFromValkey("db_connections")
	if err != nil {
		log.Printf("Valkey: Load Connections: Empty: %s", err)
	} else {
		log.Printf("Valkey: Load Connections: Load Config: %v", loadConnectionsSettings)
	}

	if loadConnectionsSettings.MongoDBConnectionString != "" {
		Settings.MongoDBConnectionString = loadConnectionsSettings.MongoDBConnectionString
	} else {
		Settings.MongoDBConnectionString = fmt.Sprintf("mongodb://%s:%s@%s:%s/",
			app.Config.MongoDB.User,
			app.Config.MongoDB.Password,
			app.Config.MongoDB.Host,
			app.Config.MongoDB.Port)
	}

	if loadConnectionsSettings.MySQLConnectionString != "" {
		Settings.MySQLConnectionString = loadConnectionsSettings.MySQLConnectionString
	} else {
		Settings.MySQLConnectionString = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			app.Config.MySQL.User,
			app.Config.MySQL.Password,
			app.Config.MySQL.Host,
			app.Config.MySQL.Port,
			app.Config.MySQL.DB)
	}
	if loadConnectionsSettings.PostgresConnectionString != "" {
		Settings.PostgresConnectionString = loadConnectionsSettings.PostgresConnectionString
	} else {
		Settings.PostgresConnectionString = fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
			app.Config.Postgres.User,
			app.Config.Postgres.Password,
			app.Config.Postgres.DB,
			app.Config.Postgres.Host,
			app.Config.Postgres.Port)
	}

	Settings.MySQLStatus = mysql.CheckMySQL(Settings.MySQLConnectionString)
	Settings.MongoDBStatus = mongodb.CheckMongoDB(Settings.MongoDBConnectionString)
	Settings.PostgresStatus = postgres.CheckPostgreSQL(Settings.PostgresConnectionString)

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
			log.Printf("Valkey: Save Config: Error: %s", err)
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

		LoadConfig, err := valkey.LoadConfigFromValkey()
		if err != nil {
			log.Printf("Valkey: Load Config Error: %s", err)
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

		mysqlStatus := mysql.CheckMySQL(mysqlConnectionString)

		valkeySettings := app.Connections{}

		if mysqlStatus == "Connected" {
			valkeySettings.MySQLConnectionString = mysqlConnectionString
		}

		mongodbStatus := mongodb.CheckMongoDB(mongodbConnectionString)

		if mongodbStatus == "Connected" {
			valkeySettings.MongoDBConnectionString = mongodbConnectionString
		}

		postgresqlStatus := postgres.CheckPostgreSQL(postgresqlConnectionString)

		if postgresqlStatus == "Connected" {
			valkeySettings.PostgresConnectionString = postgresqlConnectionString
		}

		err := valkey.SaveToValkey("db_connections", valkeySettings)
		if err != nil {
			log.Printf("Valkey: Settings: Save Connections: Error: %s", err)
		} else {
			log.Printf("Valkey: Settings: Save Connections: Success: %v", valkeySettings)
		}

		data := map[string]interface{}{
			"mysql_status":    mysqlStatus,
			"mongodb_status":  mongodbStatus,
			"postgres_status": postgresqlStatus,
		}

		log.Printf("Form: %v", data)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}
func dataset(w http.ResponseWriter, r *http.Request) {
	log.Printf("Method: %v", r.Method)

	var wg sync.WaitGroup
	results := make(chan struct {
		dbType string
		data   app.Database
	}, 3)

	// MySQL
	wg.Add(1)
	go func() {
		defer wg.Done()
		my, err := mysql.Connect(app.Config)
		if err != nil {
			log.Printf("Dataset: MySQL: Connect error: %s", err)
			return
		}
		defer my.Close()

		mysql_pulls, _ := mysql.SelectInt(my, `SELECT COUNT(*) FROM github.pulls;`)
		mysql_repositories, _ := mysql.SelectInt(my, `SELECT COUNT(*) FROM github.repositories;`)

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
		pg, err := postgres.Connect(app.Config)
		if err != nil {
			log.Printf("Dataset: Postgres: Connect error: %s", err)
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
		mongo, err := mongodb.Connect(app.Config, mongo_ctx)
		if err != nil {
			log.Printf("Dataset: MongoDB: Connect error: %s", err)
			return
		}
		defer mongo.Disconnect(mongo_ctx)

		mongo_pulls, err := mongodb.CountDocuments(mongo, "github", "pulls", bson.D{})
		if err != nil {
			log.Fatal(err)
		}

		mongo_repositories, err := mongodb.CountDocuments(mongo, "github", "repositories", bson.D{})
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
