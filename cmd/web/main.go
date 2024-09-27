package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"

	app "github-stat/internal"
	"github-stat/internal/databases/valkey"
)

// Global variable to store the configuration. Default values can be set.
var LoadConfig = app.Load{
	MySQLConnections:      0,
	PostgreSQLConnections: 0,
	MongoDBConnections:    0,
	MySQLSwitch:           false,
	PostgreSQLSwitch:      false,
	MongoDBSwitch:         false,
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
	loadSettingFromValkey, err := valkey.LoadConfigFromValkey()
	if err != nil {
		log.Print("Valkey: Load Config Empty")
	} else {
		LoadConfig = loadSettingFromValkey
		log.Printf("Valkey: Load Config: %v", LoadConfig)
	}

}

func handleRequest() {

	// http.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir("./assets/"))))
	http.HandleFunc("/", index)
	http.HandleFunc("/config", config)
	http.HandleFunc("/start", start)

	host := fmt.Sprintf("%s:%s", app.Config.ControlPanel.Host, app.Config.ControlPanel.Port)
	fmt.Printf("\nYou can open the control panel in your browser at http://%s\n\n", host)
	http.ListenAndServe(host, nil)

}

func index(w http.ResponseWriter, r *http.Request) {
	log.Printf("Index start LoadConfig: %v", LoadConfig)
	t, err := template.ParseFiles("templates/index.html", "templates/header.html", "templates/footer.html")
	if err != nil {
		log.Fatal("Errors: Index: Templates: ", err)
	}

	t.ExecuteTemplate(w, "index", LoadConfig)
}

func config(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {

		var load app.Load

		err := json.NewDecoder(r.Body).Decode(&load)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		log.Printf("API: Received values: MySQL: %d, PostgreSQL: %d, MongoDB: %d, MySQL Switch: %t, PostgreSQL Switch: %t, MongoDB Switch: %t",
			load.MySQLConnections, load.PostgreSQLConnections, load.MongoDBConnections, load.MySQLSwitch, load.PostgreSQLSwitch, load.MongoDBSwitch)

		LoadConfig = load

		err = valkey.SaveConfigToValkey(load)
		if err != nil {
			log.Printf("Valkey: Save Config: Error: %s", err)
		} else {
			log.Printf("Valkey: Save Config: Success: %v", load)
		}

		w.Header().Set("Content-Type", "application/json")
		response := map[string]interface{}{
			"mysql_connections":      load.MySQLConnections,
			"postgresql_connections": load.PostgreSQLConnections,
			"mongodb_connections":    load.MongoDBConnections,
			"mysql_switch":           load.MySQLSwitch,
			"postgresql_switch":      load.PostgreSQLSwitch,
			"mongodb_switch":         load.MongoDBSwitch,
		}

		json.NewEncoder(w).Encode(response)

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
