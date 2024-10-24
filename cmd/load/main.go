package main

import (
	"context"
	app "github-stat/internal"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"github-stat/internal/databases/valkey"
	"log"
	"sync"
	"time"

	"github-stat/internal/load"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var EnvVars app.EnvVars

// Global variable to store the configuration. Default values can be set.
var LoadConfig = app.Load{
	MySQLConnections:      1,
	PostgreSQLConnections: 1,
	MongoDBConnections:    1,
	MySQLSwitch1:          true,
	MySQLSwitch2:          true,
	MySQLSwitch3:          true,
	MySQLSwitch4:          true,
	PostgresSwitch1:       true,
	PostgresSwitch2:       true,
	PostgresSwitch3:       true,
	PostgresSwitch4:       true,
	MongoDBSwitch1:        true,
	MongoDBSwitch2:        true,
	MongoDBSwitch3:        true,
	MongoDBSwitch4:        true,
}

func main() {

	// Get the configuration from environment variables or .env file.
	// app.Config - contains all environment variables
	app.InitConfig()

	// Valkey client (valkey.Valkey) initialization
	valkey.InitValkey(app.Config)
	defer valkey.Valkey.Close()

	checkControlPanelSettings()
	checkConnectSettings()

	if app.Config.LoadGenerator.MySQL {
		go manageLoad("MySQL")
	}

	if app.Config.LoadGenerator.MongoDB {
		go manageLoad("MongoDB")
	}

	if app.Config.LoadGenerator.Postgres {
		go manageLoad("Postgres")
	}

	// Continuously check and update the configuration from the control panel every 3 seconds
	for {
		time.Sleep(10 * time.Second)

		checkControlPanelSettings()
		checkConnectSettings()
	}

}

func manageLoad(db string) {

	checkOrWaitDB(db)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	currentConnections := getCurrentConnections(db)

	routines := make(map[int]context.CancelFunc)

	// Initial startup of Go routines
	for i := 0; i < currentConnections; i++ {
		wg.Add(1)
		rctx, rcancel := context.WithCancel(ctx)
		routines[i] = rcancel
		go func(id int, rctx context.Context) {
			defer wg.Done()
			runDB(db, rctx, id)
		}(i, rctx)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Check DB connection status
			connect := checkConnection(db)

			if !connect {
				log.Printf("%s: Detected disconnect, restarting routines.", db)

				// Cancel all running goroutines
				for _, cancel := range routines {
					cancel()
				}

				// Wait for all routines to finish
				wg.Wait()

				// Clear routines map
				routines = make(map[int]context.CancelFunc)

				// Reconnect and restart goroutines
				checkOrWaitDB(db)
				for i := 0; i < currentConnections; i++ {
					wg.Add(1)
					rctx, rcancel := context.WithCancel(ctx)
					routines[i] = rcancel
					go func(id int, rctx context.Context) {
						defer wg.Done()
						runDB(db, rctx, id)
					}(i, rctx)
				}
			}

			// Manage changes in number of connections
			newConnections := getCurrentConnections(db)
			if currentConnections != newConnections {
				log.Printf("%s: Manage: Change in the number of connections: %d -> %d", db, currentConnections, newConnections)

				// Reduce the number of connections
				if newConnections < currentConnections {
					for i := newConnections; i < currentConnections; i++ {
						if rcancel, exists := routines[i]; exists {
							rcancel()
							delete(routines, i)
						}
					}
				}

				// Increase the number of connections
				if newConnections > currentConnections {
					for i := currentConnections; i < newConnections; i++ {
						wg.Add(1)
						rctx, rcancel := context.WithCancel(ctx)
						routines[i] = rcancel
						go func(id int, rctx context.Context) {
							defer wg.Done()
							runDB(db, rctx, id)
						}(i, rctx)
					}
				}

				currentConnections = newConnections
			}

			time.Sleep(3 * time.Second)
		}
	}
}

func runDB(db string, rctx context.Context, id int) {
	if db == "MongoDB" {
		runMongoDB(rctx, id)
	} else if db == "MySQL" {
		runMySQL(rctx, id)
	} else if db == "Postgres" {
		runPostgreSQL(rctx, id)
	}
}

func getCurrentConnections(db string) int {
	var currentConnections int
	if db == "MongoDB" {
		currentConnections = LoadConfig.MongoDBConnections
	} else if db == "MySQL" {
		currentConnections = LoadConfig.MySQLConnections
	} else if db == "Postgres" {
		currentConnections = LoadConfig.PostgreSQLConnections
	}

	return currentConnections
}

func runMySQL(ctx context.Context, id int) {

	db, err := mysql.ConnectByString(app.Config.MySQL.ConnectionString)
	if err != nil {
		log.Printf("MySQL: Error: goroutine: %d: message: %s", id+1, err)
	}
	defer db.Close()

	log.Printf("MySQL: goroutine %d in progress", id+1)

	for {
		select {
		case <-ctx.Done():
			log.Printf("MySQL: goroutine: %d stopped", id+1)
			return
		default:

			if LoadConfig.MySQLSwitch1 {
				load.MySQLSwitch1(db, id)
			}

			if LoadConfig.MySQLSwitch2 {
				load.MySQLSwitch2(db, id)
			}

			if LoadConfig.MySQLSwitch3 {
				load.MySQLSwitch3(db, id)
			}

			if LoadConfig.MySQLSwitch4 {
				load.MySQLSwitch4(db, id)
			}

		}
	}
}

func runPostgreSQL(ctx context.Context, id int) {

	db, err := postgres.ConnectByString(app.Config.Postgres.ConnectionString)
	if err != nil {
		log.Printf("Postgres: Error: goroutine: %d: message: %s", id+1, err)
	}
	defer db.Close()

	log.Printf("Postgres: goroutine %d in progress", id+1)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Postgres: goroutine %d stopped", id+1)
			return
		default:

			if LoadConfig.PostgresSwitch1 {
				load.PostgresSwitch1(db, id)
			}

			if LoadConfig.PostgresSwitch2 {
				load.PostgresSwitch2(db, id)
			}

			if LoadConfig.PostgresSwitch3 {
				load.PostgresSwitch3(db, id)
			}

			if LoadConfig.PostgresSwitch4 {
				load.PostgresSwitch4(db, id)
			}

		}
	}
}

func runMongoDB(ctx context.Context, id int) {

	mongo_ctx := context.Background()

	client, err := mongodb.ConnectByString(app.Config.MongoDB.ConnectionString, mongo_ctx)
	if err != nil {
		log.Printf("MongoDB: Connect Error: goroutine: %d: message: %s", id+1, err)
	}
	defer client.Disconnect(mongo_ctx)

	log.Printf("MongoDB: goroutine %d in progress", id+1)

	for {
		select {
		case <-ctx.Done():
			log.Printf("MongoDB: goroutine %d stopped", id+1)
			return
		default:

			if LoadConfig.MongoDBSwitch1 {
				load.MongoDBSwitch1(client, id)
			}

			if LoadConfig.MongoDBSwitch2 {
				load.MongoDBSwitch2(client, id)
			}

			if LoadConfig.MongoDBSwitch3 {
				load.MongoDBSwitch3(client, id)
			}

			if LoadConfig.MongoDBSwitch4 {
				load.MongoDBSwitch4(client, id)
			}

		}
	}

}

func checkOrWaitDB(db string) {
	for {

		result := checkConnection(db)

		if result {
			log.Printf("Check Or Wait DB: %s: Connected", db)
			break
		}
		if db == "MongoDB" {
			log.Printf("Wait DB Connect: %s: Connection failed: %s", db, app.Config.MongoDB.ConnectionStatus)
		} else if db == "MySQL" {
			log.Printf("Wait DB Connect: %s: Connection failed: %s", db, app.Config.MySQL.ConnectionStatus)
		} else if db == "Postgres" {
			log.Printf("Wait DB Connect: %s: Connection failed: %s", db, app.Config.Postgres.ConnectionStatus)
		}

		time.Sleep(5 * time.Second)
		app.InitConfig()
		checkConnectSettings()
	}
}

func checkConnection(db string) bool {

	var result string
	if db == "MongoDB" {
		app.Config.MongoDB.ConnectionStatus = mongodb.CheckMongoDB(app.Config.MongoDB.ConnectionString)
		result = app.Config.MongoDB.ConnectionStatus
	} else if db == "MySQL" {
		app.Config.MySQL.ConnectionStatus = mysql.CheckMySQL(app.Config.MySQL.ConnectionString)
		result = app.Config.MySQL.ConnectionStatus
	} else if db == "Postgres" {
		app.Config.Postgres.ConnectionStatus = postgres.CheckPostgreSQL(app.Config.Postgres.ConnectionString)
		result = app.Config.Postgres.ConnectionStatus
	}

	log.Printf("DB Connect: %s: Status: %s", db, result)

	if result == "Connected" {
		return true
	} else {
		return false
	}
}

func checkConnectSettings() {
	log.Printf("Check connection parameters to databases.")
	settings, err := valkey.LoadFromValkey("db_connections")
	if err != nil {
		log.Printf("Error: Valkey: Get connections to databases: %v", err)
	}

	if app.Config.LoadGenerator.MongoDB {
		if settings.MongoDBConnectionString != "" {
			app.Config.MongoDB.ConnectionString = settings.MongoDBConnectionString
		} else {
			app.Config.MongoDB.ConnectionString = mongodb.GetConnectionString(app.Config)
		}
	}

	if app.Config.LoadGenerator.MySQL {
		if settings.MySQLConnectionString != "" {
			app.Config.MySQL.ConnectionString = settings.MySQLConnectionString
		} else {
			app.Config.MySQL.ConnectionString = mysql.GetConnectionString(app.Config)
		}
	}

	if app.Config.LoadGenerator.Postgres {
		if settings.PostgresConnectionString != "" {
			app.Config.Postgres.ConnectionString = settings.PostgresConnectionString
		} else {
			app.Config.Postgres.ConnectionString = postgres.GetConnectionString(app.Config)
		}
	}

}

func checkControlPanelSettings() {

	loadSettingFromValkey, err := valkey.LoadControlPanelConfigFromValkey()
	if err != nil {
		log.Print("Valkey: Load Config: Empty")
	} else {
		LoadConfig = loadSettingFromValkey
		log.Printf("Valkey: Get Control Panel Settings: %v", LoadConfig)
	}

}
