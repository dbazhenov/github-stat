package main

import (
	"context"
	app "github-stat/internal"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"github-stat/internal/databases/valkey"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github-stat/internal/load"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var EnvVars app.EnvVars

var databasesLoad DatabasesLoad
var stopLoadIDs StopLoadIDs

type DatabasesLoad struct {
	MySQL    []map[string]string
	Postgres []map[string]string
	MongoDB  []map[string]string
}

type StopLoadIDs struct {
	MySQL    []string
	Postgres []string
	MongoDB  []string
}

func main() {

	// Get the configuration from environment variables or .env file.
	app.InitConfig()

	// Valkey client initialization
	valkey.InitValkey(app.Config)
	defer valkey.Valkey.Close()

	err := getLoadDatabases()
	if err != nil {
		log.Printf("Error: Getting databases: %v", err)
	}

	log.Printf("Load MySQL: %v", databasesLoad.MySQL)
	log.Printf("Load Postgres: %v", databasesLoad.Postgres)
	log.Printf("Load MongoDB: %v", databasesLoad.MongoDB)

	if app.Config.LoadGenerator.MySQL {
		go manageAllLoad("mysql")
	}

	if app.Config.LoadGenerator.MongoDB {
		go manageAllLoad("mongodb")
	}

	if app.Config.LoadGenerator.Postgres {
		go manageAllLoad("postgres")
	}

	// Continuously check and update the configuration from the control panel every 10 seconds
	for {
		time.Sleep(5 * time.Second)

		err := updateLoadDatabases()
		if err != nil {
			log.Printf("Error: Updating databases: %v", err)
		}

		log.Printf("Updated Load MySQL: %d, Postgres: %d, MongoDB: %d", len(databasesLoad.MySQL), len(databasesLoad.Postgres), len(databasesLoad.MongoDB))
		log.Printf("Stop Load: %v", stopLoadIDs)
	}
}

func manageAllLoad(dbType string) {
	var wg sync.WaitGroup
	runningIDs := make(map[string]context.CancelFunc)

	for {
		var databases []map[string]string
		var stopIDs []string

		// Retrieve the latest data from global variables based on the database type
		switch dbType {
		case "mysql":
			databases = databasesLoad.MySQL
			stopIDs = stopLoadIDs.MySQL
		case "postgres":
			databases = databasesLoad.Postgres
			stopIDs = stopLoadIDs.Postgres
		case "mongodb":
			databases = databasesLoad.MongoDB
			stopIDs = stopLoadIDs.MongoDB
		}

		log.Printf("manageAllLoad: %s, stopIDs: %v", dbType, stopIDs)
		log.Printf("RunningIDS: %v", runningIDs)

		// Iterate over the databases and manage goroutines
		for _, db := range databases {
			id := db["id"]

			// Start new goroutines if the database ID is not in the runningIDs map
			if _, exists := runningIDs[id]; !exists {
				ctx, cancel := context.WithCancel(context.Background())
				runningIDs[id] = cancel
				wg.Add(1)
				go func(db map[string]string, ctx context.Context) {
					defer wg.Done()
					manageLoad(db, ctx)
				}(db, ctx)
				log.Printf("Started load for database %s", id)
			}
		}

		// Ensure cancellation of contexts for any remaining IDs in stopIDs
		for _, stopID := range stopIDs {
			if cancel, exists := runningIDs[stopID]; exists {
				log.Printf("Ensuring cancellation of context for database %s", stopID)
				cancel()
				delete(runningIDs, stopID)
				log.Printf("Confirmed stopped load for database %s", stopID)
			}
		}

		// Sleep for 3 seconds before the next iteration
		time.Sleep(3 * time.Second)
	}
}

// func testManageLoad(db map[string]string, ctx context.Context) {
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			log.Printf("Context done for newManageLoad: %v", db["id"])
// 			log.Printf("Stopped newManageLoad for: %v", db["id"])
// 			return
// 		default:
// 			log.Printf("newManageLoad for: %v", db["id"])
// 			time.Sleep(3 * time.Second)
// 		}
// 	}
// }

func manageLoad(db map[string]string, ctx context.Context) {
	dbType := db["dbType"]
	id := db["id"]

	checkOrWaitDB(id, dbType)

	db = getDatabaseByID(id, dbType)

	var wg sync.WaitGroup
	currentConnections, err := strconv.Atoi(db["connections"])
	if err != nil {
		log.Printf("Start: Error converting connections for database %s: %v", id, err)
		return
	}

	routines := make(map[int]context.CancelFunc)

	// Initial startup of Go routines
	for i := 0; i < currentConnections; i++ {
		wg.Add(1)
		rctx, rcancel := context.WithCancel(ctx)
		routines[i] = rcancel
		go func(connID int, rctx context.Context) {
			defer wg.Done()
			runDB(db, rctx, connID)
		}(i, rctx)
	}

	log.Printf("Manage Load: Start %s: %d routines in progress for database %s", dbType, len(routines), id)

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		default:

			db = getDatabaseByID(id, dbType)
			if db == nil {
				log.Printf("Database %s no longer exists, stopping all routines", id)
				for _, cancel := range routines {
					cancel()
				}
				wg.Wait()
				return
			}

			newConnections, err := strconv.Atoi(db["connections"])
			if err != nil {
				log.Printf("Update: Error converting connections for database %s: %v", id, err)
				continue
			}

			// Check DB connection status
			if !checkConnection(db) {
				log.Printf("%s: Detected disconnect, restarting routines.", dbType)

				// Cancel all running goroutines
				for _, cancel := range routines {
					cancel()
				}

				// Wait for all routines to finish
				wg.Wait()

				// Clear routines map
				routines = make(map[int]context.CancelFunc)

				// Reconnect and restart goroutines
				checkOrWaitDB(id, dbType)

				db = getDatabaseByID(id, dbType)
				if db == nil {
					log.Printf("Database %s no longer exists after reconnection, stopping all routines", id)
					return
				}

				newConnections, err = strconv.Atoi(db["connections"])
				if err != nil {
					log.Printf("Reconnect: Error converting connections for database %s: %v", id, err)
					return
				}

				for i := 0; i < newConnections; i++ {
					wg.Add(1)
					rctx, rcancel := context.WithCancel(ctx)
					routines[i] = rcancel
					go func(connID int, rctx context.Context) {
						defer wg.Done()
						runDB(db, rctx, connID)
					}(i, rctx)
				}
				log.Printf("Manage Load: %s: %d routines in progress for database %s", dbType, len(routines), id)
			}

			// Manage changes in number of connections
			if currentConnections != newConnections {
				log.Printf("%s: Manage: Change in the number of connections: %d -> %d for database %s", dbType, currentConnections, newConnections, id)

				// Reduce the number of connections
				if newConnections < currentConnections {
					for i := newConnections; i < currentConnections; i++ {
						if rcancel, exists := routines[i]; exists {
							rcancel()
							delete(routines, i)
							log.Printf("Stopped routine %d for database %s", i, db["id"])
						}
					}
				}

				// Increase the number of connections
				if newConnections > currentConnections {
					for i := currentConnections; i < newConnections; i++ {
						wg.Add(1)
						rctx, rcancel := context.WithCancel(ctx)
						routines[i] = rcancel
						go func(connID int, rctx context.Context) {
							defer wg.Done()
							runDB(db, rctx, connID)
						}(i, rctx)
					}
				}

				log.Printf("Manage Load: %s: %d routines in progress for database %s", dbType, len(routines), id)

				currentConnections = newConnections
			}

			time.Sleep(3 * time.Second)
		}
	}
}

func runDB(db map[string]string, ctx context.Context, id int) {
	dbType := db["dbType"]

	switch dbType {
	case "mongodb":
		runMongoDB(ctx, id, db)
	case "mysql":
		runMySQL(ctx, id, db)
	case "postgres":
		runPostgreSQL(ctx, id, db)
	default:
		log.Printf("Unknown database type %s for ID %d", dbType, id)
	}
}

func runMySQL(ctx context.Context, routineId int, dbConfig map[string]string) {
	connectionString := dbConfig["connectionString"]

	db, err := mysql.ConnectByString(connectionString)
	if err != nil {
		log.Printf("MySQL: Error: goroutine: %d: %s: message: %s", routineId+1, dbConfig["id"], err)
		return
	}
	defer db.Close()

	log.Printf("MySQL: goroutine %d in progress for %s", routineId+1, dbConfig["id"])

	for {
		select {
		case <-ctx.Done():
			log.Printf("MySQL: goroutine: %d stopped for %s", routineId+1, dbConfig["id"])
			return
		default:

			updatedDBConfig := getDatabaseByID(dbConfig["id"], "mysql")

			if updatedDBConfig == nil {
				log.Printf("MySQL: goroutine: %d: database %s has been removed, stopping goroutine", routineId+1, dbConfig["id"])
				return
			}

			log.Printf("MySQL: goroutine: %d: id: %s in progress. Switches: %s, %s, %s, %s, Sleep: %s", routineId+1, dbConfig["id"], updatedDBConfig["switch1"], updatedDBConfig["switch2"], updatedDBConfig["switch3"], updatedDBConfig["switch4"], updatedDBConfig["sleep"])

			if updatedDBConfig["switch1"] == "true" {
				load.MySQLSwitch1(db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch2"] == "true" {
				load.MySQLSwitch2(db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch3"] == "true" {
				load.MySQLSwitch3(db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch4"] == "true" {
				load.MySQLSwitch4(db, routineId, updatedDBConfig)
			}

			sleepDuration, err := strconv.Atoi(updatedDBConfig["sleep"])
			if err == nil && sleepDuration > 0 {
				time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
			}
		}
	}
}

func runPostgreSQL(ctx context.Context, routineId int, dbConfig map[string]string) {
	connectionString := dbConfig["connectionString"]

	db, err := postgres.ConnectByString(connectionString)
	if err != nil {
		log.Printf("Postgres: Error: goroutine: %d: %s: message: %s", routineId+1, dbConfig["id"], err)
		return
	}
	defer db.Close()

	log.Printf("Postgres: goroutine %d in progress for %s", routineId+1, dbConfig["id"])

	for {
		select {
		case <-ctx.Done():
			log.Printf("Postgres: goroutine: %d stopped for %s", routineId+1, dbConfig["id"])
			return
		default:
			updatedDBConfig := getDatabaseByID(dbConfig["id"], "postgres")

			if updatedDBConfig == nil {
				log.Printf("Postgres: goroutine: %d: database %s has been removed, stopping goroutine", routineId+1, dbConfig["id"])
				return
			}

			log.Printf("Postgres: goroutine: %d: id: %s in progress. Switches: %s, %s, %s, %s, Sleep: %s", routineId+1, dbConfig["id"], updatedDBConfig["switch1"], updatedDBConfig["switch2"], updatedDBConfig["switch3"], updatedDBConfig["switch4"], updatedDBConfig["sleep"])

			if updatedDBConfig["switch1"] == "true" {
				load.PostgresSwitch1(db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch2"] == "true" {
				load.PostgresSwitch2(db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch3"] == "true" {
				load.PostgresSwitch3(db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch4"] == "true" {
				load.PostgresSwitch4(db, routineId, updatedDBConfig)
			}

			sleepDuration, err := strconv.Atoi(updatedDBConfig["sleep"])
			if err == nil && sleepDuration > 0 {
				time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
			}
		}
	}
}

func runMongoDB(ctx context.Context, routineId int, dbConfig map[string]string) {
	connectionString := dbConfig["connectionString"]

	mongo_ctx := context.Background()

	client, err := mongodb.ConnectByString(connectionString, mongo_ctx)
	if err != nil {
		log.Printf("MongoDB: Connect Error: goroutine: %d: %s: message: %s", routineId+1, dbConfig["id"], err)
		return
	}
	defer client.Disconnect(mongo_ctx)

	db := dbConfig["database"]
	log.Printf("MongoDB: goroutine %d in progress for %s", routineId+1, dbConfig["id"])

	for {
		select {
		case <-ctx.Done():
			log.Printf("MongoDB: goroutine: %d stopped for %s", routineId+1, dbConfig["id"])
			return
		default:
			updatedDBConfig := getDatabaseByID(dbConfig["id"], "mongodb")

			if updatedDBConfig == nil {
				log.Printf("MongoDB: goroutine: %d: database %s has been removed, stopping goroutine", routineId+1, dbConfig["id"])
				return
			}

			log.Printf("MongoDB: goroutine: %d: id: %s in progress. Switches: %s, %s, %s, %s, Sleep: %s", routineId+1, dbConfig["id"], updatedDBConfig["switch1"], updatedDBConfig["switch2"], updatedDBConfig["switch3"], updatedDBConfig["switch4"], updatedDBConfig["sleep"])

			if updatedDBConfig["switch1"] == "true" {
				load.MongoDBSwitch1(client, db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch2"] == "true" {
				load.MongoDBSwitch2(client, db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch3"] == "true" {
				load.MongoDBSwitch3(client, db, routineId, updatedDBConfig)
			}

			if updatedDBConfig["switch4"] == "true" {
				load.MongoDBSwitch4(client, db, routineId, updatedDBConfig)
			}

			sleepDuration, err := strconv.Atoi(updatedDBConfig["sleep"])
			if err == nil && sleepDuration > 0 {
				time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
			}
		}
	}
}

func updateLoadDatabases() error {
	databases, err := valkey.GetDatabases()
	if err != nil {
		return err
	}

	sort.Slice(databases, func(i, j int) bool {
		pos1, _ := strconv.Atoi(databases[i]["position"])
		pos2, _ := strconv.Atoi(databases[j]["position"])
		return pos1 < pos2
	})

	newDatabasesLoad := DatabasesLoad{}

	for _, db := range databases {
		if db["loadSwitch"] == "true" {
			switch db["dbType"] {
			case "mysql":
				newDatabasesLoad.MySQL = append(newDatabasesLoad.MySQL, db)
			case "postgres":
				newDatabasesLoad.Postgres = append(newDatabasesLoad.Postgres, db)
			case "mongodb":
				newDatabasesLoad.MongoDB = append(newDatabasesLoad.MongoDB, db)
			}

			switch db["dbType"] {
			case "mysql":
				stopLoadIDs.MySQL = removeIDFromList(stopLoadIDs.MySQL, db["id"])
			case "postgres":
				stopLoadIDs.Postgres = removeIDFromList(stopLoadIDs.Postgres, db["id"])
			case "mongodb":
				stopLoadIDs.MongoDB = removeIDFromList(stopLoadIDs.MongoDB, db["id"])
			}
		}
	}

	stopLoadIDs = StopLoadIDs{}
	for _, oldDB := range append(databasesLoad.MySQL, append(databasesLoad.Postgres, databasesLoad.MongoDB...)...) {
		found := false
		for _, newDB := range append(newDatabasesLoad.MySQL, append(newDatabasesLoad.Postgres, newDatabasesLoad.MongoDB...)...) {
			if oldDB["id"] == newDB["id"] {
				found = true
				break
			}
		}
		if !found {
			switch oldDB["dbType"] {
			case "mysql":
				stopLoadIDs.MySQL = append(stopLoadIDs.MySQL, oldDB["id"])
			case "postgres":
				stopLoadIDs.Postgres = append(stopLoadIDs.Postgres, oldDB["id"])
			case "mongodb":
				stopLoadIDs.MongoDB = append(stopLoadIDs.MongoDB, oldDB["id"])
			}
		}
	}

	databasesLoad = newDatabasesLoad

	return nil
}

func removeIDFromList(list []string, id string) []string {
	newList := []string{}
	for _, item := range list {
		if item != id {
			newList = append(newList, item)
		}
	}
	return newList
}

func getLoadDatabases() error {
	databases, err := valkey.GetDatabases()
	if err != nil {
		return err
	}

	sort.Slice(databases, func(i, j int) bool {
		pos1, _ := strconv.Atoi(databases[i]["position"])
		pos2, _ := strconv.Atoi(databases[j]["position"])
		return pos1 < pos2
	})

	databasesLoad = DatabasesLoad{}

	for _, db := range databases {
		if db["loadSwitch"] == "true" {
			switch db["dbType"] {
			case "mysql":
				databasesLoad.MySQL = append(databasesLoad.MySQL, db)
			case "postgres":
				databasesLoad.Postgres = append(databasesLoad.Postgres, db)
			case "mongodb":
				databasesLoad.MongoDB = append(databasesLoad.MongoDB, db)
			}
		}
	}

	return nil
}

func checkOrWaitDB(id string, dbType string) {
	for {
		db := getDatabaseByID(id, dbType)

		log.Printf("checkOrWaitDB: %s: %s: %s, %s", dbType, id, db["connectionStatus"], db["connectionString"])

		connectionStatus := db["connectionStatus"]

		if checkConnection(db) {
			log.Printf("Check Or Wait DB: %s: Connected", dbType)
			break
		}

		log.Printf("Wait DB Connect: %s: Connection failed: %s", dbType, connectionStatus)
		time.Sleep(5 * time.Second)
	}
}

func getDatabaseByID(id string, dbType string) map[string]string {
	switch dbType {
	case "mysql":
		for _, db := range databasesLoad.MySQL {
			if db["id"] == id {
				return db
			}
		}
	case "postgres":
		for _, db := range databasesLoad.Postgres {
			if db["id"] == id {
				return db
			}
		}
	case "mongodb":
		for _, db := range databasesLoad.MongoDB {
			if db["id"] == id {
				return db
			}
		}
	}
	return nil
}

func checkConnection(db map[string]string) bool {
	connectionString := db["connectionString"]
	dbType := db["dbType"]

	var result string
	switch dbType {
	case "mysql":
		result = mysql.CheckMySQL(connectionString)
	case "postgres":
		result = postgres.CheckPostgreSQL(connectionString)
	case "mongodb":
		result = mongodb.CheckMongoDB(connectionString)
	}

	updateConnectionStatus(dbType, db["id"], result)

	return result == "Connected"
}

func updateConnectionStatus(dbType, id, status string) {
	switch dbType {
	case "mysql":
		for i, db := range databasesLoad.MySQL {
			if db["id"] == id {
				databasesLoad.MySQL[i]["connectionStatus"] = status
				break
			}
		}
	case "postgres":
		for i, db := range databasesLoad.Postgres {
			if db["id"] == id {
				databasesLoad.Postgres[i]["connectionStatus"] = status
				break
			}
		}
	case "mongodb":
		for i, db := range databasesLoad.MongoDB {
			if db["id"] == id {
				databasesLoad.MongoDB[i]["connectionStatus"] = status
				break
			}
		}
	}
}
