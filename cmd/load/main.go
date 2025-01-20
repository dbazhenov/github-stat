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

// Environment variables for the application
var EnvVars app.EnvVars

// Structure to hold load information for databases
var databasesLoad DatabasesLoad

// DatabasesLoad holds the load information for different types of databases
type DatabasesLoad struct {
	MySQL    []map[string]string
	Postgres []map[string]string
	MongoDB  []map[string]string
}

// Structure to hold IDs of databases that need to stop loading
var stopLoadIDs StopLoadIDs

// StopLoadIDs holds the IDs of databases that need to stop loading for different types of databases
type StopLoadIDs struct {
	MySQL    []string
	Postgres []string
	MongoDB  []string
}

var databasesLoadMutex sync.Mutex
var stopLoadIDsMutex sync.Mutex

func main() {
	// Get the configuration from environment variables or .env file.
	app.InitConfig("load")

	// Valkey client initialization
	valkey.InitValkey(app.Config)
	defer valkey.Valkey.Close()

	// Retrieve the load configurations for the databases
	err := getLoadDatabases()
	if err != nil {
		log.Printf("Error: Getting databases: %v", err)
	}

	// Start managing load for each type of database based on configuration
	if app.Config.LoadGenerator.MySQL {
		log.Printf("Load MySQL: %v", databasesLoad.MySQL)
		go manageAllLoad("mysql")
	}

	if app.Config.LoadGenerator.MongoDB {
		log.Printf("Load MongoDB: %v", databasesLoad.MongoDB)
		go manageAllLoad("mongodb")
	}

	if app.Config.LoadGenerator.Postgres {
		log.Printf("Load Postgres: %v", databasesLoad.Postgres)
		go manageAllLoad("postgres")
	}

	// Continuously check and update the configuration from the control panel every 10 seconds
	for {
		time.Sleep(5 * time.Second)

		err := updateLoadDatabases()
		if err != nil {
			log.Printf("Error: Updating databases: %v", err)
		}

		log.Printf("Updated Load MySQL: %d, Postgres: %d, MongoDB: %d ... Stop Load: %v", len(databasesLoad.MySQL), len(databasesLoad.Postgres), len(databasesLoad.MongoDB), stopLoadIDs)
	}
}

// manageAllLoad manages the load for all databases of a specific type
func manageAllLoad(dbType string) {
	var wg sync.WaitGroup
	runningIDs := make(map[string]context.CancelFunc)

	for {
		var databases []map[string]string
		var stopIDs []string

		databasesLoadMutex.Lock()
		switch dbType {
		case "mysql":
			databases = deepCopy(databasesLoad.MySQL)
			stopIDs = stopLoadIDs.MySQL
		case "postgres":
			databases = deepCopy(databasesLoad.Postgres)
			stopIDs = stopLoadIDs.Postgres
		case "mongodb":
			databases = deepCopy(databasesLoad.MongoDB)
			stopIDs = stopLoadIDs.MongoDB
		}
		databasesLoadMutex.Unlock()

		for _, db := range databases {
			id := db["id"]

			if _, exists := runningIDs[id]; !exists {
				ctx, cancel := context.WithCancel(context.Background())
				runningIDs[id] = cancel
				wg.Add(1)
				go func(db map[string]string, ctx context.Context) {
					defer wg.Done()
					manageLoad(db, ctx)
				}(db, ctx)
				time.Sleep(10 * time.Millisecond)
				log.Printf("Started load for database %s", id)
			}
		}

		for _, stopID := range stopIDs {
			if cancel, exists := runningIDs[stopID]; exists {
				log.Printf("Ensuring cancellation of context for database %s", stopID)
				cancel()
				delete(runningIDs, stopID)
				log.Printf("Confirmed stopped load for database %s", stopID)
			}
		}

		log.Printf("manageAllLoad: %s, stopIDs: %v, RunningIDs: %v", dbType, stopIDs, runningIDs)

		time.Sleep(5 * time.Second)
	}
}

func deepCopy(original []map[string]string) []map[string]string {
	copy := make([]map[string]string, len(original))
	for i, v := range original {
		innerCopy := make(map[string]string)
		for key, value := range v {
			innerCopy[key] = value
		}
		copy[i] = innerCopy
	}
	return copy
}

// manageLoad manages the load for a single database
func manageLoad(dbConfig map[string]string, ctx context.Context) {
	dbType := dbConfig["dbType"]
	id := dbConfig["id"]

	routines := make(map[int]context.CancelFunc)

	db := checkOrWaitDB(id, dbType)
	if db == nil {
		log.Printf("Start: manageLoad: %s: %s: Database no longer exists", dbType, id)
		return
	}

	var wg sync.WaitGroup
	currentConnections, err := strconv.Atoi(db["connections"])
	if err != nil {
		log.Printf("%s: %s: Start: Error converting connections for database: %v", dbType, id, err)
		return
	}

	// Initial startup of Go routines
	for i := 0; i < currentConnections; i++ {
		wg.Add(1)
		rctx, rcancel := context.WithCancel(ctx)
		routines[i] = rcancel
		go func(connID int, rctx context.Context) {
			defer wg.Done()
			runDB(db, rctx, connID)
		}(i, rctx)
		time.Sleep(20 * time.Millisecond)
	}

	log.Printf("%s: %s: Manage Load: Start: Connections: %d routines started", dbType, id, len(routines))

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		default:
			// Get updated database configuration
			db = getDatabaseByID(id, dbType)

			if db == nil {
				log.Printf("%s: Database %s no longer exists, stopping all routines", dbType, id)
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
			checkStatus := checkConnection(db)

			if checkStatus != "Connected" {

				log.Printf("%s: %s: Detected disconnect, restarting routines. Connection status: %s", dbType, id, checkStatus)

				// Cancel all running goroutines
				for _, cancel := range routines {
					cancel()
				}

				// Wait for all routines to finish
				wg.Wait()

				// Clear routines map
				routines = make(map[int]context.CancelFunc)

				log.Printf("%s: %s: checkOrWaitDB: Start", dbType, id)
				// Reconnect and restart goroutines
				db := checkOrWaitDB(id, dbType)
				log.Printf("%s: %s: checkOrWaitDB: Finish", dbType, id)
				if db == nil {
					log.Printf("%s: %s: Database no longer exists after reconnection, stopping all routines", dbType, id)
					return
				} else {
					log.Printf("%s: %s: Database connection has been restored. Restarting %s routines. ", dbType, id, db["connections"])
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
					time.Sleep(20 * time.Millisecond)
				}
				log.Printf("%s: %s: Manage Load: %d routines in progress", dbType, id, len(routines))
			}

			// Manage changes in number of connections
			if currentConnections != newConnections {
				log.Printf("%s: %s: Manage Load: Change in the number of connections: %d -> %d ", dbType, id, currentConnections, newConnections)

				// Reduce the number of connections
				if newConnections < currentConnections {
					for i := newConnections; i < currentConnections; i++ {
						if rcancel, exists := routines[i]; exists {
							rcancel()
							delete(routines, i)
							log.Printf("%s: %s: Stopped routine %d", dbType, db["id"], i)
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
						log.Printf("%s: %s: Started routine %d", dbType, db["id"], i)
						time.Sleep(20 * time.Millisecond)
					}
				}

				log.Printf("%s: %s: Manage Load: %d routines in progress", dbType, id, len(routines))

				currentConnections = newConnections
			}

			time.Sleep(3 * time.Second)
		}
	}
}

// runDB runs the database operations for a specific connection
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

// runMySQL runs the MySQL database operations for a specific connection
func runMySQL(ctx context.Context, routineId int, dbConfig map[string]string) {
	connectionString := dbConfig["connectionString"]

	// Connect to the MySQL database
	db, err := mysql.ConnectByString(connectionString)
	if err != nil {
		log.Printf("MySQL: %s: Error: goroutine: %d: message: %s", dbConfig["id"], routineId, err)
		return
	}
	defer db.Close()

	log.Printf("MySQL: %s: goroutine %d in progress", dbConfig["id"], routineId)

	// Create an independent copy of dbConfig for use in the loop
	localDBConfig := make(map[string]string)
	for k, v := range dbConfig {
		localDBConfig[k] = v
	}

	// Variable to store the time of the last configuration update
	lastUpdate := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Printf("MySQL: %s goroutine: %d stopped", localDBConfig["id"], routineId)
			return
		default:
			// Update localDBConfig every 2 seconds
			if time.Since(lastUpdate) > 1*time.Second {
				localDBConfig = getDatabaseByID(localDBConfig["id"], "mysql")
				if localDBConfig == nil {
					log.Printf("MySQL: %s: goroutine: %d: database has been removed, stopping goroutine", dbConfig["id"], routineId)
					return
				}
				// log.Printf("MySQL: %s: goroutine: %d: Config: %s, %s, %s, %s", dbConfig["id"], routineId, localDBConfig["switch1"], localDBConfig["switch2"], localDBConfig["switch3"], localDBConfig["switch4"])
				lastUpdate = time.Now()
			}

			// Use localDBConfig for other operations
			if localDBConfig["switch1"] == "true" {

				load.MySQLSwitch1(db, routineId, localDBConfig)
			}

			if localDBConfig["switch2"] == "true" {

				load.MySQLSwitch2(db, routineId, localDBConfig)
			}

			if localDBConfig["switch3"] == "true" {

				load.MySQLSwitch3(db, routineId, localDBConfig)
			}

			if localDBConfig["switch4"] == "true" {

				load.MySQLSwitch4(db, routineId, localDBConfig)
			}

			// Check that sleep is not empty before attempting conversion
			if sleepDurationStr, ok := localDBConfig["sleep"]; ok && sleepDurationStr != "" {
				sleepDuration, err := strconv.Atoi(sleepDurationStr)
				if err == nil && sleepDuration > 0 {
					time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
				}
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

	// Variable to store the time of the last configuration update
	lastUpdate := time.Now()

	localDBConfig := make(map[string]string)
	for k, v := range dbConfig {
		localDBConfig[k] = v
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("Postgres: goroutine: %d stopped for %s", routineId, dbConfig["id"])
			return
		default:

			if time.Since(lastUpdate) > 1*time.Second {
				localDBConfig = getDatabaseByID(dbConfig["id"], "postgres")

				if localDBConfig == nil {
					log.Printf("Postgres: goroutine: %d: database %s has been removed, stopping goroutine", routineId, dbConfig["id"])
					return
				}

				lastUpdate = time.Now()
			}

			// Debug
			// newConnections, _ := strconv.Atoi(localDBConfig["connections"])
			// if newConnections > 0 {
			// 	log.Printf("Postgres: goroutine: %d: id: %s in progress. Switches: %s, %s, %s, %s, Sleep: %s", routineId+1, dbConfig["id"], localDBConfig["switch1"], localDBConfig["switch2"], localDBConfig["switch3"], localDBConfig["switch4"], localDBConfig["sleep"])
			// }

			if localDBConfig["switch1"] == "true" {
				load.PostgresSwitch1(db, routineId, localDBConfig)
			}

			if localDBConfig["switch2"] == "true" {
				load.PostgresSwitch2(db, routineId, localDBConfig)
			}

			if localDBConfig["switch3"] == "true" {
				load.PostgresSwitch3(db, routineId, localDBConfig)
			}

			if localDBConfig["switch4"] == "true" {
				load.PostgresSwitch4(db, routineId, localDBConfig)
			}

			sleepDuration, err := strconv.Atoi(localDBConfig["sleep"])
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

	// Variable to store the time of the last configuration update
	lastUpdate := time.Now()

	localDBConfig := make(map[string]string)
	for k, v := range dbConfig {
		localDBConfig[k] = v
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("MongoDB: goroutine: %d stopped for %s", routineId, dbConfig["id"])
			return
		default:

			if time.Since(lastUpdate) > 1*time.Second {
				localDBConfig = getDatabaseByID(dbConfig["id"], "mongodb")

				if localDBConfig == nil {
					log.Printf("MongoDB: goroutine: %d: database %s has been removed, stopping goroutine", routineId, dbConfig["id"])
					return
				}

				lastUpdate = time.Now()
			}
			// log.Printf("MongoDB: goroutine: %d: id: %s in progress. Switches: %s, %s, %s, %s, Sleep: %s", routineId+1, dbConfig["id"], updatedDBConfig["switch1"], updatedDBConfig["switch2"], updatedDBConfig["switch3"], updatedDBConfig["switch4"], updatedDBConfig["sleep"])

			if localDBConfig["switch1"] == "true" {
				load.MongoDBSwitch1(client, db, routineId, localDBConfig)
			}

			if localDBConfig["switch2"] == "true" {
				load.MongoDBSwitch2(client, db, routineId, localDBConfig)
			}

			if localDBConfig["switch3"] == "true" {
				load.MongoDBSwitch3(client, db, routineId, localDBConfig)
			}

			if localDBConfig["switch4"] == "true" {
				load.MongoDBSwitch4(client, db, routineId, localDBConfig)
			}

			sleepDuration, err := strconv.Atoi(localDBConfig["sleep"])
			if err == nil && sleepDuration > 0 {
				time.Sleep(time.Duration(sleepDuration) * time.Millisecond)
			}
		}
	}
}

// updateLoadDatabases updates the load configuration for the databases.
func updateLoadDatabases() error {
	// Retrieve databases configuration from Valkey
	databases, err := valkey.GetDatabases()
	if err != nil {
		return err
	}

	// Sort databases by their position
	sort.Slice(databases, func(i, j int) bool {
		pos1, _ := strconv.Atoi(databases[i]["position"])
		pos2, _ := strconv.Atoi(databases[j]["position"])
		return pos1 < pos2
	})

	// Initialize a new DatabasesLoad structure
	newDatabasesLoad := DatabasesLoad{}

	// Apply a single lock for reading stopLoadIDs and databasesLoad
	stopLoadIDsMutex.Lock()
	databasesLoadMutex.Lock()
	defer stopLoadIDsMutex.Unlock()
	defer databasesLoadMutex.Unlock()

	// Iterate over each database configuration
	for _, db := range databases {
		if db["loadSwitch"] == "true" {
			// Add databases to the respective type in newDatabasesLoad
			switch db["dbType"] {
			case "mysql":
				newDatabasesLoad.MySQL = append(newDatabasesLoad.MySQL, db)
			case "postgres":
				newDatabasesLoad.Postgres = append(newDatabasesLoad.Postgres, db)
			case "mongodb":
				newDatabasesLoad.MongoDB = append(newDatabasesLoad.MongoDB, db)
			}

			// Remove IDs from stopLoadIDs if they are still in load
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

	// Update the stopLoadIDs for databases that are no longer in load
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

	// Update the global databasesLoad with the new configuration
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

func checkOrWaitDB(id string, dbType string) map[string]string {

	for {
		db := getDatabaseByID(id, dbType)

		if db == nil {
			log.Printf("checkOrWaitDB: %s: %s: Database no longer exists after reconnection, stopping all routines", dbType, id)
			return nil
		}

		checkStatus := checkConnection(db)

		if checkStatus == "Connected" {
			db["connectionStatus"] = checkStatus
			log.Printf("checkOrWaitDB: %s: %s: Status: Connected", dbType, id)
			return db
		}

		log.Printf("checkOrWaitDB: %s: %s: Connection failed: %s", dbType, id, checkStatus)

		time.Sleep(5 * time.Second)
	}

}

// getDatabaseByID retrieves the database configuration by its ID.
func getDatabaseByID(id string, dbType string) map[string]string {
	var databasesForProcess []map[string]string

	// Lock the mutex only for the time needed to copy the reference to the data
	databasesLoadMutex.Lock()
	switch dbType {
	case "mysql":
		databasesForProcess = databasesLoad.MySQL
	case "postgres":
		databasesForProcess = databasesLoad.Postgres
	case "mongodb":
		databasesForProcess = databasesLoad.MongoDB
	}
	databasesLoadMutex.Unlock()

	// Search for the database ID outside of the mutex lock
	for _, db := range databasesForProcess {
		if db["id"] == id {
			return db
		}
	}

	return nil
}

func checkConnection(db map[string]string) string {
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

	// updateConnectionStatus(dbType, db["id"], result)

	return result
}

// // updateConnectionStatus updates the connection status of a specific database.
// func updateConnectionStatus(dbType, id, status string) {
// 	var databasesForProcess []map[string]string

// 	// Lock the mutex only for the time needed to copy the reference to the data
// 	databasesLoadMutex.Lock()
// 	switch dbType {
// 	case "mysql":
// 		databasesForProcess = databasesLoad.MySQL
// 	case "postgres":
// 		databasesForProcess = databasesLoad.Postgres
// 	case "mongodb":
// 		databasesForProcess = databasesLoad.MongoDB
// 	}
// 	databasesLoadMutex.Unlock()

// 	// Update the connection status outside of the mutex lock
// 	for i, db := range databasesForProcess {
// 		if db["id"] == id {
// 			databasesLoadMutex.Lock()
// 			switch dbType {
// 			case "mysql":
// 				databasesLoad.MySQL[i]["connectionStatus"] = status
// 			case "postgres":
// 				databasesLoad.Postgres[i]["connectionStatus"] = status
// 			case "mongodb":
// 				databasesLoad.MongoDB[i]["connectionStatus"] = status
// 			}
// 			databasesLoadMutex.Unlock()
// 			break
// 		}
// 	}
// }
