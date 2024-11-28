package valkey

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/go-redis/redis"

	app "github-stat/internal"
)

var Valkey *redis.Client

// InitValkey initializes the Redis client using the provided environment variables.
// It connects to the Redis server and verifies the connection.
//
// Arguments:
//   - envVars: app.EnvVars containing the environment variables for the Valkey configuration.
func InitValkey(envVars app.EnvVars) {
	addr := fmt.Sprintf("%s:%s", envVars.Valkey.Addr, envVars.Valkey.Port)
	Valkey = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: envVars.Valkey.Password,
		DB:       envVars.Valkey.DB,
	})

	// Verify the connection to the Redis server.
	_, err := Valkey.Ping().Result()
	if err != nil {
		log.Printf("Valkey: Failed to connect to: %v", err)
	} else {
		log.Printf("Valkey: Connect: %v", Valkey.Options().Addr)
	}
}

// GetMaxID retrieves the maximum ID for the specified database type from Redis.
//
// Arguments:
//   - dbType: string containing the type of the database (e.g., "mysql", "postgres", "mongodb").
//
// Returns:
//   - int64: The maximum ID found for the specified database type.
//   - error: An error object if an error occurs, otherwise nil.
func GetMaxID(dbType string) (int64, error) {
	keys, err := Valkey.Keys(fmt.Sprintf("databases:%s-*", dbType)).Result()
	if err != nil {
		return 0, err
	}

	var maxID int64
	for _, key := range keys {
		parts := strings.Split(key, "-")
		if len(parts) < 2 {
			continue
		}
		idNum, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		if idNum > maxID {
			maxID = idNum
		}
	}

	return maxID, nil
}

// GetDatabases retrieves the details of all databases from Redis.
//
// Returns:
//   - []map[string]string: A slice of maps, each containing the details of a database.
//   - error: An error object if an error occurs, otherwise nil.
func GetDatabases() ([]map[string]string, error) {
	keys, err := Valkey.Keys("databases:*").Result()
	if err != nil {
		return nil, err
	}

	var databases []map[string]string
	for _, key := range keys {
		fields, err := Valkey.HGetAll(key).Result()
		if err != nil {
			return nil, err
		}
		databases = append(databases, fields)
	}

	return databases, nil
}

// GetDatabase retrieves the details of a specified database from Redis.
//
// Arguments:
//   - id: string containing the ID of the database.
//
// Returns:
//   - map[string]string: A map containing the database details.
//   - error: An error object if an error occurs, otherwise nil.
func GetDatabase(id string) (map[string]string, error) {
	key := "databases:" + id
	fields, err := Valkey.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("database with id %s not found", id)
	}
	return fields, nil
}

// AddDatabase adds or updates a database in Redis with the specified ID and fields.
//
// Arguments:
//   - id: string containing the ID of the database.
//   - fields: map[string]string containing the fields and values to set for the database.
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func AddDatabase(id string, fields map[string]string) error {
	key := fmt.Sprintf("databases:%s", id)

	data := make(map[string]interface{})
	for k, v := range fields {
		data[k] = v
	}

	_, err := Valkey.HMSet(key, data).Result()
	if err != nil {
		return err
	}

	return nil
}

// DeleteDatabase deletes a database from Redis with the specified ID.
//
// Arguments:
//   - id: string containing the ID of the database.
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func DeleteDatabase(id string) error {
	key := fmt.Sprintf("databases:%s", id)

	res, err := Valkey.Del(key).Result()

	log.Printf("Valkey: Delete DB: %s: Result: %v", id, res)

	if err != nil {
		log.Printf("Valkey: Delete DB: %s: Error: %v", id, err)
		return err
	}

	return nil
}

// SaveReport saves a report to Redis with the specified report ID and report data.
//
// Arguments:
//   - reportID: string containing the ID of the report.
//   - reportMap: map[string]interface{} containing the report data.
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func SaveReport(reportID string, reportMap map[string]interface{}) error {
	// Form the key for the report.
	key := fmt.Sprintf("reports_runs:%s", reportID)

	// Convert the report map to a format suitable for Redis.
	data := make(map[string]interface{})
	for k, v := range reportMap {
		data[k] = v
	}

	// Save the data to Redis.
	_, err := Valkey.HMSet(key, data).Result()
	if err != nil {
		log.Printf("Redis: Error: message: %s", err)
		return err
	}

	log.Printf("Report successfully saved with ID: %s", reportID)
	return nil
}

// SaveDatasetLoader saves the dataset loader data to Valkey.
func SaveDatasetLoader(data map[string]interface{}) error {
	// Marshal the data into JSON format
	dataJSON, err := json.Marshal(data)
	if err != nil {
		log.Printf("Error marshaling dataset loader data: %v", err)
		return err
	}

	// Save the JSON string in Valkey under the key "DatasetLoader"
	cmd := Valkey.Set("DatasetLoader", dataJSON, 0)
	if err := cmd.Err(); err != nil {
		log.Printf("Error saving dataset loader data in Valkey: %v", err)
		return err
	}

	return nil
}

// GetDatasetState retrieves the dataset loader data from Valkey and returns it as a map.
func GetDatasetState() map[string]interface{} {
	// Get the data from Valkey
	dataJSON, err := Valkey.Get("DatasetLoader").Result()
	if err != nil {
		log.Printf("Error getting dataset loader data from Valkey: %v", err)
		return nil
	}

	// If no data is available, return an empty map
	if dataJSON == "" {
		return nil
	}

	// Unmarshal the JSON data into a map
	var datasetState map[string]interface{}
	err = json.Unmarshal([]byte(dataJSON), &datasetState)
	if err != nil {
		log.Printf("Error unmarshaling dataset loader data: %v", err)
		return nil
	}

	return datasetState
}

// GetLatestDatasetReport retrieves the latest dataset report from Valkey and returns it as a map.
func GetLatestDatasetReport() (map[string]interface{}, error) {
	// Get all keys that match the pattern "reports_runs:*"
	keys, err := Valkey.Keys("reports_runs:*").Result()
	if err != nil {
		log.Printf("Error getting report keys from Valkey: %v", err)
		return nil, err
	}

	// Sort the keys in descending order by date
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] > keys[j]
	})

	// Get the latest report
	if len(keys) > 0 {
		latestKey := keys[0]
		data, err := Valkey.HGetAll(latestKey).Result()
		if err != nil {
			log.Printf("Error getting latest report data from Valkey: %v", err)
			return nil, err
		}

		// Convert the data into a map
		report := make(map[string]interface{})
		for k, v := range data {
			report[k] = v
		}

		return report, nil
	}

	return nil, nil
}
