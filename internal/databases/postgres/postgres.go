package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/go-github/github"
	_ "github.com/lib/pq"
)

func ConnectByString(connection_string string) (*sql.DB, error) {

	db, err := sql.Open("postgres", connection_string)
	if err != nil {
		log.Printf("PostgreSQL Connect: Error: %s", err)
		return nil, err
	}

	return db, nil
}

func CheckPostgreSQL(connectionString string) string {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return fmt.Sprintf("Error opening connection: %v", err)
	}
	defer db.Close()

	// Using a context with a timeout for Ping
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errChan := make(chan error, 1)

	go func() {
		errChan <- db.PingContext(ctx)
	}()

	select {
	case err := <-errChan:
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				return "Error: Connection timeout exceeded"
			}
			return fmt.Sprintf("Error checking connection: %v", err)
		}
	case <-ctx.Done():
		return "Error: Connection timeout exceeded"
	}

	return "Connected"
}

func SelectInt(db *sql.DB, query string) (int, error) {
	var integer int
	err := db.QueryRow(query).Scan(&integer)
	if err != nil {
		return 0, err
	}
	return integer, nil
}

func SelectString(db *sql.DB, query string) (string, error) {
	var repo string
	err := db.QueryRow(query).Scan(&repo)
	if err != nil {
		return "", err
	}
	return repo, nil
}

func SelectListOfStrings(db *sql.DB, query string) ([]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var str string
		if err := rows.Scan(&str); err != nil {
			return nil, err
		}
		results = append(results, str)
	}
	return results, nil
}

func SelectListOfInt(db *sql.DB, query string) ([]int, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []int
	for rows.Next() {
		var integer int
		if err := rows.Scan(&integer); err != nil {
			return nil, err
		}
		results = append(results, integer)
	}
	return results, nil
}

func SelectPulls(db *sql.DB, query string) ([]*github.PullRequest, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pullRequests []*github.PullRequest
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}

		var pr github.PullRequest
		if err := json.Unmarshal([]byte(data), &pr); err != nil {
			return nil, err
		}

		pullRequests = append(pullRequests, &pr)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pullRequests, nil
}

func CreateTable(db *sql.DB, name string) error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS github.%s (
		id INT NOT NULL,
		repo VARCHAR(255) NOT NULL,
		data JSON,
		PRIMARY KEY (id, repo)
	)`, name)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func DropTable(db *sql.DB, name string) (string, error) {
	query := fmt.Sprintf("DROP TABLE IF EXISTS github.%s", name)
	_, err := db.Exec(query)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("Table '%s' dropped successfully or did not exist.", name), nil
}

func databaseExists(db *sql.DB, dbName string) (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT EXISTS (SELECT datname FROM pg_database WHERE datname = '%s');", dbName)
	err := db.QueryRow(query).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func executeSQL(db *sql.DB, query string) error {
	const maxRetries = 10
	for i := 0; i < maxRetries; i++ {
		_, err := db.Exec(query)
		if err == nil {
			return nil
		}
		log.Printf("Error executing SQL (attempt %d/%d): %v", i+1, maxRetries, err)
		if strings.Contains(err.Error(), "server login has been failing") {
			time.Sleep(5 * time.Second)
			continue
		}
		return err
	}
	return fmt.Errorf("reached maximum retry limit for query: %s", query)
}

func InitSchema(connection_string string) error {

	newDBName, err := getDbName(connection_string)
	if err != nil {
		log.Printf("getDbName: Error: %v\n", err)
		return err
	} else {
		log.Printf("getDbName: New Database name: %s", newDBName)
	}

	connect_postgres_db_string := strings.Replace(connection_string, fmt.Sprintf("dbname=%s", newDBName), "dbname=postgres", 1)

	log.Printf("InitDB: Connect String to Postgres DB: %s", connect_postgres_db_string)

	mainDB, err := ConnectByString(connect_postgres_db_string)
	if err != nil {
		log.Printf("InitDB: Connect to Main DB: Error: %s", newDBName)
	}
	defer mainDB.Close()

	dbExists, err := databaseExists(mainDB, newDBName)
	if err != nil {
		log.Printf("Error checking if database exists: %v", err)
	}

	if !dbExists {
		log.Printf("Database %s does not exist, creating database", newDBName)
		err := executeSQL(mainDB, fmt.Sprintf("CREATE DATABASE %s;", newDBName))
		if err != nil {
			log.Printf("Error creating database %s: %v", newDBName, err)
		}
	}

	newDB, err := ConnectByString(connection_string)
	if err != nil {
		log.Printf("InitDB: Connect to New DB: Error: %s", newDBName)
	}
	defer newDB.Close()

	log.Printf("Creating schemas and tables in database %s", newDBName)
	schemaSQL := `
        CREATE EXTENSION IF NOT EXISTS pg_stat_monitor;
        CREATE SCHEMA IF NOT EXISTS github;
        CREATE TABLE IF NOT EXISTS github.repositories (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
        CREATE TABLE IF NOT EXISTS github.repositories_test (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
		CREATE TABLE IF NOT EXISTS github.pulls (
			id BIGINT NOT NULL,
			repo VARCHAR(255) NOT NULL,
			data JSON,
			PRIMARY KEY (id, repo)
		);
		CREATE TABLE IF NOT EXISTS github.pulls_test (
			id BIGINT NOT NULL,
			repo VARCHAR(255) NOT NULL,
			data JSON,
			PRIMARY KEY (id, repo)
		);
        CREATE TABLE IF NOT EXISTS github.reports_dataset (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
		CREATE INDEX idx_id_pulls ON github.pulls (id);
		CREATE INDEX idx_repo_pulls ON github.pulls (repo);
		CREATE INDEX idx_id_pulls_test ON github.pulls_test (id);
		CREATE INDEX idx_repo_pulls_test ON github.pulls_test (repo);
    `
	err = executeSQL(newDB, schemaSQL)
	if err != nil {
		log.Printf("Error creating schemas and tables in %s: %s", newDBName, err)
	}

	return nil
}

// getLatestUpdatesFromPostgres retrieves the latest update times for each repository from a PostgreSQL database.
// It connects to the PostgreSQL database using the provided connection string and queries the update times.
//
// Arguments:
//   - dbConfig: map[string]string containing the database configuration,
//     including the connection string under the key "connectionString".
//
// Returns:
//   - map[string]string: A map where keys are repository names and values are the latest update times.
//   - error: An error object if an error occurs, otherwise nil.
func GetPullsLatestUpdates(dbConfig map[string]string) (map[string]string, error) {
	ctx := context.Background()

	// Connect to the PostgreSQL database.
	db, err := ConnectByString(dbConfig["connectionString"])
	if err != nil {
		log.Printf("Check Pulls Latest Updates: PostgreSQL: Error: %s", err)
		return nil, err
	}
	defer db.Close()

	query := `
        SELECT
            repo,
            MAX(data->>'updated_at') AS updated_at
        FROM
            github.pulls
        GROUP BY
            repo
        ORDER BY
            updated_at DESC;
    `

	// Execute the query to get the latest update times.
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lastUpdates := make(map[string]string)
	// Iterate over the result set and populate the map with the latest update times.
	for rows.Next() {
		var repo string
		var updatedAt string
		if err := rows.Scan(&repo, &updatedAt); err != nil {
			return nil, err
		}
		lastUpdates[repo] = updatedAt
	}

	// Check for errors during the iteration over the result set.
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return lastUpdates, nil
}

func DeleteSchema(connection_string string) error {

	newDBName, err := getDbName(connection_string)
	if err != nil {
		log.Printf("getDbName: Error: %v\n", err)
		return err
	}
	log.Printf("getDbName: Database name to delete: %s", newDBName)

	connect_postgres_db_string := strings.Replace(connection_string, fmt.Sprintf("dbname=%s", newDBName), "dbname=postgres", 1)
	log.Printf("DeleteDB: Connect String to Postgres DB: %s", connect_postgres_db_string)

	mainDB, err := ConnectByString(connect_postgres_db_string)
	if err != nil {
		log.Printf("DeleteDB: Connect to Main DB Error: %v", err)
		return err
	}
	defer mainDB.Close()

	dbExists, err := databaseExists(mainDB, newDBName)
	if err != nil {
		log.Printf("Error checking if database exists: %v", err)
		return err
	}

	if dbExists {
		log.Printf("Database %s exists, terminating active connections", newDBName)

		terminateConnectionsSQL := fmt.Sprintf("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '%s' AND pid <> pg_backend_pid();", newDBName)
		_, err := mainDB.Exec(terminateConnectionsSQL)
		if err != nil {
			log.Printf("Error terminating active connections for database %s: %v", newDBName, err)
			return err
		}

		log.Printf("Deleting database %s", newDBName)
		err = executeSQL(mainDB, fmt.Sprintf("DROP DATABASE %s;", newDBName))
		if err != nil {
			log.Printf("Error deleting database %s: %v", newDBName, err)
			return err
		}
		log.Printf("Database %s deleted successfully", newDBName)
	} else {
		log.Printf("Database %s does not exist", newDBName)
	}

	return nil
}

func getDbName(connStr string) (string, error) {
	params := strings.Split(connStr, " ")
	for _, param := range params {
		keyValue := strings.SplitN(param, "=", 2)
		if len(keyValue) == 2 && keyValue[0] == "dbname" {
			return keyValue[1], nil
		}
	}
	return "", errors.New("dbname not found in connection string")
}
