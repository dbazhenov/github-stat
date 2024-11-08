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

	app "github-stat/internal"
)

func Connect(envVars app.EnvVars) (*sql.DB, error) {
	dsn := fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
		envVars.Postgres.User, envVars.Postgres.Password, envVars.Postgres.DB, envVars.Postgres.Host, envVars.Postgres.Port)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Printf("PostgreSQL Connect: Error: %s", err)
		return nil, err
	}

	return db, nil
}

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

func GetConnectionString(envVars app.EnvVars) string {
	return fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
		app.Config.Postgres.User,
		app.Config.Postgres.Password,
		app.Config.Postgres.DB,
		app.Config.Postgres.Host,
		app.Config.Postgres.Port)
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

func InsertPulls(db *sql.DB, pullRequests []*github.PullRequest, table string) error {
	for _, pull := range pullRequests {
		id := pull.ID
		pullJSON, err := json.Marshal(pull)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(`INSERT INTO github.%s (id, repo, data) 
			VALUES ($1, $2, $3) 
			ON CONFLICT (id, repo) DO UPDATE SET data = $3`, table)

		_, err = db.Exec(query, id, *pull.Base.Repo.Name, pullJSON)
		if err != nil {
			return err
		}
	}

	return nil
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

func InitDB(connection_string string) error {

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
        CREATE TABLE IF NOT EXISTS github.reports_runs (
            id SERIAL PRIMARY KEY,
            data JSONB
        );
        CREATE TABLE IF NOT EXISTS github.reports_databases (
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
