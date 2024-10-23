package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/go-github/github"

	app "github-stat/internal"
)

func Connect(envVars app.EnvVars) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		envVars.MySQL.User, envVars.MySQL.Password, envVars.MySQL.Host, envVars.MySQL.Port, envVars.MySQL.DB)

	log.Printf("Databases: MySQL: Start: Connect to: %s", envVars.MySQL.Host)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("MySQL Error: %s", err)
		return nil, err
	}

	return db, nil
}

func ConnectByString(connection_string string) (*sql.DB, error) {

	db, err := sql.Open("mysql", connection_string)
	if err != nil {
		log.Printf("MySQL Error: %s", err)
		return nil, err
	}

	return db, nil
}

func CheckMySQL(connectionString string) string {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return fmt.Sprintf("Error opening connection: %v", err)
	}
	defer db.Close()

	// Using a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return "Error: Connection timeout exceeded"
		}
		return fmt.Sprintf("Error checking connection: %v", err)
	}

	return "Connected"
}

func GetConnectionString(envVars app.EnvVars) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		app.Config.MySQL.User,
		app.Config.MySQL.Password,
		app.Config.MySQL.Host,
		app.Config.MySQL.Port,
		app.Config.MySQL.DB)
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

		query := fmt.Sprintf(`INSERT INTO github.%s (id, repo, data) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE data = ?`, table)

		_, err = db.Exec(query, id, *pull.Base.Repo.Name, pullJSON, pullJSON)
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

func InitDB(connection_string string) error {
	newDBName, err := getDbName(connection_string)
	if err != nil {
		log.Printf("getDbName: Error: %v\n", err)
		return err
	} else {
		log.Printf("getDbName: New Database name: %s", newDBName)
	}

	connectMysqlDbString := strings.Replace(connection_string, fmt.Sprintf("/%s", newDBName), "/", 1)
	log.Printf("InitDB: Connect String to MySQL DB: %s", connectMysqlDbString)
	mainDB, err := ConnectByString(connectMysqlDbString)
	if err != nil {
		log.Printf("InitDB: Connect to Main DB: Error: %s", newDBName)
		return err
	}
	defer mainDB.Close()

	dbExists, err := databaseExists(mainDB, newDBName)
	if err != nil {
		log.Printf("Error checking if database exists: %v", err)
		return err
	}
	if !dbExists {
		log.Printf("Database %s does not exist, creating database", newDBName)
		err := executeSQL(mainDB, fmt.Sprintf("CREATE DATABASE %s;", newDBName))
		if err != nil {
			log.Printf("Error creating database %s: %v", newDBName, err)
			return err
		}
	}

	newDB, err := ConnectByString(connection_string)
	if err != nil {
		log.Printf("InitDB: Connect to New DB: Error: %s", newDBName)
		return err
	}
	defer newDB.Close()

	log.Printf("Creating tables in database %s", newDBName)

	schemaSQLs := []string{
		"CREATE TABLE IF NOT EXISTS repositories (id INT AUTO_INCREMENT PRIMARY KEY, data JSON);",
		"CREATE TABLE IF NOT EXISTS pulls (id INT NOT NULL, repo VARCHAR(255) NOT NULL, data JSON, PRIMARY KEY (id, repo));",
		"CREATE TABLE IF NOT EXISTS pullsTest (id INT NOT NULL, repo VARCHAR(255) NOT NULL, data JSON, PRIMARY KEY (id, repo));",
		"CREATE TABLE IF NOT EXISTS reports_runs (id INT AUTO_INCREMENT PRIMARY KEY, data JSON);",
		"CREATE TABLE IF NOT EXISTS reports_databases (id INT AUTO_INCREMENT PRIMARY KEY, data JSON);",
	}

	for _, query := range schemaSQLs {
		log.Printf("Executing query: %s", query)
		if err := executeSQL(newDB, query); err != nil {
			log.Printf("Error executing query %s: %v", query, err)
			// Drop the database if there is an error
			dropErr := executeSQL(mainDB, fmt.Sprintf("DROP DATABASE IF EXISTS %s;", newDBName))
			if dropErr != nil {
				log.Printf("Error dropping database %s: %v", newDBName, dropErr)
			} else {
				log.Printf("Database %s dropped due to query error", newDBName)
			}
			return err
		}
	}

	return nil
}

func getDbName(connStr string) (string, error) {
	parts := strings.Split(connStr, "/")
	if len(parts) < 2 {
		return "", errors.New("database name not found in connection string")
	}
	dbParams := strings.Split(parts[1], "?")
	if len(dbParams) == 0 {
		return "", errors.New("database name not found in connection string")
	}
	return dbParams[0], nil
}

func databaseExists(db *sql.DB, dbName string) (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s';", dbName)
	err := db.QueryRow(query).Scan(&exists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func executeSQL(db *sql.DB, query string) error {
	_, err := db.Exec(query)
	return err
}
