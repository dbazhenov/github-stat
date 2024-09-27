package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	app "github-stat/internal"
	"log"

	"github.com/google/go-github/github"
	_ "github.com/lib/pq"
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
