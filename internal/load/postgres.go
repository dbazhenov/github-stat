package load

import (
	"database/sql"
	"fmt"
	"github-stat/internal/databases/postgres"
	"log"

	"golang.org/x/exp/rand"
)

// PostgresSwitch1 handles the logic when the first switch on the control panel is on.
// These queries run in a loop in each connection.
func PostgresSwitch1(db *sql.DB, id int, dbConfig map[string]string) {
	// Get the list of unique repository ids with pull requests.
	repos_with_pulls, err := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.repositories;")
	if err != nil {
		log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		return
	} else if len(repos_with_pulls) > 0 {
		// Get a random repository id.
		randomIndex := rand.Intn(len(repos_with_pulls))
		randomRepo := repos_with_pulls[randomIndex]

		// Get the repository data.
		query := fmt.Sprintf("SELECT data FROM github.repositories WHERE id = %d;", randomRepo)
		data, err := postgres.SelectString(db, query)
		if err != nil {
			log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			return
		}

		// Check if the repository data is in the test table.
		query = fmt.Sprintf("SELECT COUNT(*) FROM github.repositories_test WHERE id = %d;", randomRepo)
		count, _ := postgres.SelectInt(db, query)
		if count > 0 {
			_, err = db.Exec("UPDATE github.repositories_test SET data = $1 WHERE id = $2", data, randomRepo)
			if err != nil {
				log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		} else {
			_, err = db.Exec("INSERT INTO github.repositories_test (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2", randomRepo, data)
			if err != nil {
				log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}

		// Each even-numbered connection will delete data from the test table.
		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM github.repositories_test WHERE id = $1", randomRepo)
			if err != nil {
				log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

// PostgresSwitch2 handles the logic when the second switch on the control panel is on.
// These queries run in a loop in each connection.
func PostgresSwitch2(db *sql.DB, id int, dbConfig map[string]string) {
	// Get the list of unique pull request ids.
	uniq_pulls_ids, err := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")
	if err != nil {
		log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if len(uniq_pulls_ids) > 0 {
		// Get a random pull request id.
		randomId := rand.Intn(len(uniq_pulls_ids))
		randomPull := uniq_pulls_ids[randomId]

		// Get the pull request data.
		query := fmt.Sprintf("SELECT repo, data FROM github.pulls WHERE id = %d;", randomPull)
		row := db.QueryRow(query)

		var repo, data string
		if err := row.Scan(&repo, &data); err != nil {
			log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Check if the pull request data is in the test table.
		query = fmt.Sprintf("SELECT COUNT(*) FROM github.pulls_test WHERE id = %d;", randomPull)
		count, _ := postgres.SelectInt(db, query)
		if count > 0 {
			_, err = db.Exec("UPDATE github.pulls_test SET data = $1 WHERE id = $2", data, randomPull)
			if err != nil {
				log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		} else {
			_, err = db.Exec("INSERT INTO github.pulls_test (id, repo, data) VALUES ($1, $2, $3) ON CONFLICT (id, repo) DO UPDATE SET data = $3", randomPull, repo, data)
			if err != nil {
				log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}

		// Insert the pull request data into the main table.
		_, err = db.Exec("INSERT INTO github.pulls (id, repo, data) VALUES ($1, $2, $3) ON CONFLICT (id, repo) DO UPDATE SET data = $3", randomPull, repo, data)
		if err != nil {
			log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Each even-numbered connection will delete data from the test table.
		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM github.pulls_test WHERE id = $1", randomPull)
			if err != nil {
				log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

// PostgresSwitch3 handles the logic when the third switch on the control panel is on.
// This function runs in a loop for each even-numbered connection.
func PostgresSwitch3(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 == 0 {
		// Get a random repository name from the list of unique repository names in pulls.
		repo, err := postgres.SelectString(db, `SELECT repo FROM (SELECT DISTINCT repo FROM github.pulls) AS uniq_repos ORDER BY RANDOM() LIMIT 1`)
		if err != nil {
			log.Printf("Postgres: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			return
		}

		if repo != "" {
			// Get the data from the selected repository.
			query := fmt.Sprintf("SELECT data FROM github.pulls WHERE repo = '%s' ORDER BY id ASC LIMIT 10", repo)
			_, err = postgres.SelectListOfStrings(db, query)
			if err != nil {
				log.Printf("Postgres: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

// PostgresSwitch4 handles the logic when the fourth switch on the control panel is on.
// This function runs in a loop for each even-numbered connection.
func PostgresSwitch4(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 == 0 {
		// Get pull request data created within the last 3 months.
		query := `
            SELECT data 
            FROM github.pulls 
            WHERE (to_timestamp((data->>'created_at')::text, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') >= NOW() - INTERVAL '3 months') 
            LIMIT 10;
        `
		_, err := postgres.SelectPulls(db, query)
		if err != nil {
			log.Printf("Postgres: Error: Switch4: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}
}
