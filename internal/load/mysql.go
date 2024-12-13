package load

import (
	"database/sql"
	"fmt"
	"github-stat/internal/databases/mysql"
	"log"

	"golang.org/x/exp/rand"
)

// MySQLSwitch1 loads logic when the first switch on the control panel is on.
// These queries run in a loop in each connection.
func MySQLSwitch1(db *sql.DB, id int, dbConfig map[string]string) {

	// Get the list of unique repository ids.
	repos_ids, err := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM repositories;")
	if err != nil {
		log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)

	} else if len(repos_ids) > 0 {

		// Get a random repository id
		randomIndex := rand.Intn(len(repos_ids))
		randomRepoID := repos_ids[randomIndex]

		// Get the repository data
		query := fmt.Sprintf("SELECT data FROM repositories WHERE id = %d;", randomRepoID)
		data, err := mysql.SelectString(db, query)
		if err != nil {
			log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Check if the repository data is in the test table.
		query = fmt.Sprintf("SELECT COUNT(*) FROM repositoriesTest WHERE id = %d;", randomRepoID)
		count, _ := mysql.SelectInt(db, query)

		// If there is data, we do Update, if not, we do Insert.
		if count > 0 {
			_, err = db.Exec("UPDATE repositoriesTest SET data = ? WHERE id = ?", data, randomRepoID)
			if err != nil {
				log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		} else {
			_, err = db.Exec("INSERT INTO repositoriesTest (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?", randomRepoID, data, data)
			if err != nil {
				log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}

		// Each even-numbered connection will delete data from the test table.
		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM repositoriesTest WHERE id = ?", randomRepoID)
			if err != nil {
				log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

// MySQLSwitch2 loads logic when the second switch on the control panel is on.
// These queries run in a loop in each connection.
func MySQLSwitch2(db *sql.DB, id int, dbConfig map[string]string) {

	// Get the list of unique pull request ids.
	uniq_pulls_ids, err := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM pulls;")
	if err != nil {
		log.Printf("MySQL: Error: Switch2: 1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)

	} else if len(uniq_pulls_ids) > 0 {
		// Get a random pull request id
		randomId := rand.Intn(len(uniq_pulls_ids))
		randomPull := uniq_pulls_ids[randomId]

		// Get the pull request data
		query := fmt.Sprintf("SELECT repo, data FROM pulls WHERE id = %d;", randomPull)
		row := db.QueryRow(query)

		var repo, data string
		if err := row.Scan(&repo, &data); err != nil {
			log.Printf("MySQL: Error: Switch2: 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Check if the pull request data is in the test table.
		query = fmt.Sprintf("SELECT COUNT(*) FROM pullsTest WHERE id = %d;", randomPull)
		count, _ := mysql.SelectInt(db, query)
		if count > 0 {
			_, err = db.Exec("UPDATE pullsTest SET data = ? WHERE id = ?", data, randomPull)
			if err != nil {
				log.Printf("MySQL: Error: Switch2: 3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		} else {
			_, err = db.Exec("INSERT INTO pullsTest (id, repo, data) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE data = ?", randomPull, repo, data, data)
			if err != nil {
				log.Printf("MySQL: Error: Switch2: 4: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}

		// Insert the pull request data into the main table
		_, err = db.Exec("INSERT INTO pulls (id, repo, data) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE data = ?", randomPull, repo, data, data)
		if err != nil {
			log.Printf("MySQL: Error: Switch2: 5: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Each even-numbered connection will delete data from the test table.
		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM pullsTest WHERE id = ?", randomPull)
			if err != nil {
				log.Printf("MySQL: Error: Switch2: 6: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

// MySQLSwitch3 loads logic when the third switch on the control panel is on.
// These queries run in a loop in each connection.
func MySQLSwitch3(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 != 0 {
		// Get a random repository name from the list of unique repository names in pulls
		repo, err := mysql.SelectString(db, `SELECT repo FROM (SELECT DISTINCT repo FROM pulls) AS uniq_repos ORDER BY RAND() LIMIT 1`)
		if err != nil {
			log.Printf("MySQL: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		if repo != "" {
			// Get the data from the selected repository
			query := fmt.Sprintf("SELECT data FROM pulls WHERE repo = '%s' ORDER BY id ASC LIMIT 10", repo)
			_, err = mysql.SelectListOfStrings(db, query)
			if err != nil {
				log.Printf("MySQL: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

// MySQLSwitch4 loads logic when the fourth switch on the control panel is on.
// These queries run in a loop in each connection.
func MySQLSwitch4(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 != 0 {
		// Get pull request data created within the last 3 months
		query := `
        SELECT data FROM pulls 
        WHERE STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.created_at')), '%Y-%m-%dT%H:%i:%sZ') >= NOW() - INTERVAL 3 MONTH 
        LIMIT 10;
        `
		_, err := mysql.SelectPulls(db, query)
		if err != nil {
			log.Printf("MySQL: Error: Switch4: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}
}
