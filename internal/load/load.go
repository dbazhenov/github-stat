package load

import (
	"database/sql"
	"fmt"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/exp/rand"
)

func MySQLSwitch1(db *sql.DB, id int, dbConfig map[string]string) {
	repos_with_pulls, err := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM repositories;")

	if err != nil {
		log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if len(repos_with_pulls) > 0 {
		randomIndex := rand.Intn(len(repos_with_pulls))
		randomRepo := repos_with_pulls[randomIndex]

		query := fmt.Sprintf("SELECT data FROM repositories WHERE id = %d;", randomRepo)

		data, err := mysql.SelectString(db, query)
		if err != nil {
			log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		query = fmt.Sprintf("SELECT COUNT(*) FROM repositoriesTest WHERE id = %d;", randomRepo)
		count, _ := mysql.SelectInt(db, query)
		if count > 0 {
			_, err = db.Exec("UPDATE repositoriesTest SET data = ? WHERE id = ?", data, randomRepo)
			if err != nil {
				log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		} else {
			_, err = db.Exec("INSERT INTO repositoriesTest (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?", randomRepo, data, data)
			if err != nil {
				log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}

		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM repositoriesTest WHERE id = ?", randomRepo)
			if err != nil {
				log.Printf("MySQL: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

func MySQLSwitch2(db *sql.DB, id int, dbConfig map[string]string) {
	uniq_pulls_ids, err := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM pulls;")

	if err != nil {
		log.Printf("MySQL: Error: Switch2: 1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if len(uniq_pulls_ids) > 0 {
		randomId := rand.Intn(len(uniq_pulls_ids))
		randomPull := uniq_pulls_ids[randomId]

		query := fmt.Sprintf("SELECT repo, data FROM pulls WHERE id = %d;", randomPull)
		row := db.QueryRow(query)

		var repo, data string
		if err := row.Scan(&repo, &data); err != nil {
			log.Printf("MySQL: Error: Switch2: 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

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

		_, err = db.Exec("INSERT INTO pulls (id, repo, data) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE data = ?", randomPull, repo, data, data)
		if err != nil {
			log.Printf("MySQL: Error: Switch2: 5: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM pullsTest WHERE id = ?", randomPull)
			if err != nil {
				log.Printf("MySQL: Error: Switch2: 6: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

func MySQLSwitch3(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 != 0 {
		repo, err := mysql.SelectString(db, `SELECT repo FROM (SELECT DISTINCT repo FROM pulls) AS uniq_repos ORDER BY RAND() LIMIT 1`)
		if err != nil {
			log.Printf("MySQL: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		if repo != "" {
			query := fmt.Sprintf("SELECT data FROM pulls WHERE repo = '%s' ORDER BY id ASC LIMIT 10", repo)
			_, err = mysql.SelectListOfStrings(db, query)
			if err != nil {
				log.Printf("MySQL: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

func MySQLSwitch4(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 != 0 {
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

func PostgresSwitch1(db *sql.DB, id int, dbConfig map[string]string) {
	repos_with_pulls, err := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.repositories;")

	if err != nil {
		log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		return
	} else if len(repos_with_pulls) > 0 {
		randomIndex := rand.Intn(len(repos_with_pulls))
		randomRepo := repos_with_pulls[randomIndex]

		query := fmt.Sprintf("SELECT data FROM github.repositories WHERE id = %d;", randomRepo)

		data, err := postgres.SelectString(db, query)
		if err != nil {
			log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			return
		}

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

		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM github.repositories_test WHERE id = $1", randomRepo)
			if err != nil {
				log.Printf("Postgres: Error: Switch1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

func PostgresSwitch2(db *sql.DB, id int, dbConfig map[string]string) {
	uniq_pulls_ids, err := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")

	if err != nil {
		log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if len(uniq_pulls_ids) > 0 {
		randomId := rand.Intn(len(uniq_pulls_ids))
		randomPull := uniq_pulls_ids[randomId]

		query := fmt.Sprintf("SELECT repo, data FROM github.pulls WHERE id = %d;", randomPull)
		row := db.QueryRow(query)

		var repo, data string
		if err := row.Scan(&repo, &data); err != nil {
			log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

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

		_, err = db.Exec("INSERT INTO github.pulls (id, repo, data) VALUES ($1, $2, $3) ON CONFLICT (id, repo) DO UPDATE SET data = $3", randomPull, repo, data)
		if err != nil {
			log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		if id%2 != 0 {
			_, err = db.Exec("DELETE FROM github.pulls_test WHERE id = $1", randomPull)
			if err != nil {
				log.Printf("Postgres: Error: Switch2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

func PostgresSwitch3(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 != 0 {
		repo, err := postgres.SelectString(db, `SELECT repo FROM (SELECT DISTINCT repo FROM github.pulls) AS uniq_repos ORDER BY RANDOM() LIMIT 1`)
		if err != nil {
			log.Printf("Postgres: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			return
		}

		if repo != "" {
			query := fmt.Sprintf("SELECT data FROM github.pulls WHERE repo = '%s' ORDER BY id ASC LIMIT 10", repo)
			_, err = postgres.SelectListOfStrings(db, query)
			if err != nil {
				log.Printf("Postgres: Error: Switch3: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}
	}
}

func PostgresSwitch4(db *sql.DB, id int, dbConfig map[string]string) {
	if id%2 != 0 {

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

func MongoDBSwitch1(client *mongo.Client, db string, id int, dbConfig map[string]string) {

	ids, err := mongodb.GetUniqueIntegers(client, db, "repositories", "id")

	if err != nil {
		log.Printf("MongoDB: Switch 1: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if len(ids) > 0 {
		randomIndex := rand.Intn(len(ids))
		randomRepo := ids[randomIndex]

		filter := bson.D{{Key: "id", Value: randomRepo}}
		repo, err := mongodb.FindOne(client, db, "repositories", filter, bson.D{})
		if err != nil {
			log.Printf("MongoDB: Switch 1: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		if randomRepo%2 == 0 {
			_, err = mongodb.UpsertOneDoc(client, db, "repositoriesTest", repo)
			if err != nil {
				log.Printf("MongoDB: Switch 1: Upsert One: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		} else {
			_, err = mongodb.InsertOneDoc(client, db, "repositoriesTest", repo)
			if err != nil && !mongo.IsDuplicateKeyError(err) {
				log.Printf("MongoDB: Switch 1: Insert One: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}

		filter_delete := bson.D{{Key: "id", Value: randomRepo}}

		err = mongodb.DeleteDocuments(client, db, "repositoriesTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Switch 1: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}

	_, err = mongodb.SelectRandomDocument(client, db, "pulls")
	if err != nil {
		log.Printf("MongoDB: Error: Switch 1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}
}

func MongoDBSwitch2(client *mongo.Client, db string, id int, dbConfig map[string]string) {
	one_document, err := mongodb.SelectRandomDocument(client, db, "pulls")

	if err != nil {
		log.Printf("MongoDB: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if one_document != nil {
		filter := bson.D{{Key: "name", Value: one_document["repo"]}}
		_, err := mongodb.FindOne(client, db, "repositories", filter, bson.D{})
		if err != nil {
			log.Printf("MongoDB: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		if id%2 == 0 {
			_, err = mongodb.UpsertOneDoc(client, db, "pullsTest", one_document)
			if err != nil {
				log.Printf("MongoDB: Upsert One: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		} else {
			_, err = mongodb.InsertOneDoc(client, db, "pullsTest", one_document)
			if err != nil && !mongo.IsDuplicateKeyError(err) {
				log.Printf("MongoDB: Insert One: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
			}
		}

		filter_delete := bson.D{{Key: "id", Value: one_document["id"]}}

		err = mongodb.DeleteDocuments(client, db, "pullsTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Delete old documents: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}
}

func MongoDBSwitch3(client *mongo.Client, db string, id int, dbConfig map[string]string) {
	filterPulls := bson.D{}

	documents, err := mongodb.FindPullRequests(client, db, "pulls", filterPulls, bson.D{}, 100)
	if err != nil {
		log.Printf("MongoDB: Switch3: List docs: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}

	if len(documents) > 0 {
		// Extract all ids from documents
		var interfaceDocs []interface{}
		pulls_ids := make([]interface{}, len(documents))
		for i, doc := range documents {
			interfaceDocs = append(interfaceDocs, doc)
			pulls_ids[i] = doc.ID
		}

		_, err = mongodb.InsertManyDocuments(client, db, "pullsTest", interfaceDocs)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			log.Printf("MongoDB: Switch3: Insert: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
		// Create filter to delete documents by id
		filter_delete := bson.D{{Key: "id", Value: bson.D{{Key: "$in", Value: pulls_ids}}}}
		err = mongodb.DeleteDocuments(client, db, "pullsTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Switch3: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}

	repos, err := mongodb.FindRepos(client, db, "repositories", filterPulls, bson.D{}, 100)
	if err != nil {
		log.Printf("MongoDB: Switch3: List docs: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}

	if len(repos) > 0 {
		var reposDocs []interface{}
		repos_ids := make([]interface{}, len(repos))
		for i, doc := range repos {
			reposDocs = append(reposDocs, doc)
			repos_ids[i] = doc.ID
		}

		_, err = mongodb.InsertManyDocuments(client, db, "repositoriesTest", reposDocs)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			log.Printf("MongoDB: Switch3: Insert: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Create filter to delete documents by id
		filter_repos_delete := bson.D{{Key: "id", Value: bson.D{{Key: "$in", Value: repos_ids}}}}
		err = mongodb.DeleteDocuments(client, db, "repositoriesTest", filter_repos_delete)
		if err != nil {
			log.Printf("MongoDB: Switch3: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		} else {
			log.Printf("MongoDB: Switch3: goroutine: %d: database: %s: Repos not found, probably database is empty, run dataset import", id, dbConfig["id"])
		}
	}
}

func MongoDBSwitch4(client *mongo.Client, db string, id int, dbConfig map[string]string) {
	filter_repos := bson.D{{Key: "stargazerscount", Value: bson.D{{Key: "$gt", Value: 10}}}}

	sort_repos := bson.D{{Key: "stargazerscount", Value: -1}}
	_, err := mongodb.FindRepos(client, db, "repositories", filter_repos, sort_repos, 10)
	if err != nil {
		log.Printf("MongoDB: Switch4: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}

	time := time.Now().AddDate(0, -3, 0)

	filterPulls := bson.D{{Key: "createdat", Value: bson.D{{Key: "$gt", Value: time}}}}

	documents, err := mongodb.FindDocuments(client, db, "pulls", filterPulls, bson.D{}, 10)
	if err != nil {
		log.Printf("MongoDB: Switch4: List docs: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}

	if len(documents) > 0 {
		_, err = mongodb.InsertManyDocuments(client, db, "pullsTest", documents)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			log.Printf("MongoDB: Switch4: Insert Many Docs: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		filter_delete := bson.D{{Key: "createdat", Value: bson.D{{Key: "$lt", Value: time}}}}

		err = mongodb.DeleteDocuments(client, db, "pullsTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Switch4: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}
}
