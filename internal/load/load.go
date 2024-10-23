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

func MySQLSwitch1(db *sql.DB, id int) {

	repos_with_pulls, err := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM github.repositories;")

	if err != nil {
		log.Printf("MySQL: Error: goroutine: %d: message: %s", id, err)
	} else {
		randomIndex := rand.Intn(len(repos_with_pulls))
		randomRepo := repos_with_pulls[randomIndex]

		query := fmt.Sprintf("SELECT data FROM github.repositories WHERE id = %d;", randomRepo)

		_, err := mysql.SelectString(db, query)

		if err != nil {
			log.Printf("MySQL: Error: goroutine: %d: message: %s", id, err)
		}
		// log.Printf("MySQL: Ok: goroutine: %d: message: %d", id, randomRepo)
	}

}

func MySQLSwitch2(db *sql.DB, id int) {

	uniq_pulls_ids, err := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")

	if err != nil {
		log.Printf("MySQL: Error: goroutine: %d: message: %s", id, err)
	} else {
		randomId := rand.Intn(len(uniq_pulls_ids))
		randomPull := uniq_pulls_ids[randomId]

		query := fmt.Sprintf("SELECT data FROM github.pulls WHERE id = %d;", randomPull)

		pull, err := mysql.SelectPulls(db, query)
		if err != nil {
			log.Printf("MySQL: Error: goroutine: %d: message: %s", id, err)
		}

		err = mysql.InsertPulls(db, pull, "pullsTest")
		if err != nil {
			log.Printf("MySQL: Error: goroutine: %d: message: %s", id, err)
		}
	}
}

func MySQLSwitch3(db *sql.DB, id int) {

	_, err := mysql.SelectString(db, `SELECT repo FROM github.pulls ORDER BY RAND() LIMIT 1;`)
	if err != nil {
		log.Printf("MySQL: Error: goroutine: %d: message: %s", id, err)
	}

}

func MySQLSwitch4(db *sql.DB, id int) {

	query := `
		SELECT data FROM github.pulls 
		WHERE STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.created_at')), '%Y-%m-%dT%H:%i:%sZ') >= NOW() - INTERVAL 3 MONTH 
		LIMIT 10;
	`

	_, err := mysql.SelectPulls(db, query)
	if err != nil {
		log.Printf("MySQL: Error: goroutine: %d: message: %s", id, err)
	}
}

func PostgresSwitch1(db *sql.DB, id int) {

	repos_with_pulls, err := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.repositories;")

	if err != nil {
		log.Printf("Postgres: Error: goroutine: %d: message: %s", id, err)
	} else {
		randomIndex := rand.Intn(len(repos_with_pulls))
		randomRepo := repos_with_pulls[randomIndex]

		query := fmt.Sprintf("SELECT data FROM github.repositories WHERE id = %d;", randomRepo)

		_, err := postgres.SelectString(db, query)

		if err != nil {
			log.Printf("Postgres: Error: goroutine: %d: message: %s", id, err)
		}
	}
}

func PostgresSwitch2(db *sql.DB, id int) {

	uniq_pulls_ids, err := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")

	if err != nil {
		log.Printf("Postgres: Error: goroutine: %d: message: %s", id, err)
	} else {
		randomId := rand.Intn(len(uniq_pulls_ids))
		randomPull := uniq_pulls_ids[randomId]

		query := fmt.Sprintf("SELECT data FROM github.pulls WHERE id = %d;", randomPull)

		pull, err := postgres.SelectPulls(db, query)
		if err != nil {
			log.Printf("Postgres: Error: goroutine: %d: message: %s", id, err)
		}

		err = postgres.InsertPulls(db, pull, "pullsTest")
		if err != nil {
			log.Printf("Postgres: Error: goroutine: %d: message: %s", id, err)
		}
	}
}

func PostgresSwitch3(db *sql.DB, id int) {

	_, err := postgres.SelectString(db, `SELECT repo FROM github.pulls ORDER BY RANDOM() LIMIT 1;`)

	if err != nil {
		log.Printf("Postgres: Error: goroutine: %d: message: %s", id, err)
	}

}

func PostgresSwitch4(db *sql.DB, id int) {

	query := `
		SELECT data 
		FROM github.pulls 
		WHERE (to_timestamp((data->>'created_at')::text, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') >= NOW() - INTERVAL '3 months') 
		LIMIT 10;
	`
	_, err := postgres.SelectPulls(db, query)
	if err != nil {
		log.Printf("Postgres: Error: goroutine: %d: message: %s", id, err)
	}
}

func MongoDBSwitch1(client *mongo.Client, id int) {

	// _, err := mongodb.SelectRandomDocument(client, "github", "repositories")
	// if err != nil {
	// 	log.Printf("MongoDB: Error: goroutine: %d: message: %s", id, err)
	// }
	ids, err := mongodb.GetUniqueIntegers(client, "github", "repositories", "id")

	if err != nil {
		log.Printf("MongoDB: Error: goroutine: %d: message: %s", id, err)
	} else {
		randomIndex := rand.Intn(len(ids))
		randomRepo := ids[randomIndex]

		filter := bson.D{{Key: "id", Value: randomRepo}}
		repo, err := mongodb.FindOne(client, "github", "repositories", filter, bson.D{})
		if err != nil {
			log.Printf("MongoDB: Error: goroutine: %d: message: %s", id, err)
		}

		if randomRepo%2 == 0 {
			_, err = mongodb.UpsertOneDoc(client, "github", "repositoriesTest", repo)
			if err != nil {
				log.Printf("MongoDB: Upsert One: Error: %s", err)
			}
		} else {
			_, err = mongodb.InsertOneDoc(client, "github", "repositoriesTest", repo)
			if err != nil && !mongo.IsDuplicateKeyError(err) {
				log.Printf("MongoDB: Insert One: Error: %s", err)
			}
		}

		filter_delete := bson.D{{Key: "id", Value: randomRepo}}

		err = mongodb.DeleteDocuments(client, "github", "repositoriesTest", filter_delete)

		if err != nil {
			log.Printf("MongoDB: Delete old documents: Error: %s", err)
		}

	}

	_, err = mongodb.SelectRandomDocument(client, "github", "pulls")
	if err != nil {
		log.Printf("MongoDB: Error: goroutine: %d: message: %s", id, err)
	}

}

func MongoDBSwitch2(client *mongo.Client, id int) {

	one_document, err := mongodb.SelectRandomDocument(client, "github", "pulls")

	if err != nil {
		log.Printf("MongoDB: Error: goroutine: %d: message: %s", id, err)
	} else {

		filter := bson.D{{Key: "name", Value: one_document["repo"]}}
		_, err := mongodb.FindOne(client, "github", "repositories", filter, bson.D{})
		if err != nil {
			log.Printf("MongoDB: Error: goroutine: %d: message: %s", id, err)
		}

		if id%2 == 0 {
			_, err = mongodb.UpsertOneDoc(client, "github", "pullsTest", one_document)
			if err != nil {
				log.Printf("MongoDB: Upsert One: Error: %s", err)
			}
		} else {
			_, err = mongodb.InsertOneDoc(client, "github", "pullsTest", one_document)
			if err != nil && !mongo.IsDuplicateKeyError(err) {
				log.Printf("MongoDB: Insert One: Error: %s", err)
			}
		}

		filter_delete := bson.D{{Key: "id", Value: one_document["id"]}}

		err = mongodb.DeleteDocuments(client, "github", "pullsTest", filter_delete)

		if err != nil {
			log.Printf("MongoDB: Delete old documents: Error: %s", err)
		}
	}

}

func MongoDBSwitch3(client *mongo.Client, id int) {

	filterPulls := bson.D{}

	documents, err := mongodb.FindPullRequests(client, "github", "pulls", filterPulls, bson.D{}, 100)
	if err != nil {
		log.Printf("MongoDB: List docs: Error: %s", err)
	}

	// Extract all ids from documents
	var interfaceDocs []interface{}
	pulls_ids := make([]interface{}, len(documents))
	for i, doc := range documents {
		interfaceDocs = append(interfaceDocs, doc)
		pulls_ids[i] = doc.ID
	}

	_, err = mongodb.InsertManyDocuments(client, "github", "pullsTest", interfaceDocs)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		log.Printf("MongoDB: Insert: Error: %s", err)
	}

	// Create filter to delete documents by id
	filter_delete := bson.D{{Key: "id", Value: bson.D{{Key: "$in", Value: pulls_ids}}}}
	err = mongodb.DeleteDocuments(client, "github", "pullsTest", filter_delete)
	if err != nil {
		log.Printf("MongoDB: Delete old documents: Error: %s", err)
	}

	repos, err := mongodb.FindRepos(client, "github", "repositories", filterPulls, bson.D{}, 100)
	if err != nil {
		log.Printf("MongoDB: List docs: Error: %s", err)
	}

	// Преобразование документов в []interface{}
	var reposDocs []interface{}
	repos_ids := make([]interface{}, len(documents))
	for i, doc := range repos {
		reposDocs = append(reposDocs, doc)
		repos_ids[i] = doc.ID
	}

	_, err = mongodb.InsertManyDocuments(client, "github", "repositoriesTest", reposDocs)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		log.Printf("MongoDB: Insert: Error: %s", err)
	}

	// Create filter to delete documents by id
	filter_repos_delete := bson.D{{Key: "id", Value: bson.D{{Key: "$in", Value: repos_ids}}}}
	err = mongodb.DeleteDocuments(client, "github", "repositoriesTest", filter_repos_delete)
	if err != nil {
		log.Printf("MongoDB: Delete old documents: Error: %s", err)
	}

}

func MongoDBSwitch4(client *mongo.Client, id int) {

	filter_repos := bson.D{{Key: "stargazerscount", Value: bson.D{{Key: "$gt", Value: 10}}}}

	sort_repos := bson.D{{Key: "stargazerscount", Value: -1}}
	_, err := mongodb.FindRepos(client, "github", "repositories", filter_repos, sort_repos, 10)
	if err != nil {
		log.Fatal(err)
	}

	time := time.Now().AddDate(0, -3, 0)

	filterPulls := bson.D{{Key: "createdat", Value: bson.D{{Key: "$gt", Value: time}}}}

	documents, err := mongodb.FindDocuments(client, "github", "pulls", filterPulls, bson.D{}, 10)
	if err != nil {
		log.Printf("MongoDB: List docs: Error: %s", err)
	}

	_, err = mongodb.InsertManyDocuments(client, "github", "pullsTest", documents)
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		log.Printf("MongoDB: Insert Many Docs: Error: %s", err)
	}

	filter_delete := bson.D{{Key: "createdat", Value: bson.D{{Key: "$lt", Value: time}}}}

	err = mongodb.DeleteDocuments(client, "github", "pullsTest", filter_delete)

	if err != nil {
		log.Printf("MongoDB: Delete old documents: Error: %s", err)
	}

}
