package load

import (
	"github-stat/internal/databases/mongodb"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/exp/rand"
)

// MongoDBSwitch1 handles the logic when the first switch on the control panel is on.
// These queries run in a loop in each connection.
func MongoDBSwitch1(client *mongo.Client, db string, id int, dbConfig map[string]string) {

	// Get the list of unique repository ids.
	ids, err := mongodb.GetUniqueIntegers(client, db, "repositories", "id")
	if err != nil {
		log.Printf("MongoDB: Switch 1: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if len(ids) > 0 {
		// Get a random repository id.
		randomIndex := rand.Intn(len(ids))
		randomRepo := ids[randomIndex]

		// Get the repository data.
		filter := bson.D{{Key: "id", Value: randomRepo}}
		repo, err := mongodb.FindOne(client, db, "repositories", filter, bson.D{})
		if err != nil {
			log.Printf("MongoDB: Switch 1: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Upsert or insert the repository data into the test collection.
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

		// Delete old documents from the test collection.
		filter_delete := bson.D{{Key: "id", Value: randomRepo}}
		err = mongodb.DeleteDocuments(client, db, "repositoriesTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Switch 1: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}

	// Select a random document from the pulls collection.
	_, err = mongodb.SelectRandomDocument(client, db, "pulls")
	if err != nil {
		log.Printf("MongoDB: Error: Switch 1: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}
}

// MongoDBSwitch2 handles the logic when the second switch on the control panel is on.
// These queries run in a loop in each connection.
func MongoDBSwitch2(client *mongo.Client, db string, id int, dbConfig map[string]string) {

	// Select a random document from the pulls collection.
	one_document, err := mongodb.SelectRandomDocument(client, db, "pulls")
	if err != nil {
		log.Printf("MongoDB: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	} else if one_document != nil {
		// Find the repository associated with the pull request.
		filter := bson.D{{Key: "name", Value: one_document["repo"]}}
		_, err := mongodb.FindOne(client, db, "repositories", filter, bson.D{})
		if err != nil {
			log.Printf("MongoDB: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Upsert or insert the pull request data into the test collection.
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

		// Delete old documents from the test collection.
		filter_delete := bson.D{{Key: "id", Value: one_document["id"]}}
		err = mongodb.DeleteDocuments(client, db, "pullsTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Delete old documents: Error: Switch 2: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}
}

// MongoDBSwitch3 handles the logic when the third switch on the control panel is on.
// These queries run in a loop in each connection.
func MongoDBSwitch3(client *mongo.Client, db string, id int, dbConfig map[string]string) {
	filterPulls := bson.D{}

	// Find pull requests with no specific filter.
	documents, err := mongodb.FindPullRequests(client, db, "pulls", filterPulls, bson.D{}, 100)
	if err != nil {
		log.Printf("MongoDB: Switch3: List docs: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}

	if len(documents) > 0 {
		// Extract all ids from documents.
		var interfaceDocs []interface{}
		pulls_ids := make([]interface{}, len(documents))
		for i, doc := range documents {
			interfaceDocs = append(interfaceDocs, doc)
			pulls_ids[i] = doc.ID
		}

		// Insert pull request documents into the test collection.
		_, err = mongodb.InsertManyDocuments(client, db, "pullsTest", interfaceDocs)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			log.Printf("MongoDB: Switch3: Insert: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Create filter to delete documents by id.
		filter_delete := bson.D{{Key: "id", Value: bson.D{{Key: "$in", Value: pulls_ids}}}}
		err = mongodb.DeleteDocuments(client, db, "pullsTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Switch3: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}

	// Find repositories with no specific filter.
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

		// Insert repository documents into the test collection.
		_, err = mongodb.InsertManyDocuments(client, db, "repositoriesTest", reposDocs)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			log.Printf("MongoDB: Switch3: Insert: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Create filter to delete documents by id.
		filter_repos_delete := bson.D{{Key: "id", Value: bson.D{{Key: "$in", Value: repos_ids}}}}
		err = mongodb.DeleteDocuments(client, db, "repositoriesTest", filter_repos_delete)
		if err != nil {
			log.Printf("MongoDB: Switch3: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		} else {
			log.Printf("MongoDB: Switch3: goroutine: %d: database: %s: Repos not found, probably database is empty, run dataset import", id, dbConfig["id"])
		}
	}
}

// MongoDBSwitch4 handles the logic when the fourth switch on the control panel is on.
// These queries run in a loop in each connection.
func MongoDBSwitch4(client *mongo.Client, db string, id int, dbConfig map[string]string) {
	// Find repositories with more than 10 stargazers.
	filter_repos := bson.D{{Key: "stargazerscount", Value: bson.D{{Key: "$gt", Value: 10}}}}
	sort_repos := bson.D{{Key: "stargazerscount", Value: -1}}
	_, err := mongodb.FindRepos(client, db, "repositories", filter_repos, sort_repos, 10)
	if err != nil {
		log.Printf("MongoDB: Switch4: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}

	// Get the current time minus 3 months.
	time := time.Now().AddDate(0, -3, 0)

	// Find pull requests created within the last 3 months.
	filterPulls := bson.D{{Key: "createdat", Value: bson.D{{Key: "$gt", Value: time}}}}
	documents, err := mongodb.FindDocuments(client, db, "pulls", filterPulls, bson.D{}, 10)
	if err != nil {
		log.Printf("MongoDB: Switch4: List docs: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
	}

	if len(documents) > 0 {
		// Insert pull request documents into the test collection.
		_, err = mongodb.InsertManyDocuments(client, db, "pullsTest", documents)
		if err != nil && !mongo.IsDuplicateKeyError(err) {
			log.Printf("MongoDB: Switch4: Insert Many Docs: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}

		// Delete old documents from the test collection that are older than 3 months.
		filter_delete := bson.D{{Key: "createdat", Value: bson.D{{Key: "$lt", Value: time}}}}
		err = mongodb.DeleteDocuments(client, db, "pullsTest", filter_delete)
		if err != nil {
			log.Printf("MongoDB: Switch4: Delete old documents: Error: goroutine: %d: database: %s: message: %s", id, dbConfig["id"], err)
		}
	}
}
