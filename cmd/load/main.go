package main

import (
	"context"
	"database/sql"
	"fmt"
	app "github-stat/internal"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"github-stat/internal/databases/valkey"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"
)

var EnvVars app.EnvVars

// Global variable to store the configuration. Default values can be set.
var LoadConfig = app.Load{
	MySQLConnections:      1,
	PostgreSQLConnections: 1,
	MongoDBConnections:    1,
}

func main() {

	// Get the configuration from environment variables or .env file.
	// app.Config - contains all environment variables
	app.InitConfig()

	// Valkey client (valkey.Valkey) initialization
	valkey.InitValkey(app.Config)
	defer valkey.Valkey.Close()

	for {
		// Getting settings from Valkey
		// The function will fill the global variable LoadConfig with data from Valkey. The control is performed in the Control Panel.
		checkLoadSetting()
		checkConnectSettings()

		// Running MySQL, PotgreSQL, MongoDB threads
		runDatabases()
	}
}

func checkConnectSettings() {

	log.Printf("Config: %v", app.Config)
}

func checkLoadSetting() {

	loadSettingFromValkey, err := valkey.LoadConfigFromValkey()
	if err != nil {
		log.Print("Valkey: Load Config: Empty")
	} else {
		LoadConfig = loadSettingFromValkey
		log.Printf("Valkey: Load Config: %v", LoadConfig)
	}

}

func runDatabases() {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	var wg sync.WaitGroup
	done := make(chan struct{})

	if app.Config.MySQL.Host != "" && LoadConfig.MySQLConnections > 0 {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return runMySQL()
		})
	}

	if app.Config.Postgres.Host != "" && LoadConfig.PostgreSQLConnections > 0 {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return runPostgreSQL()
		})
	}

	if app.Config.MongoDB.Host != "" && LoadConfig.MongoDBConnections > 0 {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return runMongoDB()
		})
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All database routines completed")
	case <-ctx.Done():
		log.Println("Context cancelled")
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func runMySQL() error {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)
	testTable := "loadpulls"
	var testDB *sql.DB
	var err error

	if LoadConfig.MySQLSwitch1 {

		testDB, err = mysql.Connect(app.Config)
		if err != nil {
			return err
		}

		err = mysql.CreateTable(testDB, testTable)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("MySQL: Database and table setup completed successfully!")

	}

	var wg sync.WaitGroup

	log.Printf("MySQL: Load Config: Connections: %d", LoadConfig.MySQLConnections)
	for i := 0; i < LoadConfig.MySQLConnections; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()

			db, err := mysql.Connect(app.Config)
			if err != nil {
				return err
			}
			defer db.Close()

			repo, _ := mysql.SelectString(db, `SELECT repo FROM github.pulls ORDER BY RAND() LIMIT 1;`)

			count_pulls, _ := mysql.SelectInt(db, `SELECT COUNT(*) FROM github.pulls;`)
			count_repositories, _ := mysql.SelectInt(db, `SELECT COUNT(*) FROM github.repositories;`)

			log.Printf("MySQL: Repo: %s, DB Counts: %d pulls, %d repos", repo, count_pulls, count_repositories)

			repos_with_pulls, _ := mysql.SelectListOfStrings(db, "SELECT DISTINCT repo FROM github.pulls;")
			log.Printf("MySQL: Pulls: Uniq Repos: %d", len(repos_with_pulls))

			pulls_ids, _ := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")
			log.Printf("MySQL: Pulls: Uniq IDs: %d", len(pulls_ids))

			if LoadConfig.MySQLSwitch1 {
				query := `SELECT data FROM github.pulls 
					WHERE STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(data, '$.created_at')), '%Y-%m-%dT%H:%i:%sZ') >= NOW() - INTERVAL 3 MONTH 
					LIMIT 10;`

				pullRequests, err := mysql.SelectPulls(db, query)
				if err != nil {
					return err
				}

				err = mysql.InsertPulls(db, pullRequests, testTable)
				if err != nil {
					return err
				}

				count_inserted, _ := mysql.SelectInt(db, fmt.Sprintf("SELECT COUNT(*) FROM github.%s;", testTable))

				log.Printf("MySQL: Switch result: Pulls count: %d, Iserted pulls: %d", len(pullRequests), count_inserted)

			}

			return nil
		})

	}

	wg.Wait()

	if LoadConfig.MySQLSwitch1 {
		result, err := mysql.DropTable(testDB, testTable)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("MySQL: Drop %s", result)

		testDB.Close()
	}

	return g.Wait()
}

func runPostgreSQL() error {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	testTable := "loadpulls"
	var testDB *sql.DB
	var err error

	if LoadConfig.PostgresSwitch1 {

		testDB, err = postgres.Connect(app.Config)
		if err != nil {
			return err
		}
		defer testDB.Close()

		err = postgres.CreateTable(testDB, testTable)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Postgres: Database and table setup completed successfully!")
	}

	var wg sync.WaitGroup

	log.Printf("Postgres: Load Config: Connections: %d", LoadConfig.PostgreSQLConnections)
	for i := 0; i < LoadConfig.PostgreSQLConnections; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()

			db, err := postgres.Connect(app.Config)
			if err != nil {
				return err
			}
			defer db.Close()

			repo, _ := postgres.SelectString(db, `SELECT repo FROM github.pulls ORDER BY RANDOM() LIMIT 1;`)

			count_pulls, _ := postgres.SelectInt(db, `SELECT COUNT(*) FROM github.pulls;`)
			count_repositories, _ := postgres.SelectInt(db, `SELECT COUNT(*) FROM github.repositories;`)

			log.Printf("PostgreSQL: Repo: %s, DB Counts: %d pulls, %d repos", repo, count_pulls, count_repositories)

			repos_with_pulls, _ := postgres.SelectListOfStrings(db, "SELECT DISTINCT repo FROM github.pulls;")
			log.Printf("PostgreSQL: Pulls: Uniq Repos: %d", len(repos_with_pulls))

			pulls_ids, _ := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")
			log.Printf("PostgreSQL: Pulls: Uniq IDs: %d", len(pulls_ids))

			if LoadConfig.PostgresSwitch1 {
				query := `
					SELECT data 
					FROM github.pulls 
					WHERE (to_timestamp((data->>'created_at')::text, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') >= NOW() - INTERVAL '3 months') 
					LIMIT 10;
				`
				pullRequests, err := postgres.SelectPulls(db, query)
				if err != nil {
					return err
				}

				err = postgres.InsertPulls(db, pullRequests, testTable)
				if err != nil {
					log.Printf("Log: %s", err)
					return err
				}

				count_inserted, _ := postgres.SelectInt(db, fmt.Sprintf("SELECT COUNT(*) FROM github.%s;", testTable))

				log.Printf("PostgreSQL: Switch result: Pulls count: %d, Iserted pulls: %d", len(pullRequests), count_inserted)
			}
			return nil

		})
	}

	wg.Wait()

	if LoadConfig.PostgresSwitch1 {
		result, err := postgres.DropTable(testDB, testTable)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("PostgreSQL: Drop %s", result)
	}

	return g.Wait()
}

func runMongoDB() error {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	var wg sync.WaitGroup

	log.Printf("MongoDB: Load Config: Connections: %d", LoadConfig.MongoDBConnections)
	for i := 0; i < LoadConfig.MongoDBConnections; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()

			mongo_ctx := context.Background()

			client, err := mongodb.Connect(app.Config, mongo_ctx)
			if err != nil {
				return err
			}
			defer client.Disconnect(mongo_ctx)

			countPulls, err := mongodb.CountDocuments(client, "github", "pulls", bson.D{})
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("MongoDB: Count of pulls: %d", countPulls)

			countRepositories, err := mongodb.CountDocuments(client, "github", "repositories", bson.D{})
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("MongoDB: Count of repositories: %d", countRepositories)

			filter := bson.D{{Key: "repo", Value: "community"}}
			pull, err := mongodb.FindOnePullRequest(client, "github", "pulls", filter, bson.D{})
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("MongoDB: Find One PullRequest: %s\n", pull.GetTitle())

			one_document, err := mongodb.FindOne(client, "github", "pulls", bson.D{}, bson.D{})
			if err != nil {
				log.Fatal(err)
			}

			document_field, err := GetNestedField(one_document, "base.repo.fullname")
			if err != nil {
				log.Printf("MongoDB: Find One Document: Error: %s", err)
			} else {
				log.Printf("MongoDB: Find One Document: %s", document_field)
			}

			random_document, err := mongodb.SelectRandomDocument(client, "github", "repositories")
			if err != nil {
				log.Fatal(err)
			}

			random_field, err := GetNestedField(random_document, "fullname")
			if err != nil {
				log.Printf("MongoDB: Random Document: Error: %s", err)
			} else {
				log.Printf("MongoDB: Random Document: %s", random_field)
			}

			sort := bson.D{{Key: "stargazerscount", Value: -1}}
			filter_doc := bson.D{}
			sorted_doc, err := mongodb.FindOne(client, "github", "repositories", filter_doc, sort)
			if err != nil {
				log.Fatal(err)
			}

			sorted_doc_field, err := GetNestedField(sorted_doc, "owner.login")
			if err != nil {
				log.Printf("MongoDB: Sorted Document: Error: %s", err)
			} else {
				log.Printf("MongoDB: Sorted Document: %s", sorted_doc_field)
			}

			sort_pr := bson.D{{Key: "createdat", Value: 1}}
			filter_pr := bson.D{
				{Key: "repo", Value: "community"},
			}
			prs, err := mongodb.FindPullRequests(client, "github", "pulls", filter_pr, sort_pr)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("MongoDB: Find Pull Requests: %d", len(prs))

			if LoadConfig.MongoDBSwitch1 {

				filter_repos := bson.D{}
				sort_repos := bson.D{{Key: "stargazerscount", Value: -1}}
				repos, err := mongodb.FindRepos(client, "github", "repositories", filter_repos, sort_repos)
				if err != nil {
					log.Fatal(err)
				}
				repo := repos[0]
				log.Printf("MongoDB: Find Repos: Count: %d, First %s, Stars: %d", len(repos), *repo.FullName, *repo.StargazersCount)

				insertOne, err := mongodb.InsertOneDoc(client, "github", "loadDocs", random_document)
				if err != nil {
					log.Printf("MongoDB: Insert One: Error: %s", err)
				} else {
					log.Printf("MongoDB: Inserted document with _id: %v", insertOne.InsertedID)
				}

				filterPulls := bson.D{{Key: "repo", Value: "community"}}
				listDocs, err := mongodb.FindDocuments(client, "github", "pulls", filterPulls, bson.D{}, 10)
				if err != nil {
					log.Printf("MongoDB: List docs: Error: %s", err)
				}
				log.Printf("MongoDB: List docs: %d", len(listDocs))

				insertMany, err := mongodb.InsertManyDocuments(client, "github", "loadDocs", listDocs)
				if err != nil {
					log.Printf("MongoDB: Insert Many Docs: Error: %s", err)
				} else {
					log.Printf("MongoDB: Inserted Many Docs with _id: %d", len(insertMany.InsertedIDs))
				}

				sixMonthsAgo := time.Now().AddDate(0, -6, 0)

				filter_delete := bson.D{{Key: "createdAt", Value: bson.D{{Key: "$lt", Value: sixMonthsAgo}}}}

				err = mongodb.DeleteDocuments(client, "github", "loadDocs", filter_delete)

				if err != nil {
					log.Printf("MongoDB: Delete old documents: Error: %s", err)
				} else {
					log.Printf("MongoDB: Old documents successfully deleted")
				}
			}

			return nil
		})
	}

	wg.Wait()

	if LoadConfig.MongoDBSwitch1 {
		drop_ctx := context.Background()

		drop_client, err := mongodb.Connect(app.Config, drop_ctx)
		if err != nil {
			return err
		}
		defer drop_client.Disconnect(drop_ctx)

		err = mongodb.DropCollection(drop_client, "github", "loadDocs")
		if err != nil {
			log.Printf("MongoDB: Drop collection: Error: %s", err)
		} else {
			log.Printf("MongoDB: Collection 'loadDocs' successfully dropped")
		}
	}

	return g.Wait()
}

func GetNestedField(data map[string]interface{}, fieldPath string) (interface{}, error) {
	fieldParts := strings.Split(fieldPath, ".")
	var value interface{} = data

	for _, part := range fieldParts {
		if v, ok := value.(map[string]interface{})[part]; ok {
			value = v
		} else {
			return nil, fmt.Errorf("field %s not found", fieldPath)
		}
	}
	return value, nil
}
