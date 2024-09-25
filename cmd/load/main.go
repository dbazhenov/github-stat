package main

import (
	"context"
	"fmt"
	app "github-stat/internal"
	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"
)

type Load struct {
	Connections int
}

func main() {

	// Get the configuration from environment variables or .env file.
	envVars := app.GetConfig()

	for {

		connections := 2
		load := Load{
			Connections: connections,
		}

		log.Printf("envVars: %v", envVars)

		runDatabases(envVars, load)

	}
}

func runDatabases(envVars app.EnvVars, load Load) {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	var wg sync.WaitGroup
	done := make(chan struct{})

	if envVars.MySQL.Host != "" {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return runMySQL(envVars, load)
		})
	}

	if envVars.Postgres.Host != "" {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return runPostgreSQL(envVars, load)
		})
	}

	if envVars.MongoDB.Host != "" {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return runMongoDB(envVars, load)
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

func runMySQL(envVars app.EnvVars, load Load) error {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	db_table, err := mysql.Connect(envVars)
	if err != nil {
		return err
	}
	defer db_table.Close()

	testTable := "loadpulls"
	err = mysql.CreateTable(db_table, testTable)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("MySQL: Database and table setup completed successfully!")

	var wg sync.WaitGroup

	for i := 0; i < load.Connections; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()

			db, err := mysql.Connect(envVars)
			if err != nil {
				return err
			}
			defer db.Close()

			repo, _ := mysql.SelectString(db, `SELECT repo FROM github.pulls ORDER BY RAND() LIMIT 1;`)

			count_pulls, _ := mysql.SelectInt(db, `SELECT COUNT(*) FROM github.pulls;`)
			count_repositories, _ := mysql.SelectInt(db, `SELECT COUNT(*) FROM github.repositories;`)

			repos_with_pulls, _ := mysql.SelectListOfStrings(db, "SELECT DISTINCT repo FROM github.pulls;")
			log.Printf("MySQL: Pulls: Uniq Repos: %v\n", len(repos_with_pulls))

			pulls_ids, _ := mysql.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")
			log.Printf("MySQL: Pulls: Uniq IDs: %v\n", len(pulls_ids))

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

			log.Printf("MySQL: Connection result: Random Repo: %s, Repos count: %d, Pulls count: %d, Iserted pulls: %d", repo, count_repositories, count_pulls, count_inserted)

			return nil
		})

	}

	wg.Wait()

	result, err := mysql.DropTable(db_table, testTable)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("MySQL: Drop %s", result)

	return g.Wait()
}

func runPostgreSQL(envVars app.EnvVars, load Load) error {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	db_table, err := postgres.Connect(envVars)
	if err != nil {
		return err
	}
	defer db_table.Close()

	testTable := "loadpulls"
	err = postgres.CreateTable(db_table, testTable)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Postgres: Database and table setup completed successfully!")

	var wg sync.WaitGroup

	for i := 0; i < load.Connections; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()

			db, err := postgres.Connect(envVars)
			if err != nil {
				return err
			}
			defer db.Close()

			repo, _ := postgres.SelectString(db, `SELECT repo FROM github.pulls ORDER BY RANDOM() LIMIT 1;`)

			count_pulls, _ := postgres.SelectInt(db, `SELECT COUNT(*) FROM github.pulls;`)
			count_repositories, _ := postgres.SelectInt(db, `SELECT COUNT(*) FROM github.repositories;`)

			log.Printf("PostgreSQL: Integers: %d, %d", count_pulls, count_repositories)

			pulls_ids, _ := postgres.SelectListOfInt(db, "SELECT DISTINCT id FROM github.pulls;")
			log.Printf("PostgreSQL: Pulls: Uniq IDs: %v\n", len(pulls_ids))

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

			log.Printf("PostgreSQL: Connection result: Random Repo: %s, Repos count: %d, Pulls count: %d, Iserted pulls: %d", repo, count_repositories, count_pulls, count_inserted)

			return nil

		})
	}

	wg.Wait()

	result, err := postgres.DropTable(db_table, testTable)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("PostgreSQL: Drop %s", result)

	return g.Wait()
}

func runMongoDB(envVars app.EnvVars, load Load) error {

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	var wg sync.WaitGroup

	for i := 0; i < load.Connections; i++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()

			mongo_ctx := context.Background()

			client, err := mongodb.Connect(envVars, mongo_ctx)
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
			listDocs, err := mongodb.FindDocuments(client, "github", "pulls", filterPulls, bson.D{})
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

			err = mongodb.DeleteDocuments(client, "github", "loadPulls", filter_delete)

			if err != nil {
				log.Printf("MongoDB: Delete old documents: Error: %s", err)
			} else {
				log.Printf("MongoDB: Old documents successfully deleted")
			}

			return nil
		})
	}

	wg.Wait()

	drop_ctx := context.Background()

	drop_client, err := mongodb.Connect(envVars, drop_ctx)
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
