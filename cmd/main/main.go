package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github-stat/internal"

	"github.com/google/go-github/github"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func main() {
	log.Print("Read config")

	envVars, err := internal.GetEnvVars()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx := context.Background()

	log.Print("GitHub API requests: Start")
	allRepos, counter, err := internal.FetchGitHubRepos(ctx, envVars.GitHub.Token, envVars.GitHub.Organisation)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("GitHub API requests: Finish")

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return MySQLprocess(ctx, envVars, allRepos, counter)
	})

	g.Go(func() error {
		return MongoDBprocess(ctx, envVars, allRepos, counter)
	})

	g.Go(func() error {
		return PostgreSQLprocess(ctx, envVars, allRepos, counter)
	})

	if err := g.Wait(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func MySQLprocess(ctx context.Context, envVars internal.EnvVars, allRepos []*github.Repository, counter int) error {

	report := internal.Report{
		Type:      "GitHub Repositories",
		StartedAt: time.Now().Format(time.RFC3339),
	}

	report.RequestsAPI = counter
	report.Count = len(allRepos)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		envVars.MySQL.User, envVars.MySQL.Password, envVars.MySQL.Host, envVars.MySQL.Port, envVars.MySQL.DB)

	log.Printf("MySQL: Connect to: %s", dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Print("MySQL: Start Insert")
	for i, repo := range allRepos {

		id := repo.ID
		repoJSON, err := json.Marshal(repo)
		if err != nil {
			return err
		}

		log.Printf("MySQL: Insert data, row: %d, repo: %s", i, *repo.FullName)

		_, err = db.Exec("INSERT INTO github.repositories (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?", id, repoJSON, repoJSON)
		if err != nil {
			return err
		}
	}

	log.Print("MySQL: Finish Insert")
	report.FinishedAt = time.Now().Format(time.RFC3339)

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Print("MySQL: Report")
	_, err = db.Exec("INSERT INTO github.report (data) VALUES (?)", reportJSON)
	if err != nil {
		return err
	}
	log.Printf("Finish MySQL: %s", reportJSON)

	return nil
}

func PostgreSQLprocess(ctx context.Context, envVars internal.EnvVars, allRepos []*github.Repository, counter int) error {
	report := internal.Report{
		Type:      "GitHub Repositories",
		StartedAt: time.Now().Format(time.RFC3339),
	}

	report.RequestsAPI = counter
	report.Count = len(allRepos)

	dsn := fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
		envVars.Postgres.User, envVars.Postgres.Password, envVars.Postgres.DB, envVars.Postgres.Host, envVars.Postgres.Port)

	log.Printf("PostgreSQL: Connect to: %s", dsn)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Print("PostgreSQL: Start Insert")
	for i, repo := range allRepos {
		id := repo.ID
		repoJSON, err := json.Marshal(repo)
		if err != nil {
			return err
		}

		log.Printf("PostgreSQL: Insert data, row: %d, repo: %s", i, *repo.FullName)

		_, err = db.Exec("INSERT INTO github.repositories (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2", id, repoJSON)
		if err != nil {
			return err
		}
	}
	log.Print("PostgreSQL: Finish Insert")

	report.FinishedAt = time.Now().Format(time.RFC3339)

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Print("PostgreSQL: Report")
	_, err = db.Exec("INSERT INTO github.report (data) VALUES ($1)", reportJSON)
	if err != nil {
		return err
	}
	log.Printf("Finish PostgreSQL: %s", reportJSON)

	return nil
}

func MongoDBprocess(ctx context.Context, envVars internal.EnvVars, allRepos []*github.Repository, counter int) error {
	report := internal.Report{
		Type:      "GitHub Repositories",
		StartedAt: time.Now().Format(time.RFC3339),
	}

	ApplyURI := fmt.Sprintf("mongodb://%s:%s@%s:%s/", envVars.MongoDB.User, envVars.MongoDB.Password, envVars.MongoDB.Host, envVars.MongoDB.Port)
	log.Printf("MongoDB: Connect to: %s", ApplyURI)
	clientOptions := options.Client().ApplyURI(ApplyURI)
	mongodb, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return err
	}
	defer mongodb.Disconnect(ctx)

	db := mongodb.Database(envVars.MongoDB.DB)

	dbCollectionRepos := db.Collection("repositories")

	log.Print("MongoDB: Start Insert")
	for i, repo := range allRepos {
		filter := bson.M{"id": repo.ID}
		update := bson.M{"$set": repo}

		log.Printf("MongoDB: Insert data, row: %d, repo: %s", i, *repo.FullName)
		_, err := dbCollectionRepos.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			return err
		}
	}
	log.Print("MongoDB: Finish Insert")

	report.FinishedAt = time.Now().Format(time.RFC3339)
	report.RequestsAPI = counter
	report.Count = len(allRepos)

	log.Print("MongoDB: Report")
	dbCollectionReport := db.Collection("report")
	report.FinishedAt = time.Now().Format(time.RFC3339)
	_, err = dbCollectionReport.InsertOne(ctx, report)
	if err != nil {
		return err
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Printf("Finish MongoDB: %s", reportJSON)

	return nil
}
