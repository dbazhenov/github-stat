package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github-stat/internal"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	log.Print("Read config")
	report := internal.Report{
		Type:      "GitHub Repositories",
		StartedAt: time.Now().Format(time.RFC3339),
	}

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

	report.RequestsAPI = counter
	report.Count = len(allRepos)

	ApplyURI := fmt.Sprintf("mongodb://%s:%s@%s:%s/", envVars.MongoDB.User, envVars.MongoDB.Password, envVars.MongoDB.Host, envVars.MongoDB.Port)
	log.Printf("MongoDB: Connect to: %s", ApplyURI)
	clientOptions := options.Client().ApplyURI(ApplyURI)
	mongodb, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
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
			log.Fatal(err)
		}
	}
	log.Print("MongoDB: Finish Insert")

	report.FinishedAt = time.Now().Format(time.RFC3339)
	report.Count = len(allRepos)

	log.Print("MongoDB: Report")
	dbCollectionReport := db.Collection("report")
	report.FinishedAt = time.Now().Format(time.RFC3339)
	_, err = dbCollectionReport.InsertOne(ctx, report)
	if err != nil {
		log.Fatal(err)
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Finish: %s", reportJSON)
}
