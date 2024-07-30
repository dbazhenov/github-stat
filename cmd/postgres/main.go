package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"

	"github-stat/internal"
)

func main() {
	log.Print("Read config")

	envVars, err := internal.GetEnvVars()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx := context.Background()

	report := internal.Report{
		Type:      "GitHub Repos",
		StartedAt: time.Now().Format(time.RFC3339),
	}

	log.Print("GitHub API requests: Start")
	allRepos, counter, err := internal.FetchGitHubRepos(ctx, envVars.GitHub.Token, envVars.GitHub.Organisation)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("GitHub API requests: Finish")
	report.RequestsAPI = counter
	report.Count = len(allRepos)

	dsn := fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
		envVars.Postgres.User, envVars.Postgres.Password, envVars.Postgres.DB, envVars.Postgres.Host, envVars.Postgres.Port)

	log.Printf("MySQL: Connect to: %s", dsn)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Print("PostgreSQL: Start Insert")
	for i, repo := range allRepos {

		id := repo.ID
		repoJSON, err := json.Marshal(repo)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("PostgreSQL: Insert data, row: %d, repo: %s", i, *repo.FullName)

		_, err = db.Exec("INSERT INTO github.repositories (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2", id, repoJSON)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Print("PostgreSQL: Finish Insert")

	report.FinishedAt = time.Now().Format(time.RFC3339)

	reportJSON, err := json.Marshal(report)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("PostgreSQL: Report")
	_, err = db.Exec("INSERT INTO github.report (data) VALUES ($1)", reportJSON)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Finish: %s", reportJSON)
}
