package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github-stat/internal"
)

func main() {
	log.Print("Read config")

	report := internal.Report{
		Type:      "GitHub Repos",
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

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		envVars.MySQL.User, envVars.MySQL.Password, envVars.MySQL.Host, envVars.MySQL.Port, envVars.MySQL.DB)

	log.Printf("MySQL: Connect to: %s", dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Print("MySQL: Start Insert")
	for i, repo := range allRepos {

		id := repo.ID
		repoJSON, err := json.Marshal(repo)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("MySQL: Insert data, row: %d, repo: %s", i, *repo.FullName)

		_, err = db.Exec("INSERT INTO github.repositories (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?", id, repoJSON, repoJSON)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Print("MySQL: Finish Insert")
	report.FinishedAt = time.Now().Format(time.RFC3339)

	reportJSON, err := json.Marshal(report)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("MySQL: Report")
	_, err = db.Exec("INSERT INTO github.report (data) VALUES (?)", reportJSON)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Finish: %s", reportJSON)
}
