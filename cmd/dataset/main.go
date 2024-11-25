package main

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	app "github-stat/internal"

	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"github-stat/internal/databases/valkey"

	"github.com/google/go-github/github"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// allPullsData stores pull request data for each repository.
// The outer map key is the repository name, and the inner map key is the pull request ID.
var allPullsData map[string]map[int64]*github.PullRequest = make(map[string]map[int64]*github.PullRequest)

// allReposData stores repository data.
// The map key is the repository ID.
var allReposData map[int64]*github.Repository = make(map[int64]*github.Repository)

// databases holds the configuration for each database.
var databases []map[string]string

// statusData holds the current status of the dataset update process.
var statusData string

// maxHeapAlloc keeps track of the maximum heap allocation observed.
var maxHeapAlloc uint64

// main is the entry point of the application. It initializes the configuration, starts the dataset update process,
// starts the process to update databases, and keeps the main function running indefinitely.
func main() {
	// Get the configuration from environment variables or .env file.
	app.InitConfig()

	// Initialize the Valkey client (valkey.Valkey)
	valkey.InitValkey(app.Config)
	defer valkey.Valkey.Close()

	// Initialize status to "Initializing"
	statusData = "Initializing"

	// Start the dataset data update process in a separate goroutine
	go updateDatasetData()

	// Start the process to update databases from Valkey in a separate goroutine
	go updateDatabases()

	// Keep the main function running
	select {}
}

// updateDatabases periodically checks and updates the status of databases from Valkey.
// It processes each database by writing data from memory into the database based on its type.
func updateDatabases() {
	for {
		// Wait until the status is no longer "Initializing"
		for statusData == "Initializing" {
			time.Sleep(1 * time.Second)
		}

		var err error
		databases, err = valkey.GetDatabases()
		if err != nil {
			log.Printf("Check Databases: Error: %v", err)
		} else {

			for _, db := range databases {
				if db["datasetStatus"] == "Waiting" {
					log.Printf("Processing Database ID: %s, Type: %s", db["id"], db["dbType"])

					// Set status to In Progress before starting the work
					err = updateDatabaseStatus(db["id"], "In Progress")
					if err != nil {
						continue
					}

					switch db["dbType"] {
					case "mysql":
						go func(db map[string]string) {

							err := writeDataFromMemoryToMySQL(db)

							if err != nil {
								log.Printf("MySQL process error: %v", err)
								updateDatabaseStatus(db["id"], "Error")
							} else {
								updateDatabaseStatus(db["id"], "Done")
							}
						}(db)

					case "postgres":
						go func(db map[string]string) {

							err := writeDataFromMemoryToPostgres(db)

							if err != nil {
								log.Printf("Postgres process error: %v", err)
								updateDatabaseStatus(db["id"], "Error")
							} else {
								updateDatabaseStatus(db["id"], "Done")
							}
						}(db)

					case "mongodb":
						go func(db map[string]string) {

							err := writeDataFromMemoryToMongoDB(db)

							if err != nil {
								log.Printf("MongoDB process error: %v", err)
								updateDatabaseStatus(db["id"], "Error")
							} else {
								updateDatabaseStatus(db["id"], "Done")
							}
						}(db)
					}
				}
			}

			log.Printf("Check Databases: Successfully updated databases")
		}

		time.Sleep(10 * time.Second)
	}
}

// updateStatus updates the dataset status of a given database in Valkey.
//
// Arguments:
//   - dbID: string containing the database ID.
//   - status: string containing the new status for the database.
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func updateDatabaseStatus(dbID, status string) error {
	fields := map[string]string{
		"datasetStatus": status,
	}

	err := valkey.AddDatabase(dbID, fields)
	if err != nil {
		log.Printf("Error updating status for database %s: %v", dbID, err)
		return err
	}

	log.Printf("Status for database %s updated to %s", dbID, status)
	return nil
}

// writeDataFromMemoryToPostgres imports repository and pull request data into a PostgreSQL database.
// It connects to the PostgreSQL database using the provided connection string,
// fetches the latest update times for each repository, and then updates the database
// with new or updated repositories and pull requests based on their last update time.
//
// Arguments:
//   - dbConfig: map[string]string containing the database configuration,
//     including the connection string under the key "connectionString".
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func writeDataFromMemoryToPostgres(dbConfig map[string]string) error {

	log.Printf("%s process start: %v", dbConfig["dbType"], dbConfig["id"])

	// Initialize the report for tracking the import process.
	report := app.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "PostgreSQL",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	// Connect to the PostgreSQL database.
	db, err := postgres.ConnectByString(dbConfig["connectionString"])
	if err != nil {
		log.Printf("Databases: PostgreSQL: Start: Error: %s", err)
		return err
	}
	defer db.Close()

	log.Printf("Databases: PostgreSQL: Start")

	// Get the latest update times for each repository from the PostgreSQL database.
	pullsLastUpdate, err := postgres.GetPullsLatestUpdates(dbConfig)
	if err != nil {
		log.Printf("Error getting latest updates: %v", err)
		return err
	}

	// Iterate over all repositories and update the database with new or updated repositories and pull requests.
	for _, repo := range allReposData {
		report.Counter.Repos++
		id := repo.ID
		repoJSON, err := json.Marshal(repo)
		if err != nil {
			return err
		}

		_, err = db.Exec("INSERT INTO github.repositories (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2", id, repoJSON)
		if err != nil {
			return err
		}

		if len(allPullsData) > 0 {
			repoName := *repo.Name
			pullRequests, exists := allPullsData[repoName]
			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
			} else {
				report.Counter.ReposWithPRs++

				// Get the last update time for the current repository.
				pullLastUpdate := pullsLastUpdate[repoName]
				var lastUpdatedTime time.Time
				if pullLastUpdate != "" {
					lastUpdatedTime, err = time.Parse(time.RFC3339, pullLastUpdate)
					if err != nil {
						log.Printf("Error: PostgreSQL: lastUpdatedTime: parsing startedAt: %v", err)
						lastUpdatedTime = time.Time{} // Reset lastUpdatedTime in case of error
					}
				}

				// Iterate over all pull requests and update the database with new or updated pull requests.
				for _, pull := range pullRequests {
					// Skip processing if lastUpdatedTime is not empty and the pull request is older
					if !lastUpdatedTime.IsZero() && pull.UpdatedAt != nil && lastUpdatedTime.After(*pull.UpdatedAt) {
						continue
					}

					id := pull.ID
					pullJSON, err := json.Marshal(pull)
					if err != nil {
						return err
					}

					res, err := db.Exec("INSERT INTO github.pulls (id, repo, data) VALUES ($1, $2, $3) ON CONFLICT (id, repo) DO UPDATE SET data = $3", id, *repo.Name, pullJSON)
					if err != nil {
						return err
					}

					// Check if the row has been updated or inserted.
					rowsAffected, err := res.RowsAffected()
					if err != nil {
						return err
					}

					if rowsAffected == 1 {
						report.Counter.PullsInserted++
					} else {
						report.Counter.PullsUpdated++
					}
				}

				report.Counter.Pulls += len(pullRequests)
			}
		}
	}

	// Finalize the report with end times and total duration.
	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.TotalMilli = report.FinishedAtUnix - report.StartedAtUnix
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Printf("Datasets: PostgreSQL: Finish: Report: %s", reportJSON)
	_, err = db.Exec("INSERT INTO github.reports_dataset (data) VALUES ($1)", reportJSON)
	if err != nil {
		return err
	}

	log.Printf("%s process complete for database ID: %v", dbConfig["dbType"], dbConfig["id"])

	return nil
}

// writeDataFromMemoryToMySQL imports repository and pull request data into a MySQL database.
// It connects to the MySQL database using the provided connection string,
// fetches the latest update times for each repository, and then updates the database
// with new or updated repositories and pull requests based on their last update time.
//
// Arguments:
//   - dbConfig: map[string]string containing the database configuration,
//     including the connection string under the key "connectionString".
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func writeDataFromMemoryToMySQL(dbConfig map[string]string) error {
	log.Printf("%s process start: %v", dbConfig["dbType"], dbConfig["id"])

	// Initialize the report for tracking the import process.
	report := app.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "MySQL",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	// Connect to the MySQL database.
	db, err := mysql.ConnectByString(dbConfig["connectionString"])
	if err != nil {
		log.Printf("Databases: MySQL: Error: message: %s", err)
		return err
	}
	defer db.Close()

	log.Printf("Databases: MySQL: Start")

	// Get the latest update times for each repository from the MySQL database.
	pullsLastUpdate, err := mysql.GetPullsLatestUpdates(dbConfig)
	if err != nil {
		log.Printf("Error getting latest updates: %v", err)
		return err
	}

	// Iterate over all repositories and update the database with new or updated repositories and pull requests.
	for _, repo := range allReposData {
		report.Counter.Repos++
		id := repo.ID
		repoJSON, err := json.Marshal(repo)
		if err != nil {
			return err
		}

		_, err = db.Exec("INSERT INTO repositories (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?", id, repoJSON, repoJSON)
		if err != nil {
			return err
		}

		if len(allPullsData) > 0 {
			repoName := *repo.Name

			pullRequests, exists := allPullsData[repoName]
			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
			} else {
				report.Counter.ReposWithPRs++

				// Get the last update time for the current repository.
				pullLastUpdate := pullsLastUpdate[repoName]
				var lastUpdatedTime time.Time
				if pullLastUpdate != "" {
					lastUpdatedTime, err = time.Parse(time.RFC3339, pullLastUpdate)
					if err != nil {
						log.Printf("Error parsing startedAt: %v", err)
						lastUpdatedTime = time.Time{} // Reset lastUpdatedTime in case of error
					}
				}

				// Iterate over all pull requests and update the database with new or updated pull requests.
				for _, pull := range pullRequests {
					// Skip processing if lastUpdatedTime is not empty and the pull request is older
					if !lastUpdatedTime.IsZero() && pull.UpdatedAt != nil && lastUpdatedTime.After(*pull.UpdatedAt) {
						continue
					}

					id := pull.ID
					pullJSON, err := json.Marshal(pull)
					if err != nil {
						return err
					}

					res, err := db.Exec("INSERT INTO pulls (id, repo, data) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE data = ?", id, *repo.Name, pullJSON, pullJSON)
					if err != nil {
						return err
					}

					rowsAffected, err := res.RowsAffected()
					if err != nil {
						return err
					}

					if rowsAffected == 1 {
						report.Counter.PullsInserted++
					} else if rowsAffected == 2 {
						report.Counter.PullsUpdated++
					}
				}

				report.Counter.Pulls += len(pullRequests)
			}
		}
	}

	// Finalize the report with end times and total duration.
	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.TotalMilli = report.FinishedAtUnix - report.StartedAtUnix

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Printf("Databases: MySQL: Finish: Report: %s", reportJSON)
	_, err = db.Exec("INSERT INTO reports_dataset (data) VALUES (?)", reportJSON)
	if err != nil {
		return err
	}

	log.Printf("%s process complete for database ID: %v", dbConfig["dbType"], dbConfig["id"])

	return nil
}

// writeDataFromMemoryToMongoDB imports repository and pull request data into a MongoDB database.
// It connects to the MongoDB database using the provided connection string,
// fetches the latest update times for each repository, and then updates the database
// with new or updated repositories and pull requests based on their last update time.
//
// Arguments:
//   - dbConfig: map[string]string containing the database configuration,
//     including the connection string under the key "connectionString".
//
// Returns:
//   - error: An error object if an error occurs, otherwise nil.
func writeDataFromMemoryToMongoDB(dbConfig map[string]string) error {
	log.Printf("%s process start: %v", dbConfig["dbType"], dbConfig["id"])

	// Initialize the report for tracking the import process.
	report := app.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "MongoDB",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	ctx := context.Background()

	// Connect to the MongoDB database.
	client, err := mongodb.ConnectByString(dbConfig["connectionString"], ctx)
	if err != nil {
		log.Printf("MongoDB: Connect Error: message: %s", err)
		return err
	}
	defer client.Disconnect(ctx)

	log.Printf("Databases: MongoDB: Start")

	db := client.Database(dbConfig["database"])
	dbCollectionRepos := db.Collection("repositories")
	dbCollectionPulls := db.Collection("pulls")

	// Create an index by id and repo fields
	indexKeys := bson.D{
		{Key: "id", Value: 1},
		{Key: "repo", Value: 1},
	}
	if err := mongodb.CreateIndex(client, dbConfig["database"], "pulls", indexKeys, true); err != nil {
		log.Printf("Error: MongoDB: %s: CreateIndex: %v", dbConfig["id"], err)
		return err
	}

	// Get the latest update times for each repository from the MongoDB database.
	pullsLastUpdate, err := mongodb.GetPullsLatestUpdates(dbConfig)
	if err != nil {
		log.Printf("Error getting latest updates: %v", err)
		return err
	}

	// Iterate over all repositories and update the database with new or updated repositories and pull requests.
	for _, repo := range allReposData {
		report.Counter.Repos++
		filter := bson.M{"id": repo.ID}
		update := bson.M{"$set": repo}

		_, err := dbCollectionRepos.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			return err
		}

		if len(allPullsData) > 0 {
			repoName := *repo.Name

			pullRequests, exists := allPullsData[repoName]
			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
			} else {
				report.Counter.ReposWithPRs++

				// Get the last update time for the current repository.
				pullLastUpdate := pullsLastUpdate[repoName]
				var lastUpdatedTime time.Time
				if pullLastUpdate != "" {
					lastUpdatedTime, err = time.Parse(time.RFC3339, pullLastUpdate)
					if err != nil {
						log.Printf("Error parsing startedAt: %v", err)
						lastUpdatedTime = time.Time{} // Reset lastUpdatedTime in case of error
					}
				}

				// Iterate over all pull requests and update the database with new or updated pull requests.
				for _, pull := range pullRequests {
					// Skip processing if lastUpdatedTime is not empty and the pull request is older
					if !lastUpdatedTime.IsZero() && pull.UpdatedAt != nil && lastUpdatedTime.After(*pull.UpdatedAt) {
						continue
					}

					filter := bson.M{"id": pull.ID, "repo": repoName}
					update := bson.M{"$set": pull}

					res, err := dbCollectionPulls.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
					if err != nil {
						return err
					}
					if res.UpsertedCount > 0 {
						report.Counter.PullsInserted++
					} else if res.MatchedCount > 0 {
						report.Counter.PullsUpdated++
					}
				}

				report.Counter.Pulls += len(pullRequests)
			}
		}
	}

	// Finalize the report with end times and total duration.
	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.TotalMilli = report.FinishedAtUnix - report.StartedAtUnix

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Printf("Databases: MongoDB: Finish: Report: %s", reportJSON)
	dbCollectionReport := db.Collection("reports_dataset")
	_, err = dbCollectionReport.InsertOne(ctx, report)
	if err != nil {
		return err
	}

	log.Printf("%s process complete for database ID: %v", dbConfig["dbType"], dbConfig["id"])
	return nil
}

// updateDatasetData continuously updates dataset data by importing from GitHub API or CSV files into memory.
// It logs memory usage and waits for a specified delay before the next import cycle.
func updateDatasetData() {
	for {
		log.Printf("updateDatasetData: Start import: Type %v", app.Config.App.DatasetLoadType)

		// The main process of getting data from GitHub API and storing it into memory.
		if app.Config.App.DatasetLoadType == "github" {
			importGitHubToMemory(app.Config)
		} else {
			importCSVToMemory(app.Config)
		}

		log.Printf("updateDatasetData: Finish Import: Type %v", app.Config.App.DatasetLoadType)

		// Log current memory usage
		logMemoryUsage("updateDatasetData")

		// Delay before the next start (Defined by the DELAY_MINUTES parameter)
		helperSleep(app.Config)
		app.InitConfig()
	}
}

// importGitHubToMemory imports data from GitHub repositories and stores it in memory.
// It fetches all repositories and their pull requests, then updates global data structures.
func importGitHubToMemory(envVars app.EnvVars) {
	if statusData == "Done" {
		statusData = "In Progress"
		log.Printf("importGitHubData: Status: %s", statusData)
	}

	report := helperReportStart()

	// Get all the repositories of the organization.
	allRepos, counterReposApi, err := app.FetchGitHubRepos(envVars)
	if err != nil {
		log.Printf("Error: FetchGitHubRepos: %v", err)
	}

	report.Timer["ApiRepos"] = time.Now().UnixMilli()

	// For DEBUG mode. Keep only 4 repositories out of many to speed up the complete process.
	allRepos = filterRepos(envVars, allRepos)

	counter := map[string]*int{
		"pulls_api_requests": new(int),
		"pulls":              new(int),
		"pulls_full":         new(int),
		"pulls_latest":       new(int),
		"repos":              new(int),
		"repos_full":         new(int),
		"repos_latest":       new(int),
	}

	if envVars.GitHub.Token != "" {
		log.Printf("Check Latest Updates: Start")
		// Get the latest Pull Requests updates to download only the new ones. Will download all Pull Requests on the first run.
		pullsLastUpdate, err := getLatestUpdatesFromData(allRepos)
		if err != nil {
			log.Printf("getLatestUpdates: %v", err)
		}

		report.Timer["DBLatestUpdates"] = time.Now().UnixMilli()

		// Get Pull Requests for all repositories.
		for _, repo := range allRepos {
			log.Printf("GitHub API: Start: Repo: %s", *repo.Name)

			repoID := repo.GetID()
			repoName := repo.GetName()

			var allPulls []*github.PullRequest

			allPulls, err = app.FetchGitHubPullsByRepo(envVars, repo, pullsLastUpdate, counter)
			if err != nil {
				log.Printf("FetchGitHubPullsByRepos: %v", err)
			}

			allReposData[repoID] = repo

			allPullsData[repoName] = make(map[int64]*github.PullRequest)

			for _, pull := range allPulls {
				allPullsData[repoName][pull.GetID()] = pull
			}

			if statusData == "Initializing" && len(allPullsData) > 20 {
				statusData = "In Progress"
				log.Printf("importGitHubData: Status: %s", statusData)
			}
		}

		report.Timer["ApiPulls"] = time.Now().UnixMilli()
	}

	counterMap := make(map[string]int)
	for k, v := range counter {
		counterMap[k] = *v
	}
	counterMap["repos_api_requests"] = counterReposApi

	statusData = "Done"

	helperReportFinish(envVars, report, counterMap)
}

// getLatestUpdatesFromData retrieves the latest update times for each repository from the in-memory data.
// It iterates over all repositories and calculates the latest update time for each repository.
//
// Arguments:
//   - allRepos: []*github.Repository containing all repositories.
//
// Returns:
//   - map[string]*app.PullsLastUpdate: A map where keys are repository names and values are the latest update times.
//   - error: An error object if an error occurs, otherwise nil.
func getLatestUpdatesFromData(allRepos []*github.Repository) (map[string]*app.PullsLastUpdate, error) {
	lastUpdates := make(map[string]*app.PullsLastUpdate)

	for _, repo := range allRepos {
		repoName := repo.GetName()
		lastUpdates[repoName] = &app.PullsLastUpdate{}

		if pulls, ok := allPullsData[repoName]; ok {
			var lastUpdateTime time.Time
			for _, pull := range pulls {
				if pull.UpdatedAt != nil && pull.UpdatedAt.After(lastUpdateTime) {
					lastUpdateTime = *pull.UpdatedAt
				}
			}
			lastUpdates[repoName].Minimum = lastUpdateTime.Format(time.RFC3339)
		} else {
			lastUpdates[repoName].Force = true
		}
	}

	return lastUpdates, nil
}

// importCSVToMemory imports data from CSV files and stores it in memory.
// It reads repository and pull request data from CSV files and updates global data structures.
func importCSVToMemory(envVars app.EnvVars) error {
	report := helperReportStart()

	if statusData == "Done" {
		statusData = "In Progress"
	}

	allRepos, err := getReposCSV(envVars)
	if err != nil {
		log.Printf("importCSVToDB: Repos: Error: %v", err)
		return err
	}

	report.Timer["allRepos"] = time.Now().UnixMilli()

	allPulls, err := getPullsCSV(envVars)
	if err != nil {
		log.Printf("importCSVToDB: Pulls: Error: %v", err)
		return err
	}

	report.Timer["allPulls"] = time.Now().UnixMilli()

	allPullsData = make(map[string]map[int64]*github.PullRequest)
	for _, pull := range allPulls {
		repoName := pull.Base.Repo.GetName()
		if allPullsData[repoName] == nil {
			allPullsData[repoName] = make(map[int64]*github.PullRequest)
		}
		allPullsData[repoName][pull.GetID()] = pull
	}

	allReposData = make(map[int64]*github.Repository)
	for _, repo := range allRepos {
		repoID := repo.GetID()
		allReposData[repoID] = repo
	}

	statusData = "Done"

	counter := map[string]int{
		"pulls": len(allPulls),
		"repos": len(allRepos),
	}

	helperReportFinish(envVars, report, counter)

	return nil
}

func getPullsCSV(envVars app.EnvVars) ([]*github.PullRequest, error) {
	filePath := envVars.App.DatasetDemoPulls

	if strings.HasPrefix(filePath, "http://") || strings.HasPrefix(filePath, "https://") {
		log.Printf("getPullsCSV: Downloading file from URL: %s", filePath)
		tempFile, err := downloadFileFromURL(filePath)
		if err != nil {
			log.Printf("getPullsCSV: DownloadFileFromURL: Error: %v", err)
			return nil, err
		}
		defer os.Remove(tempFile)
		filePath = tempFile
	}

	var allPulls []*github.PullRequest

	ext := strings.ToLower(filepath.Ext(filePath))
	if ext == ".zip" {
		zipFile, err := zip.OpenReader(filePath)
		if err != nil {
			log.Printf("getPullsCSV: Open ZIP: Error: %v", err)
			return nil, err
		}
		defer zipFile.Close()

		for _, f := range zipFile.File {
			if !strings.HasSuffix(f.Name, ".csv") {
				continue
			}

			rc, err := f.Open()
			if err != nil {
				log.Printf("getPullsCSV: Open CSV in ZIP: Error: %v", err)
				return nil, err
			}
			defer rc.Close()

			reader := csv.NewReader(rc)
			reader.LazyQuotes = true
			records, err := reader.ReadAll()
			if err != nil {
				log.Printf("getPullsCSV: ZIP: CSV: ReadAll: Error: %v", err)
				return nil, err
			}

			allPulls, err = processPullsRecords(records, allPulls)
			if err != nil {
				return nil, err
			}
		}
	} else if ext == ".csv" {
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("getPullsCSV: Open CSV: Error: %v", err)
			return nil, err
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.LazyQuotes = true
		records, err := reader.ReadAll()
		if err != nil {
			log.Printf("getPullsCSV: ReadAll: CSV: Error: %v", err)
			return nil, err
		}

		allPulls, err = processPullsRecords(records, allPulls)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("getPullsCSV: unsupported file type: %s", ext)
	}

	return allPulls, nil
}

func processPullsRecords(records [][]string, allPulls []*github.PullRequest) ([]*github.PullRequest, error) {
	for i, record := range records {
		if i == 0 {
			continue
		}

		data := record[2]

		var pullRequest github.PullRequest

		err := json.Unmarshal([]byte(data), &pullRequest)
		if err != nil {
			log.Printf("processPullsRecords: Unmarshal: Error: %v", err)
			return nil, err
		}

		allPulls = append(allPulls, &pullRequest)
	}
	return allPulls, nil
}

func getReposCSV(envVars app.EnvVars) ([]*github.Repository, error) {
	filePath := envVars.App.DatasetDemoRepos

	if strings.HasPrefix(filePath, "http://") || strings.HasPrefix(filePath, "https://") {
		log.Printf("getReposCSV: Downloading file from URL: %s", filePath)
		tempFile, err := downloadFileFromURL(filePath)
		if err != nil {
			log.Printf("getReposCSV: DownloadFileFromURL: Error: %v", err)
			return nil, err
		}
		defer os.Remove(tempFile)
		filePath = tempFile
	}

	var allRepos []*github.Repository

	ext := strings.ToLower(filepath.Ext(filePath))
	if ext == ".zip" {
		zipFile, err := zip.OpenReader(filePath)
		if err != nil {
			log.Printf("getReposCSV: Open ZIP: Error: %v", err)
			return nil, err
		}
		defer zipFile.Close()

		for _, f := range zipFile.File {
			if !strings.HasSuffix(f.Name, ".csv") {
				continue
			}

			rc, err := f.Open()
			if err != nil {
				log.Printf("getReposCSV: Open CSV in ZIP: Error: %v", err)
				return nil, err
			}
			defer rc.Close()

			reader := csv.NewReader(rc)
			reader.LazyQuotes = true
			records, err := reader.ReadAll()
			if err != nil {
				log.Printf("getReposCSV: ReadAll: Error: %v", err)
				return nil, err
			}

			allRepos, err = processRepoRecords(records, allRepos)
			if err != nil {
				return nil, err
			}
		}
	} else if ext == ".csv" {
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("getReposCSV: Open CSV: Error: %v", err)
			return nil, err
		}
		defer file.Close()

		reader := csv.NewReader(file)
		reader.LazyQuotes = true
		records, err := reader.ReadAll()
		if err != nil {
			log.Printf("getReposCSV: ReadAll: Error: %v", err)
			return nil, err
		}

		allRepos, err = processRepoRecords(records, allRepos)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("getReposCSV: unsupported file type: %s", ext)
	}

	return allRepos, nil
}

func processRepoRecords(records [][]string, allRepos []*github.Repository) ([]*github.Repository, error) {
	for i, record := range records {
		if i == 0 {
			continue
		}

		data := record[1]

		var repo github.Repository

		err := json.Unmarshal([]byte(data), &repo)
		if err != nil {
			log.Printf("processRepoRecords: Unmarshal: Error: %v", err)
			return nil, err
		}

		allRepos = append(allRepos, &repo)
	}
	return allRepos, nil
}

// downloadFileFromURL downloads a file from the given URL and saves it to a temporary file.
// It returns the path to the downloaded file.
//
// Arguments:
//   - url: string containing the URL of the file to download.
//
// Returns:
//   - string: The path to the downloaded file.
//   - error: An error object if an error occurs, otherwise nil.
func downloadFileFromURL(url string) (string, error) {

	// Extract the file extension from the URL.
	ext := filepath.Ext(url)

	// Create a temporary file to save the downloaded content.
	tmpFile, err := os.CreateTemp("", "dataset-*"+ext)
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	// Send a GET request to the URL.
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Copy the response body to the temporary file.
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", err
	}

	// Return the path to the downloaded file.
	return tmpFile.Name(), nil
}

// filterRepos filters the given list of repositories based on a predefined set of included repositories.
// It returns the filtered list of repositories if the application is in debug mode.
//
// Arguments:
//   - envVars: app.EnvVars containing environment variables and configuration.
//   - allRepos: []*github.Repository containing all repositories.
//
// Returns:
//   - []*github.Repository: The filtered list of repositories.
func filterRepos(envVars app.EnvVars, allRepos []*github.Repository) []*github.Repository {

	// Define the set of included repositories.
	includedRepos := map[string]bool{
		"pxc-docs":           true,
		"ivee-docs":          true,
		"percona-valkey-doc": true,
		"ab":                 true,
		"documentation":      true,
		"pg_tde":             true,
		"postgres_exporter":  true,
		"pg_stat_monitor":    true,
		"roadmap":            true,
		"everest-catalog":    true,
		"openstack_ansible":  true,
		"go-mysql":           true,
		"rds_exporter":       true,
		"postgres":           true,
		"awesome-pmm":        true,
		"pmm-demo":           true,
		"qan-api":            true,
		"percona-toolkit":    true,
		"mongodb_exporter":   true,
		"everest":            true,
		"community":          true,
		"rocksdb":            true,
		"mysql-wsrep":        true,
		"vagrant-fabric":     true,
		"debian":             true,
		"jemalloc":           true,
	}

	var allReposFiltered []*github.Repository
	// Filter repositories if the application is in debug mode.
	if envVars.App.Debug {
		for _, repo := range allRepos {
			if includedRepos[*repo.Name] {
				allReposFiltered = append(allReposFiltered, repo)
			}
		}

		return allReposFiltered
	}

	return allRepos
}

// helperSleep pauses execution for a specified number of minutes.
// The duration is defined by the DelayMinutes parameter in the environment variables.
//
// Arguments:
//   - envVars: app.EnvVars containing environment variables and configuration.
func helperSleep(envVars app.EnvVars) {
	minutes := time.Duration(envVars.App.DelayMinutes) * time.Minute
	log.Printf("Dataset update memory data: The repeat run will start automatically after %v", minutes)
	time.Sleep(minutes)
}

// helperReportStart initializes and returns a new report with the current start time and Unix timestamp.
//
// Returns:
//   - app.Report: A new report with the current start time and Unix timestamp.
func helperReportStart() app.Report {

	report := app.Report{
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	report.Timer = make(map[string]int64)

	return report
}

// helperReportFinish finalizes the report with end times, total duration, and counter values.
// It saves the report to the database and logs the final report.
//
// Arguments:
//   - envVars: app.EnvVars containing environment variables and configuration.
//   - report: app.Report containing the report data.
//   - counter: map[string]int containing counter values for repositories and pull requests.
func helperReportFinish(envVars app.EnvVars, report app.Report, counter map[string]int) {

	report.Type = envVars.App.DatasetLoadType

	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.FullTime = time.Now().UnixMilli() - report.StartedAtUnix

	counterPullsJSON, _ := json.Marshal(counter)

	reportMap := map[string]interface{}{
		"Type":           report.Type,
		"StartedAtUnix":  report.StartedAtUnix,
		"StartedAt":      report.StartedAt,
		"FinishedAtUnix": report.FinishedAtUnix,
		"FinishedAt":     report.FinishedAt,
		"FullTimeMilli":  report.FullTime,
		"Repos":          counter["repos"],
		"Pulls":          counter["pulls"],
		"Counter":        string(counterPullsJSON),
	}

	startedAtTime, err := time.Parse("2006-01-02T15:04:05.000", report.StartedAt)
	if err != nil {
		log.Printf("Error parsing StartedAt: %v", err)
		return
	}

	reportID := startedAtTime.Format("20060102150405")
	if err := valkey.SaveReport(reportID, reportMap); err != nil {
		log.Printf("Error: helperReportFinish: %v", err)
	}

	log.Printf("Successfully completed: Final Report: %v", reportMap)
}

// logMemoryUsage logs the current memory usage statistics for the given name.
// It records the allocated memory, total allocated memory, system memory, heap allocated memory, number of garbage collections, and maximum heap allocated memory.
//
// Arguments:
//   - name: string containing the name of the process or function for which memory usage is logged.
func logMemoryUsage(name string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	alloc := bToMb(m.HeapAlloc)

	if alloc > maxHeapAlloc {
		maxHeapAlloc = alloc
	}

	log.Printf("%s: Memory Usage: Alloc = %v MiB, TotalAlloc = %v MiB, Sys = %v MiB, HeapAlloc = %v MiB, NumGC = %v, MaxHeapAlloc = %v MiB",
		name, bToMb(m.Alloc), bToMb(m.TotalAlloc), bToMb(m.Sys), alloc, m.NumGC, maxHeapAlloc)
}

// bToMb converts bytes to megabytes.
//
// Arguments:
//   - b: uint64 representing the number of bytes.
//
// Returns:
//   - uint64: The equivalent number of megabytes.
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
