package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	app "github-stat/internal"

	"github-stat/internal/databases/mongodb"
	"github-stat/internal/databases/mysql"
	"github-stat/internal/databases/postgres"
	"github-stat/internal/databases/valkey"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/go-github/github"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

func main() {

	// Get the configuration from environment variables or .env file.
	// app.Config - contains all environment variables
	app.InitConfig()

	// Valkey client (valkey.Valkey) initialization
	valkey.InitValkey(app.Config)
	defer valkey.Valkey.Close()

	for {

		checkConnectSettings()

		// The main process of getting data from GitHub API and store it into MySQL, PostgreSQL, MongoDB databases.
		fetchGitHubData(app.Config)

		// Delay before the next start (Defined by the DELAY_MINUTES parameter)
		helperSleep(app.Config)
		app.InitConfig()
	}
}

func checkConnectSettings() {

	settings, _ := valkey.LoadFromValkey("db_connections")

	if settings.MongoDBConnectionString != "" {
		app.Config.MongoDB.ConnectionString = settings.MongoDBConnectionString
	} else {
		app.Config.MongoDB.ConnectionString = mongodb.GetConnectionString(app.Config)
	}

	if settings.MongoDBDatabase != "" {
		app.Config.MongoDB.DB = settings.MongoDBDatabase
	}

	if app.Config.MongoDB.ConnectionString != "" {
		app.Config.MongoDB.ConnectionStatus = mongodb.CheckMongoDB(app.Config.MongoDB.ConnectionString)
	}

	if settings.MySQLConnectionString != "" {
		app.Config.MySQL.ConnectionString = settings.MySQLConnectionString
	} else {
		app.Config.MySQL.ConnectionString = mysql.GetConnectionString(app.Config)
	}

	if app.Config.MySQL.ConnectionString != "" {
		app.Config.MySQL.ConnectionStatus = mysql.CheckMySQL(app.Config.MySQL.ConnectionString)
	}

	if settings.PostgresConnectionString != "" {
		app.Config.Postgres.ConnectionString = settings.PostgresConnectionString
	} else {
		app.Config.Postgres.ConnectionString = postgres.GetConnectionString(app.Config)
	}

	if app.Config.Postgres.ConnectionString != "" {
		app.Config.Postgres.ConnectionStatus = postgres.CheckPostgreSQL(app.Config.Postgres.ConnectionString)
	}

}

func fetchGitHubData(envVars app.EnvVars) {

	report := helperReportStart()

	// Get all the repositories of the organization.
	allRepos, counterRepos, err := app.FetchGitHubRepos(envVars)
	if err != nil {
		log.Printf("Error: FetchGitHubRepos: %v", err)
	}

	report.Timer["ApiRepos"] = time.Now().UnixMilli()

	// For DEBUG mode. Let's keep only 4 repositories out of many to speed up the complete process.
	allRepos = filterRepos(envVars, allRepos)

	var allPulls map[string][]*github.PullRequest
	var counterPulls map[string]int

	if envVars.GitHub.Token != "" {
		log.Printf("Check Latest Updates: Start")
		// Get the latest Pull Requests updates to download only the new ones. Will download all Pull Requests on the first run.
		pullsLastUpdate, err := getLatestUpdates(envVars, allRepos)
		if err != nil {
			log.Printf("getLatestUpdates: %v", err)
		}

		report.Timer["DBLatestUpdates"] = time.Now().UnixMilli()

		// Get Pull Requests for all repositories.
		allPulls, counterPulls, err = app.FetchGitHubPullsByRepos(envVars, allRepos, pullsLastUpdate)
		if err != nil {
			log.Printf("FetchGitHubPullsByRepos: %v", err)
		}

		report.Timer["ApiPulls"] = time.Now().UnixMilli()
	} else {
		allPulls = make(map[string][]*github.PullRequest)
		counterPulls = make(map[string]int)
	}

	// Asynchronous writing of repositories and Pull Requests to databases (MySQL, PostgreSQL, MongoDB)
	asyncProcessDBs(envVars, allRepos, allPulls)

	report.Timer["DBInsert"] = time.Now().UnixMilli()

	helperReportFinish(envVars, report, counterPulls, counterRepos)

}

func filterRepos(envVars app.EnvVars, allRepos []*github.Repository) []*github.Repository {

	// Dev Filter
	var allReposFiltered []*github.Repository
	if envVars.App.Debug {
		for _, repo := range allRepos {
			if *repo.Name != "pxc-docs" && *repo.Name != "ab" && *repo.Name != "documentation" && *repo.Name != "community" {
				continue
			}
			allReposFiltered = append(allReposFiltered, repo)
		}

		return allReposFiltered
	}

	return allRepos
}

func getLatestUpdates(envVars app.EnvVars, allRepos []*github.Repository) (map[string]*app.PullsLastUpdate, error) {
	lastUpdates := make(map[string]*app.PullsLastUpdate)

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	var updatedMySQL, updatedPostgres, updatedMongo map[string]string
	var errMySQL, errPostgres, errMongo error

	if envVars.MySQL.ConnectionStatus == "Connected" {
		g.Go(func() error {
			updatedMySQL, errMySQL = getLatestUpdatesFromMySQL(envVars)
			return errMySQL
		})
	}

	if envVars.Postgres.ConnectionStatus == "Connected" {
		g.Go(func() error {
			updatedPostgres, errPostgres = getLatestUpdatesFromPostgres(envVars)
			return errPostgres
		})
	}

	if envVars.MongoDB.ConnectionStatus == "Connected" {
		g.Go(func() error {
			updatedMongo, errMongo = getLatestUpdatesFromMongoDB(envVars)
			return errMongo
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	for _, repo := range allRepos {
		repoName := repo.GetName()
		if lastUpdates[repoName] == nil {
			lastUpdates[repoName] = &app.PullsLastUpdate{}
		}

		if updatedMySQL[repoName] != "" {
			lastUpdates[repoName].MySQL = updatedMySQL[repoName]
		} else if envVars.MySQL.ConnectionStatus == "Connected" {
			lastUpdates[repoName].Force = true
		}

		if updatedPostgres[repoName] != "" {
			lastUpdates[repoName].PostgreSQL = updatedPostgres[repoName]
		} else if envVars.Postgres.ConnectionStatus == "Connected" {
			lastUpdates[repoName].Force = true
		}

		if updatedMongo[repoName] != "" {
			lastUpdates[repoName].MongoDB = updatedMongo[repoName]
		} else if envVars.MongoDB.ConnectionStatus == "Connected" {
			lastUpdates[repoName].Force = true
		}

		lastUpdates[repoName].Minimum = findMinimumDate(
			lastUpdates[repoName].MySQL,
			lastUpdates[repoName].PostgreSQL,
			lastUpdates[repoName].MongoDB,
		)
	}

	return lastUpdates, nil
}

func asyncProcessDBs(envVars app.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) {

	ctx := context.Background()

	g, _ := errgroup.WithContext(ctx)

	if envVars.MySQL.ConnectionStatus == "Connected" {
		g.Go(func() error {
			return MySQLprocessPulls(envVars, allRepos, allPulls)
		})
	}

	if envVars.Postgres.ConnectionStatus == "Connected" {
		g.Go(func() error {
			return PostgreSQLprocessPulls(envVars, allRepos, allPulls)
		})
	}

	if envVars.MongoDB.ConnectionStatus == "Connected" {
		g.Go(func() error {
			return MongoDBprocessPulls(envVars, allRepos, allPulls)
		})
	}

	if err := g.Wait(); err != nil {
		log.Printf("Error: asyncProcessDBs: %v", err)
	}

}

func getLatestUpdatesFromMySQL(envVars app.EnvVars) (map[string]string, error) {
	ctx := context.Background()

	db, err := mysql.ConnectByString(envVars.MySQL.ConnectionString)
	if err != nil {
		log.Printf("MySQL: Error: message: %s", err)
	}
	defer db.Close()

	query := `
		SELECT
			repo,
			MAX(JSON_UNQUOTE(JSON_EXTRACT(data, '$.updated_at'))) AS updated_at
		FROM
			pulls
		GROUP BY
			repo
		ORDER BY
			updated_at DESC;
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lastUpdates := make(map[string]string)
	for rows.Next() {
		var repo string
		var updatedAt string
		if err := rows.Scan(&repo, &updatedAt); err != nil {
			return nil, err
		}
		lastUpdates[repo] = updatedAt
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return lastUpdates, nil
}

func getLatestUpdatesFromPostgres(envVars app.EnvVars) (map[string]string, error) {

	ctx := context.Background()

	db, err := postgres.ConnectByString(envVars.Postgres.ConnectionString)
	if err != nil {
		log.Printf("Check Pulls Latest Updates: PostgreSQL: Error: %s", err)
	}
	defer db.Close()

	query := `
		SELECT
			repo,
			MAX(data->>'updated_at') AS updated_at
		FROM
			github.pulls
		GROUP BY
			repo
		ORDER BY
			updated_at DESC;
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lastUpdates := make(map[string]string)
	for rows.Next() {
		var repo string
		var updatedAt string
		if err := rows.Scan(&repo, &updatedAt); err != nil {
			return nil, err
		}
		lastUpdates[repo] = updatedAt
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return lastUpdates, nil
}

func getLatestUpdatesFromMongoDB(envVars app.EnvVars) (map[string]string, error) {
	ctx := context.Background()

	client, err := mongodb.ConnectByString(envVars.MongoDB.ConnectionString, ctx)
	if err != nil {
		log.Printf("MongoDB: Connect Error: message: %s", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database(envVars.MongoDB.DB)
	collection := db.Collection("pulls")

	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "updatedat", Value: -1}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$repo"},
			{Key: "updatedat", Value: bson.D{{Key: "$first", Value: "$updatedat"}}},
		}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	lastUpdates := make(map[string]string)
	for cursor.Next(ctx) {
		var result struct {
			Repo      string    `bson:"_id"`
			UpdatedAt time.Time `bson:"updatedat"`
		}
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		lastUpdates[result.Repo] = result.UpdatedAt.UTC().Format(time.RFC3339)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	return lastUpdates, nil
}

func findMinimumDate(dates ...string) string {
	var minDate string
	for _, date := range dates {
		if date != "" && (minDate == "" || date < minDate) {
			minDate = date
		}
	}
	return minDate
}

func MySQLprocessPulls(envVars app.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) error {

	report := app.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "MySQL",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	db, err := mysql.ConnectByString(envVars.MySQL.ConnectionString)
	if err != nil {
		log.Printf("Databases: MySQL: Error: message: %s", err)
		return err
	}
	defer db.Close()

	log.Printf("Databases: MySQL: Start")

	for _, repo := range allRepos {
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
		// if envVars.App.Debug {
		// 	log.Printf("MySQL: Repo %s: Insert repo data", *repo.FullName)
		// }

		if len(allPulls) > 0 {
			repoName := *repo.Name
			pullRequests, exists := allPulls[repoName]

			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
				// if envVars.App.Debug {
				// 	log.Printf("MySQL: Repo: %s: PRs: No pull requests found for repository", repoName)
				// }
			} else {
				report.Counter.ReposWithPRs++
				for _, pull := range pullRequests {
					// if envVars.App.Debug {
					// 	log.Printf("MySQL: Repo: %s: PRs: Insert data row: %d, pull: %s", repoName, p, *pull.Title)
					// }
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
				// if envVars.App.Debug {
				// 	log.Printf("MySQL: Repo: %s: PRs: Completed: Total pull requests: %d", repoName, len(pullRequests))
				// }
			}
		}
	}

	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.TotalMilli = report.FinishedAtUnix - report.StartedAtUnix

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Printf("Databases: MySQL: Finish: Report: %s", reportJSON)
	_, err = db.Exec("INSERT INTO reports_databases (data) VALUES (?)", reportJSON)
	if err != nil {
		return err
	}

	return nil
}

func PostgreSQLprocessPulls(envVars app.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) error {

	report := app.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "PostgreSQL",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	db, err := postgres.ConnectByString(envVars.Postgres.ConnectionString)
	if err != nil {
		log.Printf("Databases: PostgreSQL: Start: Error: %s", err)
		return err
	}
	defer db.Close()

	log.Printf("Databases: PostgreSQL: Start")

	for _, repo := range allRepos {

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
		// if envVars.App.Debug {
		// 	log.Printf("Postgres: Repo %s: Insert repo data", *repo.FullName)
		// }

		if len(allPulls) > 0 {
			repoName := *repo.Name
			pullRequests, exists := allPulls[repoName]

			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
				// if envVars.App.Debug {
				// 	log.Printf("Postgres: Repo: %s: PRs: No pull requests found for repository", repoName)
				// }
			} else {
				report.Counter.ReposWithPRs++
				for _, pull := range pullRequests {

					// if envVars.App.Debug {
					// 	log.Printf("Postgres: Repo: %s: PRs: Insert data row: %d, pull: %s", repoName, p, *pull.Title)
					// }
					id := pull.ID
					pullJSON, err := json.Marshal(pull)
					if err != nil {
						return err
					}

					res, err := db.Exec("INSERT INTO github.pulls (id, repo, data) VALUES ($1, $2, $3) ON CONFLICT (id, repo) DO UPDATE SET data = $3", id, *repo.Name, pullJSON)
					if err != nil {
						return err
					}

					// Check the row has been updated or inserted.
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
				// if envVars.App.Debug {
				// 	log.Printf("Postgres: Repo: %s: PRs: Completed: Total pull requests: %d", repoName, len(pullRequests))
				// }
			}
		}
	}

	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.TotalMilli = report.FinishedAtUnix - report.StartedAtUnix
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Printf("Databases: PostgreSQL: Finish: Report: %s", reportJSON)
	_, err = db.Exec("INSERT INTO github.reports_databases (data) VALUES ($1)", reportJSON)
	if err != nil {
		return err
	}

	return nil
}

func MongoDBprocessPulls(envVars app.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) error {

	report := app.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "MongoDB",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	ctx := context.Background()

	client, err := mongodb.ConnectByString(envVars.MongoDB.ConnectionString, ctx)
	if err != nil {
		log.Printf("MongoDB: Connect Error: message: %s", err)
		return err
	}
	defer client.Disconnect(ctx)

	log.Printf("Databases: MongoDB: Start")

	db := client.Database(envVars.MongoDB.DB)
	dbCollectionRepos := db.Collection("repositories")

	// Creating a collection of pulls
	dbCollectionPulls := db.Collection("pulls")

	// Create an index by id and repo fields
	indexModel := mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
			{Key: "repo", Value: 1},
		},
		Options: options.Index().SetUnique(true),
	}

	_, err = dbCollectionPulls.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return err
	}

	for _, repo := range allRepos {

		report.Counter.Repos++
		filter := bson.M{"id": repo.ID}
		update := bson.M{"$set": repo}

		_, err := dbCollectionRepos.UpdateOne(ctx, filter, update, options.Update().SetUpsert(true))
		if err != nil {
			return err
		}
		// if envVars.App.Debug {
		// 	log.Printf("MongoDB: Repo %s: Insert repo data", *repo.FullName)
		// }
		if len(allPulls) > 0 {
			dbCollectionPulls := db.Collection("pulls")
			repoName := *repo.Name
			pullRequests, exists := allPulls[repoName]

			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
				// if envVars.App.Debug {
				// 	log.Printf("MongoDB: Repo: %s: PRs: No pull requests found for repository", repoName)
				// }
			} else {
				report.Counter.ReposWithPRs++
				for _, pull := range pullRequests {

					filter := bson.M{"id": pull.ID, "repo": repoName}
					update := bson.M{"$set": pull}

					// if envVars.App.Debug {
					// 	log.Printf("MongoDB: Repo: %s: PRs: Insert data row: %d, pull: %s", repoName, p, *pull.Title)
					// }

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
				// if envVars.App.Debug {
				// 	log.Printf("MongoDB: Repo: %s: PRs: Completed: Total pull requests: %d", repoName, len(pullRequests))
				// }
			}
		}
	}

	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.TotalMilli = report.FinishedAtUnix - report.StartedAtUnix

	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	log.Printf("Databases: MongoDB: Finish: Report: %s", reportJSON)
	dbCollectionReport := db.Collection("reports_databases")
	_, err = dbCollectionReport.InsertOne(ctx, report)
	if err != nil {
		return err
	}

	return nil
}

func helperSleep(envVars app.EnvVars) {
	minutes := time.Duration(envVars.App.DelayMinutes) * time.Minute
	log.Printf("App: The repeat run will start automatically after %v", minutes)
	time.Sleep(minutes)
}

func helperReportStart() app.Report {

	report := app.Report{
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	report.Timer = make(map[string]int64)

	return report
}

func helperReportFinish(envVars app.EnvVars, report app.Report, counterPulls map[string]int, counterRepos int) {

	report.Timer["ApiReposTime"] = report.Timer["ApiRepos"] - report.StartedAtUnix

	if envVars.GitHub.Token != "" {
		report.Type = "Full"
		report.Timer["DBLatestUpdatesTime"] = report.Timer["DBLatestUpdates"] - report.Timer["ApiRepos"]
		report.Timer["ApiPullsTime"] = report.Timer["ApiPulls"] - report.Timer["DBLatestUpdates"]
		report.Timer["DBInsertTime"] = report.Timer["DBInsert"] - report.Timer["ApiPulls"]
	} else {
		report.Type = "Repos"
		report.Timer["DBInsertTime"] = report.Timer["DBInsert"] - report.Timer["ApiRepos"]
	}

	report.FinishedAt = time.Now().Format("2006-01-02T15:04:05.000")
	report.FinishedAtUnix = time.Now().UnixMilli()
	report.FullTime = time.Now().UnixMilli() - report.StartedAtUnix
	report.Counter = counterPulls
	report.Counter["repos_api_requests"] = counterRepos
	report.Databases = make(map[string]bool)
	report.Databases["MySQL"] = envVars.MySQL.ConnectionStatus != ""
	report.Databases["Postgres"] = envVars.Postgres.ConnectionStatus != ""
	report.Databases["MongoDB"] = envVars.MongoDB.ConnectionStatus != ""

	reportJSON, _ := json.Marshal(report)

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	if envVars.MySQL.ConnectionStatus == "Connected" {
		g.Go(func() error {

			db, err := mysql.ConnectByString(envVars.MySQL.ConnectionString)
			if err != nil {
				log.Printf("Databases: MySQL: Error: message: %s", err)
				return err
			}
			defer db.Close()

			_, err = db.Exec("INSERT INTO reports_runs (data) VALUES (?)", reportJSON)
			if err != nil {
				return err
			}

			return nil
		})
	}

	if envVars.Postgres.ConnectionStatus == "Connected" {
		g.Go(func() error {
			db, err := postgres.ConnectByString(envVars.Postgres.ConnectionString)
			if err != nil {
				log.Printf("Databases: PostgreSQL: Start: Error: %s", err)
				return err
			}
			defer db.Close()

			_, err = db.Exec("INSERT INTO github.reports_runs (data) VALUES ($1)", reportJSON)
			if err != nil {
				return err
			}

			return nil
		})
	}

	if envVars.MongoDB.ConnectionStatus == "Connected" {
		g.Go(func() error {

			ctx := context.Background()
			client, err := mongodb.ConnectByString(envVars.MongoDB.ConnectionString, ctx)
			if err != nil {
				log.Printf("MongoDB: Connect Error: message: %s", err)
				return err
			}
			defer client.Disconnect(ctx)

			db := client.Database(envVars.MongoDB.DB)

			dbCollectionReport := db.Collection("reports_runs")
			_, err = dbCollectionReport.InsertOne(ctx, report)
			if err != nil {
				return err
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Printf("Error: helperReportFinish: %v", err)
	}

	log.Printf("Successfully completed: Final Report: %s", reportJSON)

}
