package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github-stat/internal"

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
	envVars := getConfig()

	for {
		// The main process of getting data from GitHub API and store it into MySQL, PostgreSQL, MongoDB databases.
		fetchGitHubData(envVars)

		// Delay before the next start (Defined by the DELAY_MINUTES parameter)
		helperSleep(envVars)
	}
}

func fetchGitHubData(envVars internal.EnvVars) {

	report := helperReportStart()

	// Get all the repositories of the organization.
	allRepos, counterRepos, err := internal.FetchGitHubRepos(envVars)
	if err != nil {
		log.Fatal(err)
	}

	report.Timer["ApiRepos"] = time.Now().UnixMilli()

	// For DEBUG mode. Let's keep only 4 repositories out of many to speed up the complete process.
	allRepos = filterRepos(envVars, allRepos)

	var allPulls map[string][]*github.PullRequest
	var counterPulls map[string]int

	if envVars.GitHub.Token != "" {

		// Get the latest Pull Requests updates to download only the new ones. Will download all Pull Requests on the first run.
		pullsLastUpdate, err := getLatestUpdates(envVars, allRepos)
		if err != nil {
			log.Fatal(err)
		}

		report.Timer["DBLatestUpdates"] = time.Now().UnixMilli()

		// Get Pull Requests for all repositories.
		allPulls, counterPulls, err = internal.FetchGitHubPullsByRepos(envVars, allRepos, pullsLastUpdate)
		if err != nil {
			log.Fatal(err)
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

func filterRepos(envVars internal.EnvVars, allRepos []*github.Repository) []*github.Repository {

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

func getLatestUpdates(envVars internal.EnvVars, allRepos []*github.Repository) (map[string]*internal.PullsLastUpdate, error) {
	lastUpdates := make(map[string]*internal.PullsLastUpdate)

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	var updatedMySQL, updatedPostgres, updatedMongo map[string]string
	var errMySQL, errPostgres, errMongo error

	if envVars.MySQL.Host != "" {
		g.Go(func() error {
			updatedMySQL, errMySQL = getLatestUpdatesFromMySQL(envVars)
			return errMySQL
		})
	}

	if envVars.Postgres.Host != "" {
		g.Go(func() error {
			updatedPostgres, errPostgres = getLatestUpdatesFromPostgres(envVars)
			return errPostgres
		})
	}

	if envVars.MongoDB.Host != "" {
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
			lastUpdates[repoName] = &internal.PullsLastUpdate{}
		}

		if updatedMySQL[repoName] != "" {
			lastUpdates[repoName].MySQL = updatedMySQL[repoName]
		}
		if updatedPostgres[repoName] != "" {
			lastUpdates[repoName].PostgreSQL = updatedPostgres[repoName]
		}
		if updatedMongo[repoName] != "" {
			lastUpdates[repoName].MongoDB = updatedMongo[repoName]
		}

		lastUpdates[repoName].Minimum = findMinimumDate(
			lastUpdates[repoName].MySQL,
			lastUpdates[repoName].PostgreSQL,
			lastUpdates[repoName].MongoDB,
		)
	}

	return lastUpdates, nil
}

func asyncProcessDBs(envVars internal.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) {

	ctx := context.Background()

	g, _ := errgroup.WithContext(ctx)

	if envVars.MySQL.Host != "" {
		g.Go(func() error {
			return MySQLprocessPulls(envVars, allRepos, allPulls)
		})
	}

	if envVars.Postgres.Host != "" {
		g.Go(func() error {
			return PostgreSQLprocessPulls(envVars, allRepos, allPulls)
		})
	}

	if envVars.MongoDB.Host != "" {
		g.Go(func() error {
			return MongoDBprocessPulls(envVars, allRepos, allPulls)
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("Error: %v", err)
	}

}

func getLatestUpdatesFromMySQL(envVars internal.EnvVars) (map[string]string, error) {
	ctx := context.Background()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		envVars.MySQL.User, envVars.MySQL.Password, envVars.MySQL.Host, envVars.MySQL.Port, envVars.MySQL.DB)

	log.Printf("Check Pulls Latest Updates: MySQL: Connect to: %s", envVars.MySQL.Host)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Print(err)
		return nil, err
	}
	defer db.Close()

	query := `
		SELECT
			repo,
			MAX(JSON_UNQUOTE(JSON_EXTRACT(data, '$.updated_at'))) AS updated_at
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

func getLatestUpdatesFromPostgres(envVars internal.EnvVars) (map[string]string, error) {

	ctx := context.Background()
	dsn := fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
		envVars.Postgres.User, envVars.Postgres.Password, envVars.Postgres.DB, envVars.Postgres.Host, envVars.Postgres.Port)

	log.Printf("Check Pulls Latest Updates: PostgreSQL: Connect to: %s", envVars.Postgres.Host)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Print(err)
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

func getLatestUpdatesFromMongoDB(envVars internal.EnvVars) (map[string]string, error) {
	ctx := context.Background()

	ApplyURI := fmt.Sprintf("mongodb://%s:%s@%s:%s/", envVars.MongoDB.User, envVars.MongoDB.Password, envVars.MongoDB.Host, envVars.MongoDB.Port)
	log.Printf("Check Pulls Latest Updates: MongoDB: Connect to: %s", envVars.MongoDB.Host)
	clientOptions := options.Client().ApplyURI(ApplyURI)
	mongodb, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	defer mongodb.Disconnect(ctx)

	db := mongodb.Database(envVars.MongoDB.DB)
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

func MySQLprocessPulls(envVars internal.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) error {

	report := internal.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "MySQL",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		envVars.MySQL.User, envVars.MySQL.Password, envVars.MySQL.Host, envVars.MySQL.Port, envVars.MySQL.DB)

	log.Printf("Databases: MySQL: Start: Connect to: %s", envVars.MySQL.Host)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, repo := range allRepos {
		report.Counter.Repos++
		id := repo.ID
		repoJSON, err := json.Marshal(repo)
		if err != nil {
			return err
		}

		_, err = db.Exec("INSERT INTO github.repositories (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = ?", id, repoJSON, repoJSON)
		if err != nil {
			return err
		}
		if envVars.App.Debug {
			log.Printf("MySQL: Repo %s: Insert repo data", *repo.FullName)
		}

		if len(allPulls) > 0 {
			repoName := *repo.Name
			pullRequests, exists := allPulls[repoName]

			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
				if envVars.App.Debug {
					log.Printf("MySQL: Repo: %s: PRs: No pull requests found for repository", repoName)
				}
			} else {
				report.Counter.ReposWithPRs++
				for p, pull := range pullRequests {
					if envVars.App.Debug {
						log.Printf("MySQL: Repo: %s: PRs: Insert data row: %d, pull: %s", repoName, p, *pull.Title)
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
				if envVars.App.Debug {
					log.Printf("MySQL: Repo: %s: PRs: Completed: Total pull requests: %d", repoName, len(pullRequests))
				}
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
	_, err = db.Exec("INSERT INTO github.reports_databases (data) VALUES (?)", reportJSON)
	if err != nil {
		return err
	}

	return nil
}

func PostgreSQLprocessPulls(envVars internal.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) error {

	report := internal.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "PostgreSQL",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	dsn := fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
		envVars.Postgres.User, envVars.Postgres.Password, envVars.Postgres.DB, envVars.Postgres.Host, envVars.Postgres.Port)

	log.Printf("Databases: PostgreSQL: Start: Connect to: %s", envVars.Postgres.Host)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

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
		if envVars.App.Debug {
			log.Printf("Postgres: Repo %s: Insert repo data", *repo.FullName)
		}

		repoName := *repo.Name
		pullRequests, exists := allPulls[repoName]

		if len(allPulls) > 0 {
			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
				if envVars.App.Debug {
					log.Printf("Postgres: Repo: %s: PRs: No pull requests found for repository", repoName)
				}
			} else {
				report.Counter.ReposWithPRs++
				for p, pull := range pullRequests {

					if envVars.App.Debug {
						log.Printf("Postgres: Repo: %s: PRs: Insert data row: %d, pull: %s", repoName, p, *pull.Title)
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
				if envVars.App.Debug {
					log.Printf("Postgres: Repo: %s: PRs: Completed: Total pull requests: %d", repoName, len(pullRequests))
				}
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

func MongoDBprocessPulls(envVars internal.EnvVars, allRepos []*github.Repository, allPulls map[string][]*github.PullRequest) error {

	report := internal.ReportDatabases{
		Type:          "GitHub Pulls",
		DB:            "MongoDB",
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	ctx := context.Background()
	ApplyURI := fmt.Sprintf("mongodb://%s:%s@%s:%s/", envVars.MongoDB.User, envVars.MongoDB.Password, envVars.MongoDB.Host, envVars.MongoDB.Port)
	log.Printf("Databases: MongoDB: Start: Connect to: %s", envVars.MongoDB.Host)
	clientOptions := options.Client().ApplyURI(ApplyURI)
	mongodb, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return err
	}
	defer mongodb.Disconnect(ctx)

	db := mongodb.Database(envVars.MongoDB.DB)

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
		if envVars.App.Debug {
			log.Printf("MongoDB: Repo %s: Insert repo data", *repo.FullName)
		}
		if len(allPulls) > 0 {
			dbCollectionPulls := db.Collection("pulls")
			repoName := *repo.Name
			pullRequests, exists := allPulls[repoName]

			if !exists || len(pullRequests) == 0 {
				report.Counter.ReposWithoutPRs++
				if envVars.App.Debug {
					log.Printf("MongoDB: Repo: %s: PRs: No pull requests found for repository", repoName)
				}
			} else {
				report.Counter.ReposWithPRs++
				for p, pull := range pullRequests {

					filter := bson.M{"id": pull.ID, "repo": repoName}
					update := bson.M{"$set": pull}

					if envVars.App.Debug {
						log.Printf("MongoDB: Repo: %s: PRs: Insert data row: %d, pull: %s", repoName, p, *pull.Title)
					}

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
				if envVars.App.Debug {
					log.Printf("MongoDB: Repo: %s: PRs: Completed: Total pull requests: %d", repoName, len(pullRequests))
				}
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

func getConfig() internal.EnvVars {
	log.Print("App: Read config")

	envVars, err := internal.GetEnvVars()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	return envVars
}

func helperSleep(envVars internal.EnvVars) {
	minutes := time.Duration(envVars.App.DelayMinutes) * time.Minute
	log.Printf("App: The repeat run will start automatically after %v", minutes)
	time.Sleep(minutes)
}

func helperReportStart() internal.Report {

	report := internal.Report{
		StartedAt:     time.Now().Format("2006-01-02T15:04:05.000"),
		StartedAtUnix: time.Now().UnixMilli(),
	}

	report.Timer = make(map[string]int64)

	return report
}

func helperReportFinish(envVars internal.EnvVars, report internal.Report, counterPulls map[string]int, counterRepos int) {

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
	report.Databases["MySQL"] = envVars.MySQL.Host != ""
	report.Databases["Postgres"] = envVars.Postgres.Host != ""
	report.Databases["MongoDB"] = envVars.MongoDB.Host != ""

	reportJSON, _ := json.Marshal(report)

	ctx := context.Background()
	g, _ := errgroup.WithContext(ctx)

	if envVars.MySQL.Host != "" {
		g.Go(func() error {

			dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
				envVars.MySQL.User, envVars.MySQL.Password, envVars.MySQL.Host, envVars.MySQL.Port, envVars.MySQL.DB)

			db, err := sql.Open("mysql", dsn)
			if err != nil {
				return err
			}
			defer db.Close()

			_, err = db.Exec("INSERT INTO github.reports_runs (data) VALUES (?)", reportJSON)
			if err != nil {
				return err
			}

			return nil
		})
	}

	if envVars.Postgres.Host != "" {
		g.Go(func() error {
			dsn := fmt.Sprintf("user=%s password='%s' dbname=%s host=%s port=%s sslmode=disable",
				envVars.Postgres.User, envVars.Postgres.Password, envVars.Postgres.DB, envVars.Postgres.Host, envVars.Postgres.Port)
			db, err := sql.Open("postgres", dsn)
			if err != nil {
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

	if envVars.MongoDB.Host != "" {
		g.Go(func() error {

			ctx := context.Background()
			ApplyURI := fmt.Sprintf("mongodb://%s:%s@%s:%s/", envVars.MongoDB.User, envVars.MongoDB.Password, envVars.MongoDB.Host, envVars.MongoDB.Port)
			clientOptions := options.Client().ApplyURI(ApplyURI)
			mongodb, err := mongo.Connect(ctx, clientOptions)
			if err != nil {
				return err
			}
			defer mongodb.Disconnect(ctx)

			db := mongodb.Database(envVars.MongoDB.DB)

			dbCollectionReport := db.Collection("reports_runs")
			_, err = dbCollectionReport.InsertOne(ctx, report)
			if err != nil {
				return err
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("Error: %v", err)
	}

	log.Printf("Successfully completed: Final Report: %s", reportJSON)

}
