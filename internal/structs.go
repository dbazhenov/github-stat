package internal

type IndexData struct {
	Dataset struct {
		MySQL      Database
		PostgreSQL Database
		MongoDB    Database
	}
	Databases     []map[string]string
	DatabasesLoad []map[string]string
}

type Database struct {
	DBName       string `json:"db_name"`
	Repositories int    `json:"repositories"`
	PullRequests int    `json:"pull_requests"`
}
