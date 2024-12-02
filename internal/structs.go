package internal

// IndexData holds the various data related to databases and datasets
type IndexData struct {
	Databases        []map[string]string // Database configurations from Redis
	DatabasesLoad    []map[string]string // Filtered database configurations with loadSwitch == true
	DatabasesDataset []DatabaseInfo      // Data from databases in an array format
	DatasetState     DatasetState        // Status and information about the dataset
}

// DatasetInfo contains information about the data from the dataset
type DatasetInfo struct {
	DBName       string `json:"db_name"`       // Name of the database
	Repositories int    `json:"repositories"`  // Number of repositories
	PullRequests int    `json:"pull_requests"` // Number of pull requests
	LastUpdate   string `json:"last_update"`   // Last update date
}

// DatabaseInfo contains detailed information about the database for rendering in a table
type DatabaseInfo struct {
	ID           string `json:"id"`            // Database identifier
	DBType       string `json:"db_type"`       // Type of database (mysql, postgres, mongodb)
	DBName       string `json:"db_name"`       // Name of the database
	Repositories int    `json:"repositories"`  // Number of repositories
	PullRequests int    `json:"pull_requests"` // Number of pull requests
	LastUpdate   string `json:"last_update"`   // Last update date
	Status       string `json:"status"`        // Status of the dataset
}

// DatasetState contains the state of the dataset retrieved from Valkey
type DatasetState struct {
	Status     string `json:"status"`
	Type       string `json:"type"`
	ReposCount int    `json:"repos_count"`
	PullsCount int    `json:"pulls_count"`
	LastUpdate string `json:"last_update"`
}
