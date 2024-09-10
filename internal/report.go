package internal

type Report struct {
	Type           string `json:"type"`
	StartedAt      string `json:"started_at"`
	StartedAtUnix  int64  `json:"started_at_unix"`
	FinishedAt     string `json:"finished_at"`
	FinishedAtUnix int64  `json:"finished_at_unix"`
	FullTime       int64  `json:"full_time"`
	Counter        map[string]int
	Databases      map[string]bool
	Timer          map[string]int64
}

type ReportDatabases struct {
	Type           string `json:"type"`
	DB             string `json:"db"`
	StartedAt      string `json:"started_at"`
	StartedAtUnix  int64  `json:"started_at_unix"`
	FinishedAt     string `json:"finished_at"`
	FinishedAtUnix int64  `json:"finished_at_unix"`
	TotalMilli     int64  `json:"milliseconds"`
	Counter        ReportCounter
}

type ReportCounter struct {
	Repos           int `json:"repos"`
	ReposWithoutPRs int `json:"repos_without_pulls"`
	ReposWithPRs    int `json:"repos_with_pulls"`
	Pulls           int `json:"pulls"`
	PullsInserted   int `json:"pulls_inserted"`
	PullsUpdated    int `json:"pulls_updated"`
}
