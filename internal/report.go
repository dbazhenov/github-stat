package internal

type Report struct {
	Type        string `json:"type"`
	StartedAt   string `json:"started_at"`
	FinishedAt  string `json:"finished_at"`
	Count       int    `json:"count"`
	RequestsAPI int    `json:"api_requests"`
}
