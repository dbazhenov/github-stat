package internal

type Load struct {
	MySQLConnections      int  `json:"mysql_connections"`
	PostgreSQLConnections int  `json:"postgresql_connections"`
	MongoDBConnections    int  `json:"mongodb_connections"`
	MySQLSwitch1          bool `json:"mysql_switch_1"`
	MySQLSwitch2          bool `json:"mysql_switch_2"`
	MySQLSwitch3          bool `json:"mysql_switch_3"`
	MySQLSwitch4          bool `json:"mysql_switch_4"`
	PostgresSwitch1       bool `json:"postgres_switch_1"`
	PostgresSwitch2       bool `json:"postgres_switch_2"`
	PostgresSwitch3       bool `json:"postgres_switch_3"`
	PostgresSwitch4       bool `json:"postgres_switch_4"`
	MongoDBSwitch1        bool `json:"mongodb_switch_1"`
	MongoDBSwitch2        bool `json:"mongodb_switch_2"`
	MongoDBSwitch3        bool `json:"mongodb_switch_3"`
	MongoDBSwitch4        bool `json:"mongodb_switch_4"`
	MySQLSleep            int  `json:"mysql_sleep"`
	MongoDBSleep          int  `json:"mongodb_sleep"`
	PostgresSleep         int  `json:"postgres_sleep"`
}

type Connections struct {
	MySQLConnectionString    string `json:"mysql_connection_string"`
	MySQLStatus              string `json:"mysql_status"`
	PostgresConnectionString string `json:"postgres_connection_string"`
	PostgresStatus           string `json:"postgres_status"`
	MongoDBConnectionString  string `json:"mongodb_connection_string"`
	MongoDBDatabase          string `json:"mongodb_database"`
	MongoDBStatus            string `json:"mongodb_status"`
}

type IndexData struct {
	LoadConfig Load
	Settings   Connections
	Dataset    struct {
		MySQL      Database
		PostgreSQL Database
		MongoDB    Database
	}
}

type Database struct {
	DBName       string `json:"db_name"`
	Repositories int    `json:"repositories"`
	PullRequests int    `json:"pull_requests"`
}
