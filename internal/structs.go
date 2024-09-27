package internal

type Load struct {
	MySQLConnections      int  `json:"mysql_connections"`
	PostgreSQLConnections int  `json:"postgresql_connections"`
	MongoDBConnections    int  `json:"mongodb_connections"`
	MySQLSwitch           bool `json:"mysql_switch"`
	PostgreSQLSwitch      bool `json:"postgresql_switch"`
	MongoDBSwitch         bool `json:"mongodb_switch"`
}
