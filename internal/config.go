package internal

import (
	"os"

	"github.com/joho/godotenv"
)

type EnvVars struct {
	MySQL    ConfigMySQL
	MongoDB  ConfigMongoDB
	GitHub   ConfigGitHub
	Postgres ConfigPostgres
}

type ConfigGitHub struct {
	Token        string
	Organisation string
}

type ConfigMySQL struct {
	DB       string
	User     string
	Password string
	Host     string
	Port     string
}

type ConfigMongoDB struct {
	DB       string
	User     string
	Password string
	Host     string
	Port     string
}

type ConfigPostgres struct {
	DB       string
	User     string
	Password string
	Host     string
	Port     string
}

func GetEnvVars() (EnvVars, error) {

	var envVars EnvVars

	err := godotenv.Load()
	if err != nil {
		return EnvVars{}, err
	}

	envVars.GitHub.Organisation = os.Getenv("GITHUB_ORG")
	envVars.GitHub.Token = os.Getenv("GITHUB_TOKEN")

	envVars.MongoDB.User = os.Getenv("MONGODB_USER")
	envVars.MongoDB.Password = os.Getenv("MONGODB_PASSWORD")
	envVars.MongoDB.DB = os.Getenv("MONGODB_DB")
	envVars.MongoDB.Host = os.Getenv("MONGODB_HOST")
	envVars.MongoDB.Port = os.Getenv("MONGODB_PORT")

	envVars.Postgres.User = os.Getenv("PG_USER")
	envVars.Postgres.Password = os.Getenv("PG_PASSWORD")
	envVars.Postgres.DB = os.Getenv("PG_DB")
	envVars.Postgres.Host = os.Getenv("PG_HOST")
	envVars.Postgres.Port = os.Getenv("PG_PORT")

	envVars.MySQL.User = os.Getenv("MYSQL_USER")
	envVars.MySQL.Password = os.Getenv("MYSQL_PASSWORD")
	envVars.MySQL.DB = os.Getenv("MYSQL_DB")
	envVars.MySQL.Host = os.Getenv("MYSQL_HOST")
	envVars.MySQL.Port = os.Getenv("MYSQL_PORT")

	return envVars, nil
}
