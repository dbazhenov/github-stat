package internal

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type EnvVars struct {
	MySQL         ConfigMySQL
	MongoDB       ConfigMongoDB
	GitHub        ConfigGitHub
	Postgres      ConfigPostgres
	Valkey        ConfigValkey
	ControlPanel  ConfigControlPanel
	App           ConfigApp
	LoadGenerator ConfigLoad
}

type ConfigApp struct {
	DelayMinutes int
	Debug        bool
}

type ConfigLoad struct {
	MySQL    bool
	Postgres bool
	MongoDB  bool
}

type ConfigControlPanel struct {
	Host string
	Port string
}

type ConfigGitHub struct {
	Token        string
	Organisation string
}

type ConfigMySQL struct {
	DB               string
	User             string
	Password         string
	Host             string
	Port             string
	ConnectionString string
	ConnectionStatus string
}

type ConfigMongoDB struct {
	DB               string
	User             string
	Password         string
	Host             string
	Port             string
	ConnectionString string
	ConnectionStatus string
}

type ConfigPostgres struct {
	DB               string
	User             string
	Password         string
	Host             string
	Port             string
	ConnectionString string
	ConnectionStatus string
}

type ConfigValkey struct {
	Addr     string
	Port     string
	DB       int
	Password string
}

var Config EnvVars

func GetEnvVars() (EnvVars, error) {

	var envVars EnvVars

	err := godotenv.Load()
	if err != nil && !os.IsNotExist(err) {
		log.Println("Warning: Error loading .env file, continuing with existing environment variables")
	}

	envVars.GitHub.Organisation = os.Getenv("GITHUB_ORG")
	envVars.GitHub.Token = os.Getenv("GITHUB_TOKEN")
	if envVars.GitHub.Token == "" {
		log.Println("Configuration: GitHub Token is not set. The script will run in limited mode, only repositories will be fetched. Add Github Token to receive Pull Requests data.")
	}

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

	envVars.Valkey.Addr = os.Getenv("VALKEY_ADDR")
	envVars.Valkey.Port = os.Getenv("VALKEY_PORT")
	envVars.Valkey.Password = os.Getenv("VALKEY_PASSWORD")

	envVars.Valkey.DB, _ = parseInt("VALKEY_DB")

	envVars.App.Debug, _ = parseBool("DEBUG")

	envVars.LoadGenerator.MySQL, _ = parseBool("LOAD_MYSQL")
	envVars.LoadGenerator.Postgres, _ = parseBool("LOAD_POSTGRES")
	envVars.LoadGenerator.MongoDB, _ = parseBool("LOAD_MONGODB")

	envVars.ControlPanel.Host = os.Getenv("CONTROL_PANEL_HOST")
	envVars.ControlPanel.Port = os.Getenv("CONTROL_PANEL_PORT")

	envVars.App.DelayMinutes, _ = parseInt("DELAY_MINUTES")

	return envVars, nil
}

func parseInt(key string) (int, error) {

	result_string := os.Getenv(key)
	result, err := strconv.Atoi(result_string)
	if err != nil {
		log.Printf("Error converting %s to int: %v", key, err)
		return 0, err
	}

	return result, nil
}

func parseBool(key string) (bool, error) {

	result_string := os.Getenv(key)
	result, err := strconv.ParseBool(result_string)
	if err != nil {
		log.Printf("Error converting %s to int: %v", key, err)
		return false, err
	}

	return result, nil
}

func GetConfig() EnvVars {
	log.Print("App: Read config")

	envVars, err := GetEnvVars()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	return envVars
}

func InitConfig() {
	log.Print("App: Read config")

	envVars, err := GetEnvVars()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	Config = envVars
}
