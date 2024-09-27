package internal

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type EnvVars struct {
	MySQL        ConfigMySQL
	MongoDB      ConfigMongoDB
	GitHub       ConfigGitHub
	Postgres     ConfigPostgres
	Valkey       ConfigValkey
	ControlPanel ConfigControlPanel
	App          ConfigApp
}

type ConfigApp struct {
	DelayMinutes int
	Debug        bool
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
	if err != nil {
		return EnvVars{}, err
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

	envVars.ControlPanel.Host = os.Getenv("CONTROL_PANEL_HOST")
	envVars.ControlPanel.Port = os.Getenv("CONTROL_PANEL_PORT")

	stringValketDB := os.Getenv("VALKEY_DB")
	valkeyDBint, err := strconv.Atoi(stringValketDB)
	if err != nil {
		log.Fatalf("Error converting VALKEY_DB to int: %v", err)
	}
	envVars.Valkey.DB = valkeyDBint

	debugStr := os.Getenv("DEBUG")
	debug, err := strconv.ParseBool(debugStr)
	if err != nil {
		log.Fatalf("Error converting DEBUG to bool: %v", err)
	}

	envVars.App.Debug = debug

	delayString := os.Getenv("DELAY_MINUTES")
	delayInt, err := strconv.Atoi(delayString)
	if err != nil {
		log.Fatalf("Error converting DELAY_MINUTES to int: %v", err)
	}
	envVars.App.DelayMinutes = delayInt

	return envVars, nil
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
