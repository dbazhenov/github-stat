package internal

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type EnvVars struct {
	GitHub        ConfigGitHub
	Valkey        ConfigValkey
	ControlPanel  ConfigControlPanel
	App           ConfigApp
	LoadGenerator ConfigLoad
}

type ConfigApp struct {
	DelayMinutes     int
	DatasetLoadType  string
	Debug            bool
	DatasetDemoRepos string
	DatasetDemoPulls string
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

type ConfigValkey struct {
	Addr     string
	Port     string
	DB       int
	Password string
}

var Config EnvVars

func GetEnvVars() (EnvVars, error) {

	var envVars EnvVars

	err := godotenv.Overload()
	if err != nil {
		log.Println("Warning: Error loading .env file, continuing with existing environment variables")
	}

	envVars.GitHub.Organisation = os.Getenv("GITHUB_ORG")
	envVars.GitHub.Token = os.Getenv("GITHUB_TOKEN")
	if envVars.GitHub.Token == "" {
		log.Println("Configuration: GitHub Token is not set. The script will run in limited mode, only repositories will be fetched. Add Github Token to receive Pull Requests data.")
	}

	envVars.Valkey.Addr = os.Getenv("VALKEY_ADDR")
	envVars.Valkey.Port = os.Getenv("VALKEY_PORT")
	envVars.Valkey.Password = os.Getenv("VALKEY_PASSWORD")

	envVars.Valkey.DB, _ = parseInt("VALKEY_DB")

	envVars.App.Debug, _ = parseBool("DEBUG")

	envVars.LoadGenerator.MySQL, _ = parseBool("LOAD_MYSQL")
	envVars.LoadGenerator.Postgres, _ = parseBool("LOAD_POSTGRES")
	envVars.LoadGenerator.MongoDB, _ = parseBool("LOAD_MONGODB")

	envVars.ControlPanel.Port = os.Getenv("CONTROL_PANEL_PORT")

	envVars.App.DatasetLoadType = os.Getenv("DATASET_LOAD_TYPE")
	envVars.App.DatasetDemoRepos = os.Getenv("DATASET_DEMO_CSV_REPOS")
	envVars.App.DatasetDemoPulls = os.Getenv("DATASET_DEMO_CSV_PULLS")
	envVars.App.DelayMinutes, _ = parseInt("DELAY_MINUTES")

	return envVars, nil
}

func InitConfig() {
	log.Print("App: Read config")

	envVars, err := GetEnvVars()
	if err != nil {
		log.Printf("Error loading .env file")
	}

	Config = envVars
}

func GetConfig() EnvVars {
	log.Print("App: Read config")

	envVars, err := GetEnvVars()
	if err != nil {
		log.Printf("Error loading .env file")
	}

	return envVars
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
