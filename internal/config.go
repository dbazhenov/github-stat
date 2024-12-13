package internal

import (
	"fmt"
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
	DatasetDemoRepos string
	DatasetDemoPulls string
	Debug            bool
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

func GetEnvVars(appType string) (EnvVars, error) {

	var envVars EnvVars

	_ = godotenv.Overload()
	// if err != nil {
	// 	log.Println("Warning: Error loading .env file, continuing with existing environment variables")
	// }

	// Ensure required environment variables are set
	envVars.Valkey.Addr = os.Getenv("VALKEY_ADDR")
	if envVars.Valkey.Addr == "" {
		return envVars, fmt.Errorf("required environment variable VALKEY_ADDR is not set")
	}

	envVars.Valkey.Port = os.Getenv("VALKEY_PORT")
	if envVars.Valkey.Port == "" {
		return envVars, fmt.Errorf("required environment variable VALKEY_PORT is not set")
	}

	// Optional environment variables
	envVars.Valkey.Password = os.Getenv("VALKEY_PASSWORD")
	envVars.Valkey.DB, _ = parseInt("VALKEY_DB")

	if appType == "dataset" {
		envVars.App.DatasetLoadType = os.Getenv("DATASET_LOAD_TYPE")

		if envVars.App.DatasetLoadType == "github" {
			envVars.GitHub.Organisation = os.Getenv("GITHUB_ORG")
			if envVars.GitHub.Organisation == "" {
				return envVars, fmt.Errorf("required environment variable GITHUB_ORG is not set")
			}
		}

		envVars.GitHub.Token = os.Getenv("GITHUB_TOKEN")

		envVars.App.DatasetDemoRepos = os.Getenv("DATASET_DEMO_CSV_REPOS")
		envVars.App.DatasetDemoPulls = os.Getenv("DATASET_DEMO_CSV_PULLS")
		envVars.App.DelayMinutes, _ = parseInt("DELAY_MINUTES")
		envVars.App.Debug, _ = parseBool("DEBUG")
	}

	if appType == "load" {
		envVars.LoadGenerator.MySQL, _ = parseBool("LOAD_MYSQL")
		envVars.LoadGenerator.Postgres, _ = parseBool("LOAD_POSTGRES")
		envVars.LoadGenerator.MongoDB, _ = parseBool("LOAD_MONGODB")
	}

	if appType == "web" {
		envVars.ControlPanel.Port = os.Getenv("CONTROL_PANEL_PORT")
		if envVars.ControlPanel.Port == "" {
			return envVars, fmt.Errorf("required environment variable CONTROL_PANEL_PORT is not set")
		}
	}

	return envVars, nil
}

func InitConfig(appType string) {
	log.Printf("App: %s: Read config", appType)

	envVars, err := GetEnvVars(appType)
	if err != nil {
		log.Fatalf("App: %s: Error GetEnvVars: %v", appType, err)
	}

	Config = envVars
}

func GetConfig(appType string) EnvVars {
	log.Print("App: Read config")

	envVars, err := GetEnvVars(appType)
	if err != nil {
		log.Printf("Error loading .env file")
	}

	return envVars
}

func parseInt(key string) (int, error) {

	result_string := os.Getenv(key)
	result, err := strconv.Atoi(result_string)
	if err != nil {
		return 0, err
	}

	return result, nil
}

func parseBool(key string) (bool, error) {

	result_string := os.Getenv(key)
	result, err := strconv.ParseBool(result_string)
	if err != nil {
		return false, err
	}

	return result, nil
}
