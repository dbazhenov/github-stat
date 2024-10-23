package valkey

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-redis/redis"

	app "github-stat/internal"
)

var Valkey *redis.Client
var Load app.Load

func InitValkey(envVars app.EnvVars) {

	addr := fmt.Sprintf("%s:%s", envVars.Valkey.Addr, envVars.Valkey.Port)
	Valkey = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: envVars.Valkey.Password,
		DB:       envVars.Valkey.DB,
	})

	_, err := Valkey.Ping().Result()
	if err != nil {
		log.Fatalf("Valkey: Failed to connect to: %v", err)
	} else {
		log.Printf("Valkey: Connect: %v", Valkey.Options().Addr)
	}
}
func SaveConfigToValkey(load app.Load) error {
	data, err := json.Marshal(load)
	if err != nil {
		return err
	}
	return Valkey.Set("load_config", data, 0).Err()
}

func LoadControlPanelConfigFromValkey() (app.Load, error) {
	var load app.Load
	data, err := Valkey.Get("load_config").Result()
	if err != nil {
		return load, err
	}
	err = json.Unmarshal([]byte(data), &load)
	return load, err
}

func Connect(addr string) *redis.Client {
	Valkey = redis.NewClient(&redis.Options{
		Addr: addr, // "localhost:6379"
	})
	return Valkey
}

func SaveToValkey(key string, settings app.Connections) error {
	data, err := json.Marshal(settings)
	if err != nil {
		return err
	}
	return Valkey.Set(key, data, 0).Err()
}

func LoadFromValkey(key string) (app.Connections, error) {
	var settings app.Connections
	data, err := Valkey.Get(key).Result()
	if err != nil {
		return settings, err
	}
	err = json.Unmarshal([]byte(data), &settings)
	return settings, err
}
