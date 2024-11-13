package valkey

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

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
		log.Printf("Valkey: Failed to connect to: %v", err)
	} else {
		log.Printf("Valkey: Connect: %v", Valkey.Options().Addr)
	}
}

func GetMaxID(dbType string) (int64, error) {
	keys, err := Valkey.Keys(fmt.Sprintf("databases:%s-*", dbType)).Result()
	if err != nil {
		return 0, err
	}

	var maxID int64
	for _, key := range keys {
		parts := strings.Split(key, "-")
		if len(parts) < 2 {
			continue
		}
		idNum, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		if idNum > maxID {
			maxID = idNum
		}
	}

	return maxID, nil
}

func GetDatabases() ([]map[string]string, error) {

	keys, err := Valkey.Keys("databases:*").Result()
	if err != nil {
		return nil, err
	}

	var databases []map[string]string
	for _, key := range keys {
		fields, err := Valkey.HGetAll(key).Result()
		if err != nil {
			return nil, err
		}
		databases = append(databases, fields)
	}

	return databases, nil
}

func AddDatabase(id string, fields map[string]string) error {

	key := fmt.Sprintf("databases:%s", id)

	// Преобразуем map[string]string в map[string]interface{}
	data := make(map[string]interface{})
	for k, v := range fields {
		data[k] = v
	}

	// Добавляем информацию о базе данных в хэш
	_, err := Valkey.HMSet(key, data).Result()
	if err != nil {
		return err
	}

	return nil
}

func DeleteDatabase(id string) error {

	key := fmt.Sprintf("databases:%s", id)

	res, err := Valkey.Del(key).Result()
	log.Printf("Delete db: %s, %v, %v", id, res, err)
	if err != nil {
		return err
	}

	return nil
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
