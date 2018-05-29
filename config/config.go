package config

import (
	"encoding/json"
	"os"
	"blockchaininfo-test-task/tools"
)

type Config struct {
	Port       string `json:"port"`
	BoltDBName string `json:"bolt_db_name"`
	BucketName string `json:"bolt_db_bucket_name"`
}

func InitConfig() Config {
	config := Config{}
	file, err := os.Open("conf.json")
	tools.Check("Error while open conf.json", err)
	err = json.NewDecoder(file).Decode(&config)
	tools.Check("Error while decode conf.json", err)
	return config
}
