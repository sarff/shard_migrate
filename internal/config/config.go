package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	SourceDB      string
	TableName     string
	ShardDir      string
	LogDir        string
	NumShards     int
	ReservedShard int
	BatchSize     int
	Readers       int
	TotalRows     int64
	GarbColTicker time.Duration
}

func LoadConfig() (*Config, error) {
	if err := godotenv.Load("./.env"); err != nil {
		log.Println("The .env file was not found, default values are used")
	}

	config := Config{
		SourceDB:      getEnv("SOURCE_DB", "/clients.db"),
		TableName:     getEnv("TABLE_NAME", "clients"),
		ShardDir:      getEnv("SHARD_DIR", "/shards"),
		LogDir:        getEnv("LOG_DIR", "/"),
		NumShards:     getEnvAsInt("NUM_SHARDS", 10),
		ReservedShard: getEnvAsInt("RESERVED_SHARD", 10),
		BatchSize:     getEnvAsInt("BATCH_SIZE", 6000),
		Readers:       getEnvAsInt("READERS", 6),
		TotalRows:     getEnvAsInt64("TOTAL_ROWS", 1217065012),
		GarbColTicker: time.Duration(getEnvAsInt("GARB_COL_TICKER_MINUTES", 5)) * time.Minute,
	}

	if config.SourceDB == "" || config.TableName == "" || config.ShardDir == "" {
		return nil, fmt.Errorf("incorrect configuration: required parameters are not specified")
	}

	return &config, nil
}

// Get an environment variable or default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// Get a numeric environment variable or default value
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

// Getting an Int64 environment variable or default value
func getEnvAsInt64(key string, defaultValue int64) int64 {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
		return value
	}
	return defaultValue
}
