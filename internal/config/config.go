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
	NumShards     int
	ReservedShard int
	BatchSize     int
	Readers       int
	TotalRows     int64
	GarbColTicker time.Duration
}

func LoadConfig() (*Config, error) {
	// Завантаження .env файлу
	if err := godotenv.Load(); err != nil {
		log.Println("Файл .env не знайдено, використовуються значення за замовчуванням")
	}

	// Встановлення конфігурації з env змінних або значень за замовчуванням
	config := Config{
		SourceDB:      getEnv("SOURCE_DB", "/Users/dmit/Pycha/TG_Andrew/bot/data/clients.db"),
		TableName:     getEnv("TABLE_NAME", "clients"),
		ShardDir:      getEnv("SHARD_DIR", "/Users/dmit/Pycha/TG_Andrew/files/shards"),
		NumShards:     getEnvAsInt("NUM_SHARDS", 10),
		ReservedShard: getEnvAsInt("RESERVED_SHARD", 10),
		BatchSize:     getEnvAsInt("BATCH_SIZE", 6000),
		Readers:       getEnvAsInt("READERS", 6),
		TotalRows:     getEnvAsInt64("TOTAL_ROWS", 1217065012),
		GarbColTicker: time.Duration(getEnvAsInt("GARB_COL_TICKER_MINUTES", 5)) * time.Minute,
	}

	// Перевірка основних параметрів
	if config.SourceDB == "" || config.TableName == "" || config.ShardDir == "" {
		return nil, fmt.Errorf("неправильна конфігурація: обов'язкові параметри не вказані")
	}

	return &config, nil
}

// Отримання змінної середовища або значення за замовчуванням
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// Отримання числової змінної середовища або значення за замовчуванням
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

// Отримання Int64 змінної середовища або значення за замовчуванням
func getEnvAsInt64(key string, defaultValue int64) int64 {
	valueStr := getEnv(key, "")
	if value, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
		return value
	}
	return defaultValue
}
