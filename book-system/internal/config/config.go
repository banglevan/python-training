package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port           string
	DBPath         string
	SessionKey     []byte
	SessionMaxAge  int
	AllowedOrigins []string
}

func Load() *Config {
	return &Config{
		Port:          getEnv("PORT", "8080"),
		DBPath:        getEnv("DB_PATH", "books.db"),
		SessionKey:    []byte(getEnv("SESSION_KEY", "your-secret-key")),
		SessionMaxAge: getEnvAsInt("SESSION_MAX_AGE", 86400), // 24 hours
		AllowedOrigins: []string{
			"http://localhost:8080",
			"http://localhost:3000",
		},
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}
