package config

import (
	"net"
	"net/url"
	"os"
	"strconv"
)

type Config struct {
	Port              string
	AppURL            string
	PostgresURL       string
	PostgresHost      string
	PostgresPort      string
	PostgresUser      string
	PostgresPassword  string
	PostgresDatabase  string
	PostgresSSLMode   string
	OrisunAddress     string
	OrisunBoundary    string
	SessionSecret     string
	PIIKeySecret      string
	PIIBlindSecret    string
	UploadDir         string
	UploadBaseURL     string
	UseSSLForPostgres bool
	DevelopmentCookie bool
}

func Load() Config {
	port := env("PORT", "3000")
	useSSL := env("POSTGRES_USE_SSL", "false") == "true"
	sslMode := "disable"
	if useSSL {
		sslMode = "require"
	}

	pgURL := os.Getenv("DATABASE_URL")
	if pgURL == "" {
		connection := url.URL{
			Scheme:   "postgres",
			User:     url.UserPassword(env("POSTGRES_USER", "postgres"), env("POSTGRES_PASSWORD", "postgres")),
			Host:     net.JoinHostPort(env("POSTGRES_HOST", "localhost"), env("POSTGRES_PORT", "5432")),
			Path:     "/" + env("POSTGRES_DB", "go_orisun_datastar"),
			RawQuery: url.Values{"sslmode": {sslMode}}.Encode(),
		}
		pgURL = connection.String()
	}

	return Config{
		Port:              port,
		AppURL:            env("APP_URL", "http://localhost:"+port),
		PostgresURL:       pgURL,
		PostgresHost:      env("POSTGRES_HOST", "127.0.0.1"),
		PostgresPort:      env("POSTGRES_PORT", "5432"),
		PostgresUser:      env("POSTGRES_USER", "postgres"),
		PostgresPassword:  env("POSTGRES_PASSWORD", "postgres"),
		PostgresDatabase:  env("POSTGRES_DB", "go_orisun_datastar"),
		PostgresSSLMode:   sslMode,
		OrisunAddress:     env("ORISUN_HOST", "localhost") + ":" + env("ORISUN_PORT", "5006"),
		OrisunBoundary:    env("ORISUN_GENERAL_BOUNDARY", "go_orisun_datastar"),
		SessionSecret:     env("BETTER_AUTH_SECRET", "secret-key-that-should-be-very-secret"),
		PIIKeySecret:      env("PII_KEY_ENCRYPTION_SECRET", "development-pii-key-encryption-secret-change-me"),
		PIIBlindSecret:    env("PII_BLIND_INDEX_SECRET", "development-pii-blind-index-secret-change-me"),
		UploadDir:         env("UPLOAD_DIR", "static/uploads"),
		UploadBaseURL:     env("UPLOAD_BASE_URL", "/static/uploads"),
		UseSSLForPostgres: useSSL,
		DevelopmentCookie: env("NODE_ENV", "development") != "production",
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func IntEnv(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
