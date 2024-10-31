package main

import (
	"database/sql"
	"fmt"
	"imersaofc/internal/converter"
	"log/slog"
	"os"

	_ "github.com/lib/pq"
)

func connectToSQL() (*sql.DB, error) {
	user := getEnvOrDefault("POSTGRES_USER", "myuser")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "mypassword")
	dbName := getEnvOrDefault("POSTGRES_DB", "mydb")
	host := getEnvOrDefault("POSTGRES_HOST", "postgres_container")
	ssl := getEnvOrDefault("POSTGRES_SSQLMODE", "disable")

	strConnection := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=%s", user, password, dbName, host, ssl)
	db, err := sql.Open("postgres", strConnection)
	if err != nil {
		slog.Error("Error connection to postgres database", slog.String("string", strConnection))
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("Could not ping the database", slog.String("string", strConnection))
		return nil, err
	}

	slog.Info("Connected succesfully to database", slog.String("string", strConnection))
	return db, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value, is := os.LookupEnv(key); is {
		return value
	}

	return defaultValue
}

func main() {
	db, err := connectToSQL()
	if err != nil {
		panic(err)
	}

	vc := converter.NewVideoConverter(db)
	vc.Handle(
		[]byte(`{"video_id": 2, "path": "mediatest/media/uploads/2"}`),
		"fmpeg-test.mpd",
		"merged-test.mp4")
}
