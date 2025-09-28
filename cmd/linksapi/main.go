package main

import (
	"fmt"
	"os"

	"github.com/kris-dev-hub/globallinks/pkg/linkdb"
)

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	var host, port, dbname string

	// Check if command-line arguments are provided (backward compatibility)
	if len(os.Args) >= 4 {
		host = os.Args[1]
		port = os.Args[2]
		dbname = os.Args[3]
		fmt.Println("Using command-line arguments for database configuration")
	} else {
		// Use environment variables with defaults
		host = getEnvOrDefault("MONGO_HOST", "localhost")
		port = getEnvOrDefault("MONGO_PORT", "27017")
		dbname = getEnvOrDefault("MONGO_DATABASE", "linkdb")
		fmt.Printf("Using environment variables: MONGO_HOST=%s, MONGO_PORT=%s, MONGO_DATABASE=%s\n", host, port, dbname)
	}

	// Validate configuration
	if host == "" || port == "" || dbname == "" {
		fmt.Println("Error: Database configuration is required")
		fmt.Println("Usage:")
		fmt.Println("  Command line: ./linksapi <host> <port> <database>")
		fmt.Println("  Environment:  Set MONGO_HOST, MONGO_PORT, MONGO_DATABASE")
		fmt.Println("Example environment variables:")
		fmt.Println("  MONGO_HOST=localhost")
		fmt.Println("  MONGO_PORT=27017")
		fmt.Println("  MONGO_DATABASE=linkdb")
		fmt.Println("  MONGO_USERNAME=user (optional)")
		fmt.Println("  MONGO_PASSWORD=pass (optional)")
		fmt.Println("  MONGO_AUTH_DB=admin (optional, default: admin)")
		os.Exit(1)
	}

	linkdb.InitServer(host, port, dbname)
}
