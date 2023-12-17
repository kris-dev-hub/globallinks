package linkdb

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type App struct {
	DB             *mongo.Client
	Dbname         string
	requestRecords map[string]*RequestInfo
}

func InitServer(host string, port string, dbname string) {
	db, err := InitDB("mongodb://" + host + ":" + port)
	if err != nil {
		log.Fatal(err)
	}

	requestRecords := make(map[string]*RequestInfo)

	app := &App{DB: db, Dbname: dbname, requestRecords: requestRecords}

	router := InitRoutes(app)

	handlerWithCORS := enableCORS(router)

	// start http server
	if os.Getenv("GO_ENV") == "production" {
		if err := http.ListenAndServeTLS(":8443", "cert/fullchain.pem", "cert/privkey.pem", handlerWithCORS); err != nil {
			fmt.Println("Failed to set up server")
			panic(err)
		}
	} else {
		if err := http.ListenAndServe(":8010", handlerWithCORS); err != nil {
			fmt.Println("Failed to set up server")
			panic(err)
		}
	}
}

func InitDB(connectionString string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, err
	}

	// Pinging the database to check the connection can be done here
	if err := client.Ping(ctx, nil); err != nil {
		panic(err)
	}

	return client, nil
}

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*") // allow any origin
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		// Check if the request is for CORS options
		if r.Method == "OPTIONS" {
			// Just return with the headers, don't pass the request along
			return
		}

		// Pass down the request to the next handler (or middleware)
		next.ServeHTTP(w, r)
	})
}
