package linkdb

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type App struct {
	DB     *mongo.Client
	Dbname string
}

func InitServer(host string, port string, dbname string) {
	db, err := InitDB("mongodb://" + host + ":" + port)
	if err != nil {
		log.Fatal(err)
	}
	app := &App{DB: db, Dbname: dbname}

	router := InitRoutes(app)

	handlerWithCORS := enableCORS(router)

	// start http server
	if err := http.ListenAndServe(":8010", handlerWithCORS); err != nil {
		fmt.Println("Failed to set up server")
		panic(err)
	}
}

func InitDB(connectionString string) (*mongo.Client, error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, err
	}
	// TODO: Additional setup like pinging the database to check the connection can be done here
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
