package main

import (
	"fmt"
	"os"

	"github.com/kris-dev-hub/globallinks/pkg/linkdb"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Require database configuration : ./linksapi localhost 27017 linkdb")
		os.Exit(1)
	}

	host := os.Args[1]
	port := os.Args[2]
	dbname := os.Args[3]

	linkdb.InitServer(host, port, dbname)
}
