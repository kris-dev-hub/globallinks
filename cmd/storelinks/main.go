package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/kris-dev-hub/globallinks/pkg/commoncrawl"

	"github.com/klauspost/compress/gzip"

	"github.com/kris-dev-hub/globallinks/pkg/fileutils"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// FileLinkCompacted - compacted link file
type FileLinkCompacted struct {
	LinkDomain    string `json:"ld"`
	LinkSubDomain string `json:"lsd"`
	LinkPath      string `json:"lp"`
	LinkRawQuery  string `json:"lrq"`
	LinkScheme    string `json:"ls"`
	PageHost      string `json:"ph"`
	PagePath      string `json:"pp"`
	PageRawQuery  string `json:"prq"`
	PageScheme    string `json:"ps"`
	LinkText      string `json:"lt"`
	NoFollow      int    `json:"nf"`
	NoIndex       int    `json:"ni"`
	DateFrom      string `json:"dfrom"`
	DateTo        string `json:"dto"`
	IP            string `json:"ip"`
	Qty           int    `json:"qty"`
}

func main() {
	var err error

	if len(os.Args) < 2 {
		fmt.Println("Require target directory and source file : ./storelinks data/links/compact_01.tar.gz ")
		os.Exit(1)
	}

	linkSegmentCompacted := os.Args[1]

	if !fileutils.FileExists(linkSegmentCompacted) {
		fmt.Println("Source file does not exist")
		os.Exit(1)
	}
	err = uploadDataToDatabase(linkSegmentCompacted)
	if err != nil {
		log.Fatalf("Could not split files: %v", err)
	}

	// TODO: remove compacted file after we finish all tests
	//	os.Remove(linkSegmentCompacted)
}

// split data into many files sorted by domain names
func uploadDataToDatabase(sortFile string) error {
	// Set client options and connect to MongoDB
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.TODO()) //nolint:errcheck

	// Choose the database and collection
	collection := client.Database("linkdb").Collection("links")

	// load data from sort file
	const maxCapacityScanner = 3 * 1024 * 1024 // 3*1MB

	// Open the gzipped file
	file, err := os.Open(sortFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)
	// create buffer to avoid going over token size
	buf := make([]byte, maxCapacityScanner)
	scanner.Buffer(buf, maxCapacityScanner)

	// Read each line and append to the records slice
	line := ""

	fileLink := FileLinkCompacted{}
	linksToSave := make([]interface{}, 0, 25000)
	i := 0
	for scanner.Scan() {
		line = scanner.Text()
		parts := strings.Split(line, "|")
		if len(parts) != 16 {
			// Invalid line - skip
			continue
		}
		if !commoncrawl.IsValidDomain(parts[0]) {
			//			fmt.Printf("!")
			continue
		}

		fileLink = FileLinkCompacted{}
		fileLink.LinkDomain = parts[0]
		fileLink.LinkSubDomain = parts[1]
		fileLink.LinkPath = parts[2]
		fileLink.LinkRawQuery = parts[3]
		fileLink.LinkScheme = parts[4]
		fileLink.PageHost = parts[5]
		fileLink.PagePath = parts[6]
		fileLink.PageRawQuery = parts[7]
		fileLink.PageScheme = parts[8]
		fileLink.LinkText = parts[9]
		fileLink.NoFollow, _ = strconv.Atoi(parts[10])
		fileLink.NoIndex, _ = strconv.Atoi(parts[11])
		fileLink.DateFrom = parts[12]
		fileLink.DateTo = parts[13]
		fileLink.IP = parts[14]
		fileLink.Qty, _ = strconv.Atoi(parts[15])

		linksToSave = append(linksToSave, fileLink)
		i++

		// save file every 25000 records and reset linksToSave
		if i >= 25000 {
			i = 0
			// Insert multiple documents
			_, err := collection.InsertMany(context.TODO(), linksToSave)
			if err != nil {
				log.Fatal(err)
			}
			linksToSave = make([]interface{}, 0, 25000)
			fmt.Printf("V")
		}

	}

	if err := scanner.Err(); err != nil {
		return err
	}
	if len(linksToSave) > 0 {
		_, err := collection.InsertMany(context.TODO(), linksToSave)
		if err != nil {
			log.Fatal(err)
		}
	}

	return nil
}
