package main

import (
	"fmt"
	"github.com/kris-dev-hub/globallinks/pkg/commoncrawl"
	"github.com/kris-dev-hub/globallinks/pkg/fileutils"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

func main() {
	var err error

	archiveName := "CC-MAIN-2021-04"
	segmentName := "20210115134101"

	if len(os.Args) < 3 {
		fmt.Println("No archive name or segment specified")
		os.Exit(1)
	}

	if isCorrectArchiveFormat(os.Args[1]) != true {
		fmt.Println("Invalid archive name")
		os.Exit(1)
	}

	if isCorrectSegmentFormat(os.Args[2]) != true {
		fmt.Println("Invalid segment name")
		os.Exit(1)
	}

	archiveName = os.Args[1]
	segmentName = os.Args[2]

	maxThreads := setMaxThreads()
	maxWatFiles := setMaxWATFiles()

	dirWat := "data/wat"
	err = createDataDirectory(dirWat)
	if err != nil {
		log.Fatalf("Could not create directory: %v", err)
	}

	//check if wat.paths.gz.tmp exists
	if fileutils.FileExists("data/wat.paths.gz") != true {
		//download file
		err = fileutils.DownloadFile("https://data.commoncrawl.org/crawl-data/"+archiveName+"/wat.paths.gz", "data/wat.paths.gz", 1)
		if err != nil {
			log.Fatalf("Could not load WAT paths file: %v", err)
		}
	}

	dirOut := "data/out/" + segmentName
	err = createDataDirectory(dirOut + "/page")
	if err != nil {
		log.Fatalf("Could not create directory: %v", err)
	}

	watPaths, err := fileutils.ReadGZFileByLine("data/wat.paths.gz")
	if err != nil {
		log.Fatalf("Could not load WAT list file: %v", err)
	}

	guard := make(chan struct{}, maxThreads) // limits the number of goroutines running at once
	var wg sync.WaitGroup

	for _, watPath := range watPaths {

		fileID, err := commoncrawl.ExtractWatFileNumber(watPath)
		if err != nil {
			continue
		}

		linkFile := dirOut + "/" + fileID + ".txt"
		if fileutils.FileExists(linkFile) == true {
			continue
		}

		if maxWatFiles <= 0 {
			break
		}

		maxWatFiles--
		if strings.Contains(watPath, segmentName) == false {
			continue
		}

		//create empty file to block other processes from processing the same file
		file, err := os.Create(linkFile)
		if err != nil {
			// Handle the error, e.g., exit or log
			log.Fatalf("Failed to create file: %s", err)
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("Could not close file: %v", err)
		}

		recordFile := "data/wat/" + filepath.Base(watPath)

		if fileutils.FileExists(recordFile) != true {
			err := fileutils.DownloadFile("https://data.commoncrawl.org/"+watPath, recordFile, 1)
			if err != nil {
				log.Fatalf("Could not load WAT file %s: %v", watPath, err)
			}
		}

		fmt.Println("Importing file: ", recordFile)

		wg.Add(1)
		// Before starting the goroutine, we insert an empty struct into the guard channel.
		// If the channel is already full (meaning we have 'maxGoroutines' goroutines running),
		// this will block until one of the running goroutines finishes and reads from the channel.
		guard <- struct{}{}
		go func(recordFile string, dirOut string) {
			defer wg.Done()            // Signal the WaitGroup that the goroutine is done after it finishes
			defer func() { <-guard }() // Release the guard when the goroutine is done

			err = commoncrawl.ParseWatByLine(recordFile, dirOut)
			if err != nil {
				log.Fatalf("Could not open WAT file: %v", err)
			}

			err = os.Remove(recordFile)
			if err != nil {
				log.Fatalf("Could not delete file: %v", err)
			}

		}(recordFile, dirOut)

	}
	wg.Wait() // This will block until all goroutines have called wg.Done()

}

// isCorrectArchiveFormat checks if the archive name is in the correct format
func isCorrectArchiveFormat(s string) bool {
	pattern := `^CC-MAIN-\d{4}-\d{2}$`
	match, _ := regexp.MatchString(pattern, s)
	return match
}

// isCorrectSegmentFormat checks if the segment name is in the correct format
func isCorrectSegmentFormat(s string) bool {
	pattern := `^20\d{12}$`
	match, _ := regexp.MatchString(pattern, s)
	return match
}

// createDataDirectory creates the data directory if it does not exist
func createDataDirectory(dirOut string) error {
	var err error

	if _, err = os.Stat(dirOut); os.IsNotExist(err) {
		// The directory does not exist, create it
		err = os.MkdirAll(dirOut, os.ModePerm)
		if err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}

	return nil
}

// setMaxThreads sets the maximum number of threads to use for processing. Every thread need around 1,5GB of RAM
func setMaxThreads() int {

	envVar := "GLOBALLINKS_MAXTHREADS"
	defaultVal := 1
	minVal := 1
	maxVal := 16

	maxThreadsStr := os.Getenv(envVar)
	if maxThreadsStr == "" {
		return defaultVal
	}

	maxThreads, err := strconv.Atoi(maxThreadsStr)
	if err != nil {
		log.Printf("Invalid number for %s: %v. Using default %d", envVar, err, defaultVal)
		return defaultVal
	}

	if maxThreads < minVal || maxThreads > maxVal {
		log.Printf("Number for %s must be between %d and %d. Using default %d", envVar, minVal, maxVal, defaultVal)
		return defaultVal
	}

	return maxThreads
}

// setMaxWATFiles sets the maximum number WAT files in one go. Every WAT file need around 30sec per i5-9300H core to process
func setMaxWATFiles() int {

	envVar := "GLOBALLINKS_MAXWATFILES"
	defaultVal := 1
	minVal := 1
	maxVal := 1000

	maxFilesStr := os.Getenv(envVar)
	if maxFilesStr == "" {
		return defaultVal
	}

	maxFiles, err := strconv.Atoi(maxFilesStr)
	if err != nil {
		log.Printf("Invalid number for %s: %v. Using default %d", envVar, err, defaultVal)
		return defaultVal
	}

	if maxFiles < minVal || maxFiles > maxVal {
		log.Printf("Number for %s must be between %d and %d. Using default %d", envVar, minVal, maxVal, defaultVal)
		return defaultVal
	}

	return maxFiles
}
