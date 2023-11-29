package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/kris-dev-hub/globallinks/pkg/commoncrawl"
	"github.com/kris-dev-hub/globallinks/pkg/fileutils"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const defaultDir = "data"

// save sorting output in gz file, don't generate page file
const stopOnSort = true

const savePageData = false

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

	dirWat := defaultDir + "/wat"
	err = createDataDirectory(dirWat)
	if err != nil {
		log.Fatalf("Could not create directory: %v", err)
	}

	//check if wat.paths.gz.tmp exists
	if fileutils.FileExists(defaultDir+"/wat.paths.gz") != true {
		//download file
		err = fileutils.DownloadFile("https://data.commoncrawl.org/crawl-data/"+archiveName+"/wat.paths.gz", "data/wat.paths.gz", 1)
		if err != nil {
			log.Fatalf("Could not load WAT paths file: %v", err)
		}
	}

	dirOut := defaultDir + "/out/" + segmentName

	if savePageData == true {
		err = createDataDirectory(dirOut + "/page")
		if err != nil {
			log.Fatalf("Could not create directory: %v", err)
		}
	}

	err = createDataDirectory(defaultDir + "/out/links")
	if err != nil {
		log.Fatalf("Could not create directory: %v", err)
	}

	watPaths, err := fileutils.ReadGZFileByLine(defaultDir + "/wat.paths.gz")
	if err != nil {
		log.Fatalf("Could not load WAT list file: %v", err)
	}

	guard := make(chan struct{}, maxThreads) // limits the number of goroutines running at once
	var wg sync.WaitGroup

	watFilesQty := countFilesInSegment(watPaths, segmentName)
	for _, watPath := range watPaths {

		fileID, err := commoncrawl.ExtractWatFileNumber(watPath)
		if err != nil {
			continue
		}

		linkFile := dirOut + "/" + fileID + ".txt.gz"
		if fileutils.FileExists(linkFile) == true {
			continue
		}

		if maxWatFiles <= 0 {
			break
		}

		if strings.Contains(watPath, segmentName) == false {
			continue
		}
		maxWatFiles--

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

		recordFile := defaultDir + "/wat/" + filepath.Base(watPath)

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

			err = commoncrawl.ParseWatByLine(recordFile, dirOut, savePageData)
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

	filesQty, err := countNonEmptyGzFiles(dirOut)
	if err != nil {
		log.Fatalf("Could not count files in directory: %v", err)
	}

	//sort the file
	if filesQty == watFilesQty {
		if !fileutils.FileExists(dirOut + "/_sort.txt") {

			//just save sorted data in gzip file and exit
			if stopOnSort == true {
				err = sortOutFilesWithBashGz(dirOut, segmentName)
				if err != nil {
					log.Fatalf("Could not sort file: %v", err)
				}

				err = deleteWatPreProcessed(dirOut)
				if err != nil {
					log.Fatalf("Could not delete WAT processed files: %v", err)
				}

				os.Exit(0)
			}

			err = sortOutFilesWithBash(dirOut)
			if err != nil {
				log.Fatalf("Could not sort file: %v", err)
			}

			err = deleteWatPreProcessed(dirOut)
			if err != nil {
				log.Fatalf("Could not delete WAT processed files: %v", err)
			}

			err = splitProcessedData(dirOut+"/_sort.txt", defaultDir+"/out/links")
			if err != nil {
				log.Fatalf("Could not split files: %v", err)
			}
			os.Remove(dirOut + "/_sort.txt")

		}
	}

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

// countNonEmptyGzFiles counts all .txt.gz files with a five-digit name in the specified directory
func countNonEmptyGzFiles(dir string) (int, error) {
	var count int
	fileRegex := regexp.MustCompile(`^\d{5}\.txt\.gz$`)

	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if fileRegex.MatchString(info.Name()) {
			if info.Size() > 0 {
				count++
			}
		}

		return nil
	})

	return count, err
}

// sortOutFilesWithBashGz - sort the file with bash sort and save as gz with segment in name - you can use these segments to move pre processed data to other server
func sortOutFilesWithBashGz(dirOut string, segmentName string) error {

	cmdStr := "zcat " + dirOut + "/*.txt.gz | sort -S 2G | gzip > " + defaultDir + "/out/_sort_" + segmentName + ".txt.gz"

	// Execute the command
	cmd := exec.Command("bash", "-c", cmdStr)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return err
}

// sortOutFilesWithBash - sort the file with bash sort - it is the fastest way to sort the file
func sortOutFilesWithBash(dirOut string) error {

	cmdStr := "zcat " + dirOut + "/*.txt.gz | sort -S 2G -o " + dirOut + "/_sort.txt"

	// Execute the command
	cmd := exec.Command("bash", "-c", cmdStr)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return err
}

// countFilesInSegment - count files in segment
func countFilesInSegment(watPaths []string, segment string) int {
	filesQty := 0
	for _, watPath := range watPaths {
		if strings.Contains(watPath, segment) == true {
			filesQty++
			continue
		}
	}
	return filesQty
}

// split data into many files sorted by domain names
func splitProcessedData(sortFile string, dirOut string) error {

	file, err := os.Open(sortFile)
	if err != nil {
		return err
	}
	defer file.Close()

	var currentFile *os.File
	var gzipWriter *gzip.Writer
	var currentPrefix string

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 5 {
			continue
		}

		firstLetter := string(line[0])
		firstTwoLetters := line[1:2]
		firstThreeLetters := line[2:3]
		prefix := line[:5]
		dirPath := dirOut + "/" + firstLetter + "/" + firstTwoLetters + "/" + firstThreeLetters

		if prefix != currentPrefix {
			if gzipWriter != nil {
				gzipWriter.Close()
				currentFile.Close()
			}

			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return err
			}

			fileName := dirPath + "/" + prefix + ".gz"
			currentFile, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}

			gzipWriter = gzip.NewWriter(currentFile)
			currentPrefix = prefix
		}

		_, err = gzipWriter.Write([]byte(line + "\n"))
		if err != nil {
			return err
		}
	}

	if gzipWriter != nil {
		gzipWriter.Close()
		currentFile.Close()
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil

}

// deleteWatPreProcessed - Delete files build during WAT processing
func deleteWatPreProcessed(dirPath string) error {
	pattern := `[0-9]{5}\.txt\.gz`
	re := regexp.MustCompile(pattern)

	files, err := filepath.Glob(filepath.Join(dirPath, "*.txt.gz"))
	if err != nil {
		return err
	}

	for _, file := range files {
		if re.MatchString(filepath.Base(file)) {
			err := os.Remove(file)
			if err != nil {
				// Handle the error, but continue processing other files.
				fmt.Printf("Error deleting file %s: %s\n", file, err)
			}
		}
	}

	return nil
}
