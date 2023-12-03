package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"

	"github.com/kris-dev-hub/globallinks/pkg/commoncrawl"
	"github.com/kris-dev-hub/globallinks/pkg/fileutils"
)

// save sorting output in gz file, don't generate page file
const stopOnSort = true

const savePageData = true

const (
	extensionTxtGz = ".txt.gz"
	linkDir        = "/link/"
)

func main() {
	var err error
	var archiveName string

	if len(os.Args) < 2 {
		fmt.Println("No archive name or segment specified")
		os.Exit(1)
	}

	if !isCorrectArchiveFormat(os.Args[1]) {
		fmt.Println("Invalid archive name")
		os.Exit(1)
	}

	archiveName = os.Args[1]
	maxThreads := setMaxThreads()
	maxWatFiles := setMaxWATFiles()
	defaultDir := setDataDirectory()

	// import segment information
	segmentList, err := commoncrawl.InitImport(archiveName)
	if err != nil {
		log.Printf("Could not load segment list: %v\n", err)
		os.Exit(1)
	}

	// create data directories
	dataDir, err := commoncrawl.CreateDataDir(defaultDir)
	if err != nil {
		log.Printf("Could not create data directory: %v\n", err)
		os.Exit(1)
	}

	for i := 0; i < len(segmentList); i++ {

		// select segment to import
		segment, err := commoncrawl.SelectSegmentToImport(segmentList)
		if err != nil {
			log.Printf("Could not select segment to import: %v\n", err)
			os.Exit(1)
		}

		// parse only unfinished segments
		if segment.ImportEnded == nil {
			fmt.Printf("Importing segment %s\n", segment.Segment)
			importSegment(segment, dataDir, &segmentList, maxThreads, &maxWatFiles)
		}
	}

	//	fmt.Println("Importing segment: ", segment)
	//	fmt.Println("dataDir: ", dataDir)
}

func importSegment(segment commoncrawl.WatSegment, dataDir commoncrawl.DataDir, segmentList *[]commoncrawl.WatSegment, maxThreads int, maxWatFiles *int) {
	var err error

	guard := make(chan struct{}, maxThreads) // limits the number of goroutines running at once
	var wg sync.WaitGroup

	// save info that segment was started
	err = commoncrawl.UpdateSegmentImportStart(segmentList, segment.Segment)
	if err != nil {
		panic(fmt.Sprintf("%s: %v", segment.Segment, err))
	}

	for _, watFile := range segment.WatFiles {

		// ignore imported files
		if watFile.Imported != nil {
			continue
		}

		linkFile := dataDir.TmpDir + "/" + segment.Segment + linkDir + watFile.Number + extensionTxtGz
		err := fileutils.CreateDataDirectory(filepath.Dir(linkFile))
		if err != nil {
			panic(fmt.Sprintf("Failed to create link file: %v", err))
		}
		pageFile := dataDir.TmpDir + "/" + segment.Segment + "/page/" + watFile.Number + extensionTxtGz
		if savePageData == true {
			err = fileutils.CreateDataDirectory(filepath.Dir(pageFile))
			if err != nil {
				panic(fmt.Sprintf("Failed to create page file: %v", err))
			}
		}

		recordWatFile := dataDir.TmpDir + "/wat/" + filepath.Base(watFile.Path)

		if fileutils.FileExists(linkFile) {
			// update segmentList with imported files info
			err = commoncrawl.UpdateSegmentLinkImportStatus(segmentList, segment.Segment, recordWatFile)
			if err != nil {
				panic(fmt.Sprintf("%s: %v", segment.Segment, err))
			}
			continue
		}

		if *maxWatFiles <= 0 {
			continue
		}

		*maxWatFiles--

		err = fileutils.CreateDataDirectory(filepath.Dir(linkFile))
		if err != nil {
			panic(fmt.Sprintf("Failed to create file: %v", err))
		}

		err = fileutils.CreateDataDirectory(filepath.Dir(recordWatFile))
		if err != nil {
			panic(fmt.Sprintf("Failed to create file: %v", err))
		}
		if !fileutils.FileExists(recordWatFile) {
			err := fileutils.DownloadFile("https://data.commoncrawl.org/"+watFile.Path, recordWatFile, 1)
			if err != nil {
				log.Fatalf("Could not load WAT file %s: %v", watFile.Path, err)
			}
		}

		fmt.Println("Importing file: ", recordWatFile)

		wg.Add(1)
		// Before starting the goroutine, we insert an empty struct into the guard channel.
		// If the channel is already full (meaning we have 'maxGoroutines' goroutines running),
		// this will block until one of the running goroutines finishes and reads from the channel.
		guard <- struct{}{}
		go func(recordFile string, linkFile string, pageFile string) {
			defer wg.Done()            // Signal the WaitGroup that the goroutine is done after it finishes
			defer func() { <-guard }() // Release the guard when the goroutine is done

			err = commoncrawl.ParseWatByLine(recordWatFile, linkFile, pageFile, savePageData)
			if err != nil {
				log.Fatalf("Could not open WAT file: %v", err)
			}

			// save info that this file was parsed
			err = commoncrawl.UpdateSegmentLinkImportStatus(segmentList, segment.Segment, recordWatFile)
			if err != nil {
				panic(fmt.Sprintf("%s: %v", segment.Segment, err))
			}

			err = os.Remove(recordFile)
			if err != nil {
				log.Fatalf("Could not delete file: %v", err)
			}
		}(recordWatFile, linkFile, pageFile)

	}
	wg.Wait() // This will block until all goroutines have called wg.Done()

	// sort the file
	watFilesLeftQty := commoncrawl.CountFilesInSegmentToProcess(segment)
	segmentSorted := dataDir.LinksDir + "/_sort_" + strconv.Itoa(segment.SegmentID) + extensionTxtGz
	fmt.Println(watFilesLeftQty)
	if !fileutils.FileExists(segmentSorted) && watFilesLeftQty == 0 {
		// just save sorted data in gzip file and exit
		if stopOnSort == true {
			err = sortOutFilesWithBashGz(segmentSorted, dataDir.TmpDir+"/"+segment.Segment+linkDir)
			if err != nil {
				log.Fatalf("Could not sort file: %v", err)
			}
			// TODO also delete link directory and segment if it is empty
			err = deleteWatPreProcessed(dataDir.TmpDir + "/" + segment.Segment + linkDir)
			if err != nil {
				log.Fatalf("Could not delete WAT processed files: %v", err)
			}
			// save info that segment was finished
			err = commoncrawl.UpdateSegmentImportEnd(segmentList, segment.Segment)
			if err != nil {
				panic(fmt.Sprintf("%s: %v", segment.Segment, err))
			}

		}
		/* TODO: this must be moved to separate process
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
		*/
	}
}

// isCorrectArchiveFormat checks if the archive name is in the correct format
func isCorrectArchiveFormat(s string) bool {
	pattern := `^CC-MAIN-\d{4}-\d{2}$`
	match, _ := regexp.MatchString(pattern, s)
	return match
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

// setDataDirectory set directory for datafiles
func setDataDirectory() string {
	envVar := "GLOBALLINKS_DATAPATH"
	defaultVal := "data"

	dataDir := os.Getenv(envVar)
	if dataDir == "" {
		return defaultVal
	}

	return dataDir
}

// sortOutFilesWithBashGz - sort the file with bash sort and save as gz with segment in name - you can use these segments to move pre processed data to other server
func sortOutFilesWithBashGz(segmentSortedFile string, segmentLinksDir string) error {
	cmdStr := "zcat " + segmentLinksDir + "/*.txt.gz | sort -u -S 2G | gzip > " + segmentSortedFile

	// Execute the command
	cmd := exec.Command("bash", "-c", cmdStr)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return err
}

// split data into many files sorted by domain names
//
//nolint:unused
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

			if err := os.MkdirAll(dirPath, 0o755); err != nil {
				return err
			}

			fileName := dirPath + "/" + prefix + ".gz"
			currentFile, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
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
