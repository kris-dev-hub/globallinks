package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/kris-dev-hub/globallinks/pkg/healthcheck"

	"github.com/klauspost/compress/gzip"

	"github.com/kris-dev-hub/globallinks/pkg/commoncrawl"
	"github.com/kris-dev-hub/globallinks/pkg/fileutils"
)

const (
	savePageData     = false // collect and parse page data
	lowDiscSpaceMode = false // encrypt tmp files to save disc space during sorting, requires lzop installed
	healthCheckMode  = true  // enable health check api to monitor application on port 3005: http://localhost:3005/health
)

const (
	extensionTxtGz = ".txt.gz"
	linkDir        = "/link/"
	pageDir        = "/page/"
)

// FileLinkCompacted - compacted link file
type FileLinkCompacted struct {
	LinkDomain    string
	LinkSubDomain string
	LinkPath      string
	LinkRawQuery  string
	LinkScheme    string
	PageHost      string
	PagePath      string
	PageRawQuery  string
	PageScheme    string
	LinkText      string
	NoFollow      int
	NoIndex       int
	DateFrom      string
	DateTo        string
	IP            string
	Qty           int
}

func main() {
	var err error
	var archiveName string
	var segmentsToImport []int

	if len(os.Args) == 4 && os.Args[1] == "compacting" {
		fmt.Println("compacting")
		err = aggressiveCompacting(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Println("Aggressive compacting failed: " + err.Error())
			os.Exit(1)
		}
		os.Exit(0)
	}

	if len(os.Args) < 2 {
		fmt.Println("No archive name or segment specified. Example: ./importer CC-MAIN-2020-24 <num_of_wat_to_import> <num_of_threads> <optional_segment_list>")
		os.Exit(1)
	}

	if !commoncrawl.IsCorrectArchiveFormat(os.Args[1]) {
		fmt.Println("Invalid archive name")
		os.Exit(1)
	}

	if len(os.Args) > 2 {
		numWatFiles, err := strconv.Atoi(os.Args[2])
		if err != nil {
			numWatFiles = 1
		}
		os.Setenv("GLOBALLINKS_MAXWATFILES", strconv.Itoa(numWatFiles))
	}

	if len(os.Args) > 3 {
		numThreads, err := strconv.Atoi(os.Args[3])
		if err != nil {
			numThreads = 1
		}
		os.Setenv("GLOBALLINKS_MAXTHREADS", strconv.Itoa(numThreads))
	}

	if len(os.Args) > 4 {
		commandLineSegments := os.Args[4]
		segmentsToImport, err = parseSegmentInput(commandLineSegments)
		if err != nil {
			fmt.Println("Invalid segment input: " + err.Error())
			os.Exit(1)
		}

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

	// update information about imported segments
	commoncrawl.ValidateSegmentImportEndAtStart(&segmentList, dataDir, extensionTxtGz)

	fmt.Printf("Importing %d segments\n", len(segmentList))

	if len(segmentsToImport) > 0 {
		for _, segmentID := range segmentsToImport {

			// select only segments from command line
			segment, err := commoncrawl.SelectSegmentByID(segmentList, segmentID)
			if err != nil {
				log.Printf("Could not select segment to import: %v\n", err)
				os.Exit(0)
			}

			// parse only unfinished segments
			if segment.ImportEnded == nil && maxWatFiles > 0 {
				fmt.Printf("Importing segment %s\n", segment.Segment)
				importSegment(segment, dataDir, &segmentList, maxThreads, &maxWatFiles)
			}
		}
		os.Exit(0)
	}

	// allow to monitor script health on external servers
	if healthCheckMode == true {
		// init all the routes
		router := healthcheck.InitRoutes()

		// start http server in a new goroutine
		go func() {
			// start http server
			if err := http.ListenAndServe(":3005", router); err != nil {
				fmt.Println("Failed to set up server")
				panic(err)
			}
		}()
	}

	for i := 0; i < len(segmentList); i++ {

		// select segment to import
		segment, err := commoncrawl.SelectSegmentToImport(segmentList)
		if err != nil {
			log.Printf("Could not select segment to import: %v\n", err)
			os.Exit(0)
		}

		// parse only unfinished segments
		if segment.ImportEnded == nil && maxWatFiles > 0 {
			fmt.Printf("Importing segment %s\n", segment.Segment)
			importSegment(segment, dataDir, &segmentList, maxThreads, &maxWatFiles)
		}
	}
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
			err := fileutils.DownloadFile("https://data.commoncrawl.org/"+watFile.Path, recordWatFile, 2)
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

	// sort & compact the links and pages files
	watFilesLeftQty := commoncrawl.CountFilesInSegmentToProcess(segment)
	if watFilesLeftQty == 0 {
		err = compactSegmentData(segment, dataDir, segmentList)
		if err != nil {
			panic(fmt.Sprintf("%s: %v", segment.Segment, err))
		}
	}
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
	maxVal := 100000

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
	cmdStr := "zcat " + segmentLinksDir + "/*.txt.gz | sort -u -S 1G | gzip > " + segmentSortedFile
	if lowDiscSpaceMode == true {
		// this solves disc problem on VPS servers at cost of sorting performance
		cmdStr = "zcat " + segmentLinksDir + "/*.txt.gz | sort --compress-program=lzop -u -S 1G | gzip > " + segmentSortedFile
	}

	// Execute the command
	cmd := exec.Command("bash", "-c", cmdStr)
	err := cmd.Run()
	if err != nil {
		return err
	}
	return err
}

// aggressiveCompacting - compact data from sort file to new compacted file saving space leave only strongest link from each host and number of similar links
func aggressiveCompacting(segmentSortedFile string, linkSegmentCompacted string) error {
	segmentCompactedFile := linkSegmentCompacted

	// load data from sort file
	const maxCapacityScanner = 3 * 1024 * 1024 // 3*1MB

	// Open the .gz file
	file, err := os.Open(segmentSortedFile)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a gzip Reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("error creating gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Use a bufio.Scanner to read the file line by line
	scanner := bufio.NewScanner(gzReader)
	// create buffer to avoid going over token size
	buf := make([]byte, maxCapacityScanner)
	scanner.Buffer(buf, maxCapacityScanner)

	// Read each line and append to the records slice
	line := ""

	fileLink := FileLinkCompacted{}
	finalLink := FileLinkCompacted{}

	linksToSave := make([]FileLinkCompacted, 0, 10000)

	i := 0
	for scanner.Scan() {
		i++
		line = scanner.Text()
		parts := strings.Split(line, "|")
		if len(parts) != 14 {
			// Invalid line - skip
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
		fileLink.DateTo = parts[12]
		fileLink.IP = parts[13]
		fileLink.Qty = 1

		saveLink := compareRecords(fileLink, &finalLink)
		if saveLink {
			if finalLink.LinkDomain != "" {
				linksToSave = append(linksToSave, finalLink)
			}
			finalLink = fileLink
		}
		// save file every 10000 lines and reset linksToSave
		if i >= 10000 {
			i = 0
			err = saveFinalLinksToFile(segmentCompactedFile, linksToSave)
			if err != nil {
				return err
			}
			linksToSave = make([]FileLinkCompacted, 0, 10000)
		}
	}

	// save final part of data
	if len(linksToSave) > 0 {
		err = saveFinalLinksToFile(segmentCompactedFile, linksToSave)
		if err != nil {
			return err
		}
	}
	return err
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

	err = fileutils.DeleteDirectoryIfEmpty(dirPath)
	if err != nil {
		return err
	}

	return nil
}

// compactSegmentData - sort the file with bash sort and save as gz with segment in name - you can use these segments to move pre-processed data to other server
func compactSegmentData(segment commoncrawl.WatSegment, dataDir commoncrawl.DataDir, segmentList *[]commoncrawl.WatSegment) error {
	var err error

	linkSegmentSorted := dataDir.LinksDir + "/sort_" + strconv.Itoa(segment.SegmentID) + extensionTxtGz
	pageSegmentSorted := dataDir.PagesDir + "/sort_" + strconv.Itoa(segment.SegmentID) + extensionTxtGz
	linkSegmentCompacted := dataDir.LinksDir + "/compact_" + strconv.Itoa(segment.SegmentID) + extensionTxtGz

	if !fileutils.FileExists(linkSegmentSorted) {

		err = sortOutFilesWithBashGz(linkSegmentSorted, dataDir.TmpDir+"/"+segment.Segment+linkDir)
		if err != nil {
			return fmt.Errorf("could not sort file: %v", err)
		}
		err = deleteWatPreProcessed(dataDir.TmpDir + "/" + segment.Segment + linkDir)
		if err != nil {
			return fmt.Errorf("could not delete WAT processed files: %v", err)
		}
		if savePageData == true {
			err = sortOutFilesWithBashGz(pageSegmentSorted, dataDir.TmpDir+"/"+segment.Segment+pageDir)
			if err != nil {
				return fmt.Errorf("could not sort file: %v", err)
			}
			err = deleteWatPreProcessed(dataDir.TmpDir + "/" + segment.Segment + pageDir)
			if err != nil {
				return fmt.Errorf("could not delete WAT processed files: %v", err)
			}
		}
		err = fileutils.DeleteDirectoryIfEmpty(dataDir.TmpDir + "/" + segment.Segment)
		if err != nil {
			return fmt.Errorf("could not delete tmp directories: %v", err)
		}

		err = aggressiveCompacting(linkSegmentSorted, linkSegmentCompacted)
		if err != nil {
			return fmt.Errorf("could not compact file: %v", err)
		}
		err = os.Remove(linkSegmentSorted)
		if err != nil {
			return fmt.Errorf("could not delete file: %v", err)
		}

		// save info that segment was finished
		err = commoncrawl.UpdateSegmentImportEnd(segmentList, segment.Segment)
		if err != nil {
			return fmt.Errorf("%v", err)
		}
	}

	return nil
}

// compareRecords - compare compacted record and next record return true if we should save current record, also update compacted with information from current record when we don't have to save it
func compareRecords(fileLink FileLinkCompacted, finalLink *FileLinkCompacted) bool {
	if fileLink.LinkDomain == "" {
		return true
	}

	// if both record are different return true to save current link
	if fileLink.LinkDomain != "" && (fileLink.LinkDomain != finalLink.LinkDomain || fileLink.LinkSubDomain != finalLink.LinkSubDomain || fileLink.LinkPath != finalLink.LinkPath || fileLink.LinkRawQuery != finalLink.LinkRawQuery || fileLink.PageHost != finalLink.PageHost) {
		return true
	}

	// ignore nofollow link if we have dofollow
	if finalLink.NoFollow == 0 && fileLink.NoFollow == 1 {
		return false
	}

	// update date from and date to
	if fileLink.DateFrom < finalLink.DateFrom {
		finalLink.DateFrom = fileLink.DateFrom
	}
	if fileLink.DateTo > finalLink.DateTo {
		finalLink.DateTo = fileLink.DateTo
	}

	// take ip from latest record
	finalLink.IP = fileLink.IP

	if fileLink.PagePath != finalLink.PagePath || fileLink.PageRawQuery != finalLink.PageRawQuery {
		if len(fileLink.PagePath) < len(finalLink.PagePath) {
			// select shortest path if query is the same or shorter
			if len(fileLink.PageRawQuery) <= len(finalLink.PageRawQuery) {
				finalLink.PageRawQuery = fileLink.PageRawQuery
				finalLink.PagePath = fileLink.PagePath
			}
		} else if len(fileLink.PagePath) == len(finalLink.PagePath) && len(fileLink.PageRawQuery) < len(finalLink.PageRawQuery) {
			// select shortest query if path is the same
			finalLink.PageRawQuery = fileLink.PageRawQuery
		}
		finalLink.Qty++
		//		fmt.Printf("%d", finalLink.Qty)
	}

	return false
}

// saveFinalLinksToFile - save final compacted links to file
func saveFinalLinksToFile(segmentCompactedFile string, linksToSave []FileLinkCompacted) error {
	// Open the file for writing, create it if not exists, append to it if it does.
	fileOut, err := os.OpenFile(segmentCompactedFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return err
	}
	defer fileOut.Close()
	writer := gzip.NewWriter(fileOut)

	for _, finalLinkToSave := range linksToSave {
		// ignore empty records created while building linkToSave
		if finalLinkToSave.LinkDomain == "" {
			continue
		}
		_, err = writer.Write([]byte(fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%s|%s|%s|%d\n",
			finalLinkToSave.LinkDomain,
			finalLinkToSave.LinkSubDomain,
			finalLinkToSave.LinkPath,
			finalLinkToSave.LinkRawQuery,
			finalLinkToSave.LinkScheme,
			finalLinkToSave.PageHost,
			finalLinkToSave.PagePath,
			finalLinkToSave.PageRawQuery,
			finalLinkToSave.PageScheme,
			finalLinkToSave.LinkText,
			finalLinkToSave.NoFollow,
			finalLinkToSave.NoIndex,
			finalLinkToSave.DateFrom,
			finalLinkToSave.DateTo,
			finalLinkToSave.IP,
			finalLinkToSave.Qty,
		)))
		if err != nil {
			return err
		}

	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

// parseSegmentInput - parse segment input from command line to generate list of segmentID to import
func parseSegmentInput(segments string) ([]int, error) {
	var results []int
	parts := strings.Split(segments, ",")
	if len(parts) > 1 {
		for _, part := range parts {
			result, err := strconv.Atoi(part)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		return results, nil
	}

	if strings.Contains(segments, "-") {
		rangeParts := strings.Split(segments, "-")
		if len(rangeParts) != 2 {
			return nil, fmt.Errorf("invalid range: %s", segments)
		}
		start, err := strconv.Atoi(rangeParts[0])
		if err != nil {
			return nil, err
		}
		end, err := strconv.Atoi(rangeParts[1])
		if err != nil {
			return nil, err
		}
		if start > end {
			return nil, fmt.Errorf("invalid range: %s", segments)
		}
		for i := start; i <= end; i++ {
			results = append(results, i)
		}
		return results, nil
	}

	// Handling a single number
	number, err := strconv.Atoi(segments)
	if err != nil {
		return nil, err
	}
	results = append(results, number)

	return results, nil
}
