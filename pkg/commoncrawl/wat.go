/*
Package commoncrawl - package to parse commoncrawl wat files and save links and pages to files, sorted and split for further processing
*/
package commoncrawl

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-farm"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/gzip" // faster than std gzip library, 0.7 sec faster parsing 1M lines
	"github.com/kris-dev-hub/globallinks/pkg/config"
	"github.com/kris-dev-hub/globallinks/pkg/fileutils"
	"github.com/tidwall/gjson"
	"golang.org/x/net/publicsuffix"
)

// URLRecord - Define a struct to represent a URL record
type URLRecord struct {
	URL       *string
	Scheme    *string
	Host      *string
	Path      *string
	RawQuery  *string
	Fragment  *string
	Domain    *string
	SubDomain *string
	Text      *string // optional text from link
	NoFollow  *int
}

// WatPage - Define a struct to represent a wat page
type WatPage struct {
	IP            *string
	Imported      *string
	Title         *string
	NoIndex       *int
	NoFollow      *int
	InternalLinks int
	ExternalLinks int
	URLRecord     *URLRecord
	Links         []URLRecord
}

// FilePage - Define a struct to represent a page in file
type FilePage struct {
	Host          string
	Path          string
	RawQuery      string
	Scheme        string
	Title         string
	IP            string
	Imported      string
	InternalLinks int
	ExternalLinks int
	NoIndex       int
}

// FileLink - Define a struct to represent a link in file
type FileLink struct {
	LinkHost      string
	LinkPath      string
	LinkRawQuery  string
	LinkScheme    string
	LinkText      string
	NoFollow      int
	NoIndex       int
	Imported      string
	IP            string
	PageHash      string
	LinkDomain    string
	LinkSubDomain string
}

// SortFileLinkByFields - structure used to sort links
type SortFileLinkByFields struct {
	Key       string
	Domain    string
	Subdomain string
	Path      string
}

// WatFile - Define a struct to represent a wat file
type WatFile struct {
	Number   string     `json:"number"`
	Path     string     `json:"path"`
	Imported *time.Time `json:"imported"`
}

// WatSegment - Define a struct to represent a segment
type WatSegment struct {
	Archive       string     `json:"archive"`
	Segment       string     `json:"segment"`
	SegmentID     int        `json:"segment_id"`
	WatFiles      []WatFile  `json:"wat_files"`
	ImportStarted *time.Time `json:"import_started"`
	ImportEnded   *time.Time `json:"import_ended"`
}

// DataDir - Define a struct to represent a data directory, tmp, links, pages folders
type DataDir struct {
	DataDir  string `json:"data_dir"`
	TmpDir   string `json:"tmp_dir"`
	LinksDir string `json:"links_dir"`
	PagesDir string `json:"pages_dir"`
}

// use to validate if host is not an IP address. precompile it here to make it faster and avoid compiling it every time
// saves around 1s per 1M lines on one i5-9300H core
var ipRegex = regexp.MustCompile(`^(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)(?:\.(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]\d|\d)){3}$`)

// initialize a map for fast lookups - it will be used to ignore certain domains and extensions
var (
	ignoreDomains      = map[string]bool{}
	ignoreDomainsMutex sync.RWMutex
)

var (
	fileExtensions      = map[string]bool{}
	fileExtensionsMutex sync.RWMutex
)

// domain cache to lower amount of publicsuffix.EffectiveTLDPlusOne - 500ms faster per 1M lines
var (
	domainCache      = map[string]string{}
	domainCacheMutex sync.RWMutex
)

const debugTestMode = true // import only 20 wat files in 2 segments. To verify all mechanisms/

// InitImport - initialize import by downloading segments file and extracting segments into segmentList
func InitImport(archiveName string) ([]WatSegment, error) {
	var err error
	var segmentList []WatSegment

	// download segments file
	url := "https://data.commoncrawl.org/crawl-data/" + archiveName + "/wat.paths.gz"

	// download file
	resp, err := http.Get(url)
	if err != nil {
		return segmentList, err
	}
	defer resp.Body.Close()

	// extract gzip
	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return segmentList, err
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)
	segments := make(map[string][]string)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "/")
		if len(parts) > 4 {
			segment := parts[3]           // Extracting the segment part
			if segments[segment] == nil { // If the segment is not in the map, create a new slice
				segments[segment] = make([]string, 0)
			}
			segments[segment] = append(segments[segment], line) // Append the line to the slice
		}
	}

	if err = scanner.Err(); err != nil {
		return segmentList, err
	}

	fileNumber := ""
	segmentList = make([]WatSegment, 0, len(segments))
	j := 0
	for segment, fileList := range segments {
		// this enable debugTestMode limit
		if debugTestMode == true && j >= 2 {
			break
		}
		watFileList := make([]WatFile, 0, len(fileList))
		i := 0
		for _, file := range fileList {
			// this enable debugTestMode limit
			if debugTestMode == true && i >= 20 {
				break
			}
			fileNumber, err = ExtractWatFileNumber(file)
			if err != nil {
				return segmentList, err
			}
			watFileList = append(watFileList, WatFile{Path: file, Number: fileNumber})
			i++
		}
		segmentID, err := strconv.Atoi(strings.Split(segment, ".")[1])
		if err != nil {
			return segmentList, errors.New("error converting segment to segment_id to int")
		}
		segmentList = append(segmentList, WatSegment{Segment: segment, SegmentID: segmentID, Archive: archiveName, WatFiles: watFileList})
		j++
	}

	return segmentList, nil
}

// CreateDataDir - create data directory and tmp, links, pages folders
func CreateDataDir(defaultDir string) (DataDir, error) {
	var err error
	dataDir := DataDir{defaultDir, defaultDir + "/tmp", defaultDir + "/links", defaultDir + "/pages"}

	err = fileutils.CreateDataDirectory(dataDir.DataDir)
	if err != nil {
		log.Fatalf("Could not create data directory: %v", err)
	}

	err = fileutils.CreateDataDirectory(dataDir.TmpDir)
	if err != nil {
		log.Fatalf("Could not create tmp directory: %v", err)
	}

	err = fileutils.CreateDataDirectory(dataDir.LinksDir)
	if err != nil {
		log.Fatalf("Could not create tmp directory: %v", err)
	}

	err = fileutils.CreateDataDirectory(dataDir.PagesDir)
	if err != nil {
		log.Fatalf("Could not create tmp directory: %v", err)
	}

	return dataDir, nil
}

// ParseWatByLine - parse wat file line by line and store links in file
func ParseWatByLine(filePath string, linkFile string, pageFile string, savePage bool) error {
	// prepare ignore domains and extensions map - load only when empty
	if len(ignoreDomains) == 0 {
		ignoreDomainsMutex.Lock()
		ignoreDomains = createDomainMap(config.IgnoreDomains)
		ignoreDomainsMutex.Unlock()
	}
	if len(fileExtensions) == 0 {
		fileExtensionsMutex.Lock()
		fileExtensions = createFileExtensionMap(config.FileExtensions)
		fileExtensionsMutex.Unlock()
	}

	// clear domain cache
	domainCacheMutex.Lock()
	domainCache = map[string]string{}
	domainCacheMutex.Unlock()

	pageMap := make(map[string]FilePage)
	linkMap := make(map[string]FileLink)

	const maxCapacityScanner = 3 * 1024 * 1024 // 3*1MB

	// Open the .gz file
	file, err := os.Open(filePath)
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

	urlRecord := URLRecord{}

	validPage := false

	for scanner.Scan() {
		line = scanner.Text()
		if strings.HasPrefix(line, "WARC-Target-URI: http") {

			urlRecord = URLRecord{}
			sourceURL := strings.TrimSpace(line[17:])
			validRecord := buildURLRecord(sourceURL, &urlRecord)
			if !validRecord {
				validPage = false
				continue
			}
			if !verifyRecordQuality(&urlRecord) {
				validPage = false
				continue
			}

			validPage = true
		}

		// read content of record - only when we have proper record header - validPage = true
		if validPage && strings.HasPrefix(line, "{") && strings.Contains(line, "href") {
			validPage = false
			content := readPageContent(line, &urlRecord)
			if content == nil {
				continue
			}

			if len(content.Links) > 0 {
				// save page info to file
				filePage := FilePage{
					Host:          *content.URLRecord.Host,
					Path:          *content.URLRecord.Path,
					RawQuery:      *content.URLRecord.RawQuery,
					Scheme:        *content.URLRecord.Scheme,
					Title:         strings.ReplaceAll(*content.Title, "|", " "),
					IP:            *content.IP,
					Imported:      *content.Imported,
					InternalLinks: content.InternalLinks,
					ExternalLinks: content.ExternalLinks,
					NoIndex:       *content.NoIndex,
				}
				pageHash := fmt.Sprintf("%x", farm.Hash64([]byte(*content.URLRecord.Host+*content.URLRecord.Path+*content.URLRecord.RawQuery)))
				pageMap[pageHash] = filePage
				for _, link := range content.Links {
					// write to file
					noFollow := 0
					if link.NoFollow != nil && *link.NoFollow == 1 {
						noFollow = 1
					}

					fileLink := FileLink{
						LinkHost:      *link.Host,
						LinkPath:      *link.Path,
						LinkRawQuery:  *link.RawQuery,
						LinkScheme:    *link.Scheme,
						LinkText:      strings.ReplaceAll(*link.Text, "|", " "),
						NoFollow:      noFollow,
						NoIndex:       *content.NoIndex,
						Imported:      *content.Imported,
						IP:            *content.IP,
						PageHash:      pageHash,
						LinkDomain:    *link.Domain,
						LinkSubDomain: *link.SubDomain,
					}
					linkHash := fmt.Sprintf("%x", farm.Hash64([]byte(*link.Host+*link.Path+*link.RawQuery+*content.URLRecord.Host+*content.URLRecord.Path+*content.URLRecord.RawQuery)))
					linkMap[linkHash] = fileLink
				}
			}
		}
	}

	// saving link file and reseting linkMap
	err = saveLinkFile(linkFile, linkMap, pageMap)
	if err != nil {
		return err
	}

	if savePage {
		// saving page file and reseting pageMap
		err = savePageFile(pageFile, pageMap)
		if err != nil {
			return err
		}
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning the file: %w", err)
	}

	return nil
}

// readPageContent - read page content from json, get IP, noindex, nofollow, title, links, etc.
func readPageContent(line string, sourceURLRecord *URLRecord) *WatPage {
	var err error

	watPage := WatPage{}
	watPage.URLRecord = sourceURLRecord

	// parse json and reuse if for Get - 100ms faster per 1M lines
	parsedJSON := gjson.Parse(line)

	linksData := parsedJSON.Get("Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Links").String()
	// check if linksData json is not empty
	if len(linksData) < 10 {
		return nil
	}

	ip := parsedJSON.Get("Envelope.WARC-Header-Metadata.WARC-IP-Address").String()
	if ip != "" {
		watPage.IP = &ip
	}

	imported := parsedJSON.Get("Envelope.WARC-Header-Metadata.WARC-Date").String()
	if imported != "" {
		layout := "2006-01-02T15:04:05Z"
		t, err := time.Parse(layout, imported)
		if err == nil {
			imported = t.Format("2006-01-02")
			watPage.Imported = &imported
		}
	}

	title := parsedJSON.Get("Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Title").String()
	watPage.Title = &title

	metas := parsedJSON.Get("Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Metas").String()
	noindex, nofollow := getNoFollowNoIndex(metas)
	watPage.NoIndex = &noindex
	watPage.NoFollow = &nofollow

	// ignore pages with content problems like chinese characters in headers etc., rel canonical problems, etc.
	if !verifyContentQuality(&parsedJSON, &watPage) {
		return nil
	}

	watPage.Links, watPage.InternalLinks, watPage.ExternalLinks, err = parseLinks(linksData, sourceURLRecord, *watPage.NoFollow)
	if err != nil {
		// we ignore broken links data in source document
		return nil
	}

	return &watPage
}

// GetInfoFromMeta returns noindex and nofollow values from meta tags
func getNoFollowNoIndex(metas string) (int, int) {
	// using int instead of bool to use less space in text file
	noindex := 0
	nofollow := 0

	type MetaData struct {
		Name     string `json:"name,omitempty"`
		Content  string `json:"content"`
		Property string `json:"property,omitempty"`
	}

	var metaDataArray []MetaData
	err := jsoniter.Unmarshal([]byte(metas), &metaDataArray)
	if err != nil {
		return noindex, nofollow
	}

	for _, metaData := range metaDataArray {
		if metaData.Name == "robots" {
			if strings.Contains(metaData.Content, "noindex") {
				noindex = 1
			}
			if strings.Contains(metaData.Content, "nofollow") {
				nofollow = 1
			}
		}
	}

	return noindex, nofollow
}

// parseLinks - parse links from json
func parseLinks(links string, sourceURLRecord *URLRecord, pageNoFollow int) ([]URLRecord, int, int, error) {
	var err error
	internalLinks := 0
	externalLinks := 0

	urlRecord := URLRecord{}
	var urlRecords []URLRecord

	type LinkInfo struct {
		Path string `json:"path"`
		URL  string `json:"url"`
		Text string `json:"text"`
		Rel  string `json:"rel"`
	}

	var linksArray []LinkInfo
	err = jsoniter.Unmarshal([]byte(links), &linksArray)
	if err != nil {
		return urlRecords, internalLinks, externalLinks, err
	}

	for _, linkData := range linksArray {
		noFollow := pageNoFollow

		// ignore non A tags
		if linkData.Path != "A@/href" {
			continue
		}
		// ignore links without http, https or //
		if !strings.HasPrefix(linkData.URL, "http") && !strings.HasPrefix(linkData.URL, "//") {
			internalLinks++
			continue
		}
		if strings.HasPrefix(linkData.Rel, "nofollow") {
			noFollow = 1
		}

		urlRecord = URLRecord{
			Text:     &linkData.Text,
			NoFollow: &noFollow,
		}
		validRecord := buildURLRecord(linkData.URL, &urlRecord)
		if !validRecord {
			continue
		}

		// ignore the same hosts
		if *sourceURLRecord.Host == *urlRecord.Host {
			internalLinks++
			continue
		}

		// ignore the same domains
		if *sourceURLRecord.Domain == *urlRecord.Domain {
			externalLinks++
			continue
		}

		if !verifyRecordQuality(&urlRecord) {
			externalLinks++
			continue
		}

		// link is a file so we ignore it
		if urlRecord.Path == nil || isIgnoredExtension(*urlRecord.Path) {
			continue
		}

		if isIgnoredDomain(*urlRecord.Domain) {
			externalLinks++
			continue
		}

		externalLinks++
		urlRecords = append(urlRecords, urlRecord)

	}

	return urlRecords, internalLinks, externalLinks, nil
}

// verifyRecordQuality - verify if record is valid, no blocked TLD, no broken host, no broken query, etc.
func verifyRecordQuality(record *URLRecord) bool {
	// could not find domain
	if record.Domain == nil {
		return false
	}

	// ignore blocked TLD
	if ignoreTLD(*record.Domain) {
		return false
	}
	// validate problems with host
	if !validateHost(*record.Host) {
		return false
	}

	// validate query length. Over 200 is probably garbage
	if record.RawQuery != nil && len(*record.RawQuery) > 200 {
		return false
	}

	// validate if RawQuery contains | char
	if record.RawQuery != nil && strings.Contains(*record.RawQuery, "|") {
		return false
	}

	return true
}

// validateHose - validate host for strange characters and no dots
func validateHost(host string) bool {
	if strings.Contains(host, "%") ||
		strings.Contains(host, "[") ||
		strings.Contains(host, "]") ||
		strings.Contains(host, "=") ||
		strings.Contains(host, "'") ||
		strings.Contains(host, ":") ||
		strings.Contains(host, "*") ||
		strings.Contains(host, "(") ||
		strings.Contains(host, ")") ||
		strings.Contains(host, "<") ||
		strings.Contains(host, ">") ||
		strings.Contains(host, "&") ||
		strings.Contains(host, "!") ||
		strings.Contains(host, "+") ||
		strings.Contains(host, "`") ||
		strings.Contains(host, ",") ||
		strings.Contains(host, "}") ||
		strings.Contains(host, "{") ||
		strings.Contains(host, "$") ||
		strings.Contains(host, "\"") ||
		strings.Contains(host, ":") ||
		strings.Contains(host, ";") {
		return false
	}

	// ignore IP addresses - there is no external validation function to make it faster
	if ipRegex.MatchString(host) {
		return false
	}

	// ignore hosts without "." they are broken
	if !strings.Contains(host, ".") {
		return false
	}
	return true
}

// buildURLRecord - build url record from source url, check domain, path, query, etc.
func buildURLRecord(sourceURL string, urlRecord *URLRecord) bool {
	// ignore url with \n
	if strings.Contains(sourceURL, "\n") {
		return false
	}

	urlRecord.URL = &sourceURL

	// ignore sourceUrl that can't be parsed
	parsedURL, err := url.Parse(sourceURL)
	if err != nil {
		return false
	}

	// ignore path with \n
	if strings.Contains(parsedURL.Path, "\n") {
		return false
	}

	// ignore path with | char
	if strings.Contains(parsedURL.Path, "|") {
		return false
	}

	// add "" to Text when it is empty
	if urlRecord.Text == nil {
		emptyString := ""
		urlRecord.Text = &emptyString
	}

	scheme := setScheme(parsedURL.Scheme)
	urlRecord.Scheme = &scheme

	parsedURL.Host = strings.ToLower(strings.TrimSpace(parsedURL.Host))
	urlRecord.Host = &parsedURL.Host
	if parsedURL.Path == "" {
		parsedURL.Path = "/"
	}
	urlRecord.Path = &parsedURL.Path
	urlRecord.RawQuery = &parsedURL.RawQuery

	// ignore query starting with
	if ignoreQuery(*urlRecord.RawQuery) {
		emptyString := ""
		urlRecord.RawQuery = &emptyString
	}

	urlRecord.Fragment = &parsedURL.Fragment

	// ignore records without known domain
	domainCacheMutex.RLock()
	domain, exists := domainCache[*urlRecord.Host]
	domainCacheMutex.RUnlock()
	if !exists {
		domain, err = publicsuffix.EffectiveTLDPlusOne(*urlRecord.Host)
		if err != nil {
			return false
		}
		domainCacheMutex.Lock()
		domainCache[*urlRecord.Host] = domain
		domainCacheMutex.Unlock()
	}
	urlRecord.Domain = &domain

	subDomain := genSubdomain(urlRecord)
	urlRecord.SubDomain = &subDomain

	return true
}

// Function to convert a slice of domains to a map for fast lookup
func createDomainMap(domains []string) map[string]bool {
	domainMap := make(map[string]bool, len(domains))
	for _, domain := range domains {
		domainMap[domain] = true
	}
	return domainMap
}

// Function to convert a slice of domains to a map for fast lookup
func createFileExtensionMap(extensions []string) map[string]bool {
	fileExtensionsMap := make(map[string]bool, len(extensions))
	for _, extension := range extensions {
		fileExtensionsMap[extension] = true
	}
	return fileExtensionsMap
}

// isIgnoredDomain - ignore certain domains in links
func isIgnoredDomain(domain string) bool {
	ignoreDomainsMutex.RLock()
	_, exists := ignoreDomains[strings.ToLower(domain)]
	ignoreDomainsMutex.RUnlock()
	return exists
}

// isIgnoredExtension - ignore certain extensions in links - saved 700ms per 1M lines
func isIgnoredExtension(path string) bool {
	extension := strings.ToLower(filepath.Ext(path))
	fileExtensionsMutex.RLock()
	_, exists := fileExtensions[extension]
	fileExtensionsMutex.RUnlock()
	return exists
}

// ignoreTLD - ignore Top Level Domains
func ignoreTLD(domain string) bool {
	for _, ext := range config.IgnoreTLD {
		if strings.HasSuffix(strings.ToLower(domain), ext) {
			return true
		}
	}
	return false
}

// ignoreQuery - ignore query starting with
func ignoreQuery(query string) bool {
	for _, ext := range config.IgnoreQuery {
		if strings.HasPrefix(query, ext) {
			return true
		}
	}
	return false
}

// verifyContentQuality - verify if page is valid, noindex, nofollow, canonical, etc.
func verifyContentQuality(parsedJSON *gjson.Result, watPage *WatPage) bool {
	/* TODO: I might consider ignoring only noindex nofollow pages
	//ignore no index no follow pages
	if *watPage.NoIndex == 1 || *watPage.NoFollow == 1 {
		return false
	}
	return true
	*/

	// ignore no index pages
	if *watPage.NoIndex == 1 {
		return false
	}

	// ignore pages with canonical link pointing to other page
	if !checkPageCanonicalLink(parsedJSON, watPage) {
		return false
	}
	return true
}

// checkPageCanonicalLink - check if page has canonical link and if it is pointing to the same page and for other potential issues connected with it
func checkPageCanonicalLink(parsedJSON *gjson.Result, watPage *WatPage) bool {
	type HeadLinkData struct {
		Path string `json:"path"`
		URL  string `json:"url"`
		Rel  string `json:"rel"`
		Type string `json:"type"`
	}

	var links []HeadLinkData

	headLinksData := parsedJSON.Get("Envelope.Payload-Metadata.HTTP-Response-Metadata.HTML-Metadata.Head.Link").String()
	if len(headLinksData) > 0 {

		err := jsoniter.Unmarshal([]byte(headLinksData), &links)
		if err != nil {
			return false
		}

		for _, link := range links {
			if link.Rel == "canonical" && link.URL != "" {
				// parse canonical url
				parsedURL, err := url.Parse(link.URL)
				if err != nil {
					// ignore the page if it has broken canonical link
					// TODO: I might rethink this. Need to check this on more real data
					return false
				}

				// ignore pages with canonical pointing to other host and then analyze only path
				if strings.HasPrefix(link.URL, "http") || strings.HasPrefix(link.URL, "//") {
					// ignore pages with canonical pointing to other host
					if parsedURL.Host != *watPage.URLRecord.Host {
						return false
					}

					// change URL to path since it is the same host
					link.URL = parsedURL.Path
				}

				// standardize / path
				if link.URL == "" {
					link.URL = "/"
				}

				// ignore pages with canonical pointing to other path
				if link.URL != *watPage.URLRecord.Path {
					// TODO: we could eventually change source page path to canonical path. Need to check this on more real data
					return false
				}

				// ignore pages with canonical pointing to other query or no query
				if watPage.URLRecord.RawQuery != nil && *watPage.URLRecord.RawQuery != "" {
					// TODO: we could eventually change source page query to empty query if we have such on canonical query. Need to check this on more real data
					return false
				}
			}
		}
	}

	return true
}

// setScheme - set scheme to 0, 1 or 2 depending on http, https or other
func setScheme(scheme string) string {
	if scheme == "https" {
		return "2"
	}
	if scheme == "http" {
		return "1"
	}
	return "0"
}

// ExtractWatFileNumber extracts the number before the .warc.wat.gz extension.
func ExtractWatFileNumber(filename string) (string, error) {
	// This regex pattern looks for any digits followed by '.warc.wat.gz' at the end of the string.
	pattern := regexp.MustCompile(`-(\d+)\.warc\.wat\.gz$`)
	matches := pattern.FindStringSubmatch(filename)

	if len(matches) < 2 {
		// If there are no matches or not enough submatches, return an error.
		return "", fmt.Errorf("no number found in the filename")
	}

	// The first submatch is the entire match, and the second one is the captured group.
	numberStr := matches[1]

	return numberStr, nil
}

// savePageFile - save pages info to file
func savePageFile(pageFile string, pageMap map[string]FilePage) error {
	fileOutPage, err := os.OpenFile(pageFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		fmt.Printf("Error opening page file: %s\n", err)
		return err
	}
	defer fileOutPage.Close()
	writerPage := gzip.NewWriter(fileOutPage)

	for _, content := range pageMap {
		_, err = writerPage.Write([]byte(fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n",
			content.Host,
			content.Path,
			content.RawQuery,
			content.Scheme,
			content.Title,
			content.IP,
			content.Imported,
			strconv.Itoa(content.InternalLinks),
			strconv.Itoa(content.ExternalLinks),
			strconv.Itoa(content.NoIndex),
		)))
		if err != nil {
			return err
		}
	}

	err = writerPage.Close()
	if err != nil {
		return err
	}

	return nil
}

// saveLinkFile - save links info to file
func saveLinkFile(linkFile string, linkMap map[string]FileLink, pageMap map[string]FilePage) error {
	sortableFileLinkSlice := sortFileLink(linkMap)

	// Open the file for writing, create it if not exists, append to it if it does.
	fileOut, err := os.OpenFile(linkFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return err
	}
	defer fileOut.Close()
	writer := gzip.NewWriter(fileOut)

	for _, item := range sortableFileLinkSlice {
		content := linkMap[item.Key]

		page := pageMap[content.PageHash]

		_, err = writer.Write([]byte(fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%s|%s\n",
			content.LinkDomain,
			content.LinkSubDomain,
			content.LinkPath,
			content.LinkRawQuery,
			content.LinkScheme,
			page.Host,
			page.Path,
			page.RawQuery,
			page.Scheme,
			content.LinkText,
			content.NoFollow,
			page.NoIndex,
			page.Imported,
			page.IP,
		)))
		if err != nil {
			return err
		}

	}

	// Flush the buffer to disk
	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

// sortFileLink - sort link map by domain, subdomain and path
func sortFileLink(linkMap map[string]FileLink) []SortFileLinkByFields {
	var sortableSlice []SortFileLinkByFields
	for key, value := range linkMap {
		sortableSlice = append(sortableSlice, SortFileLinkByFields{Key: key, Domain: value.LinkDomain, Subdomain: value.LinkSubDomain, Path: value.LinkPath})
	}

	sort.Slice(sortableSlice, func(i, j int) bool {
		if sortableSlice[i].Domain == sortableSlice[j].Domain {
			if sortableSlice[i].Subdomain == sortableSlice[j].Subdomain {
				return sortableSlice[i].Path < sortableSlice[j].Path
			}
			return sortableSlice[i].Subdomain < sortableSlice[j].Subdomain
		}
		return sortableSlice[i].Domain < sortableSlice[j].Domain
	})

	return sortableSlice
}

// genSubdomain - generate subdomain from host and domain
func genSubdomain(urlRecord *URLRecord) string {
	var subDomain string
	if *urlRecord.Host == *urlRecord.Domain {
		subDomain = ""
	} else {
		subDomain = strings.TrimSuffix(*urlRecord.Host, "."+*urlRecord.Domain)
	}
	return subDomain
}

// CountFilesInSegmentToProcess - count files in segment that still need to be processed
func CountFilesInSegmentToProcess(segment WatSegment) int {
	toProcessQty := 0

	// return number of files in segment
	for _, file := range segment.WatFiles {
		if file.Imported == nil {
			toProcessQty++
		}
	}

	return toProcessQty
}

// SelectSegmentToImport - select segment to import
func SelectSegmentToImport(segmentList []WatSegment) (WatSegment, error) {
	// sort segment by segment name
	sort.Slice(segmentList, func(i, j int) bool {
		return segmentList[i].SegmentID < segmentList[j].SegmentID
	})

	for _, segment := range segmentList {
		if segment.ImportEnded == nil {
			return segment, nil
		}
	}

	return WatSegment{}, errors.New("no segment to import")
}

// UpdateSegmentLinkImportStatus - update segment link import status
func UpdateSegmentLinkImportStatus(segmentList *[]WatSegment, segmentName string, filePath string) error {
	fileID, err := ExtractWatFileNumber(filePath)
	if err != nil {
		return fmt.Errorf("error extracting file number: %w", err)
	}

	for idSegment, segment := range *segmentList {
		if segment.Segment == segmentName {
			for idWatFile, file := range segment.WatFiles {
				if file.Number == fileID {
					now := time.Now()
					(*segmentList)[idSegment].WatFiles[idWatFile].Imported = &now
					return nil
				}
			}
		}
	}
	return errors.New("segment or link not found")
}

// UpdateSegmentImportStart - update segment import status
func UpdateSegmentImportStart(segmentList *[]WatSegment, segmentName string) error {
	for idSegment, segment := range *segmentList {
		if segment.Segment == segmentName {
			if segment.ImportStarted == nil {
				now := time.Now()
				(*segmentList)[idSegment].ImportStarted = &now
			}
			return nil
		}
	}
	return errors.New("segment not found")
}

// UpdateSegmentImportEnd - update segment mport status
func UpdateSegmentImportEnd(segmentList *[]WatSegment, segmentName string) error {
	for idSegment, segment := range *segmentList {
		if segment.Segment == segmentName {
			now := time.Now()
			(*segmentList)[idSegment].ImportEnded = &now
			return nil
		}
	}
	return nil
}

// ValidateSegmentImportEndAtStart - validate segment import status
func ValidateSegmentImportEndAtStart(segmentList *[]WatSegment, dataDir DataDir, extensionTxtGz string) {
	for i, segment := range *segmentList {
		linkSegmentSorted := dataDir.LinksDir + "/sort_" + strconv.Itoa(segment.SegmentID) + extensionTxtGz
		if fileutils.FileExists(linkSegmentSorted) {
			fmt.Println("!!!Segment " + segment.Segment + " already imported!!!")
			now := time.Now()
			(*segmentList)[i].ImportEnded = &now
		}
	}
}

// IsCorrectArchiveFormat checks if the archive name is in the correct format
func IsCorrectArchiveFormat(s string) bool {
	pattern := `^CC-MAIN-\d{4}-\d{2}$`
	match, _ := regexp.MatchString(pattern, s)
	return match
}
