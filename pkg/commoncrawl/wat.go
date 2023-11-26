package commoncrawl

import (
	"bufio"
	"fmt"
	"github.com/dgryski/go-farm"
	jsoniter "github.com/json-iterator/go"
	"github.com/kris-dev-hub/globallinks/pkg/config"
	"github.com/tidwall/gjson"
	"golang.org/x/net/publicsuffix"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/klauspost/compress/gzip" // faster than std gzip library, 0.7 sec faster parsing 1M lines
)

type UrlRecord struct {
	Url       *string
	Scheme    *string
	Host      *string
	Path      *string
	RawQuery  *string
	Fragment  *string
	Domain    *string
	SubDomain *string
	Text      *string //optional text from link
	NoFollow  *int
}

type WatPage struct {
	IP            *string
	Imported      *string
	Title         *string
	NoIndex       *int
	NoFollow      *int
	InternalLinks int
	ExternalLinks int
	UrlRecord     *UrlRecord
	Links         []UrlRecord
}

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

type SortFileLinkByFields struct {
	Key       string
	Domain    string
	Subdomain string
	Path      string
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

// ParseWatByLine - parse wat file line by line and store links in file
func ParseWatByLine(filePath string, dirOut string) error {

	//prepare ignore domains and extensions map - load only when empty
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

	//clear domain cache
	domainCacheMutex.Lock()
	domainCache = map[string]string{}
	domainCacheMutex.Unlock()

	var pageMap = make(map[string]FilePage)
	var linkMap = make(map[string]FileLink)

	const maxCapacityScanner = 2 * 1024 * 1024 //1MB

	fileID, err := ExtractWatFileNumber(filePath)
	if err != nil {
		return fmt.Errorf("error extracting file number: %w", err)
	}

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
	//create buffer to avoid going over token size
	buf := make([]byte, maxCapacityScanner)
	scanner.Buffer(buf, maxCapacityScanner)

	// Read each line and append to the records slice
	line := ""

	var urlRecord = UrlRecord{}

	validPage := false

	for scanner.Scan() {
		line = scanner.Text()
		if strings.HasPrefix(line, "WARC-Target-URI: http") {

			urlRecord = UrlRecord{}
			sourceUrl := strings.TrimSpace(line[17:])
			validRecord := buildUrlRecord(sourceUrl, &urlRecord)
			if validRecord == false {
				validPage = false
				continue
			}
			if verifyRecordQuality(&urlRecord) == false {
				validPage = false
				continue
			}

			validPage = true
		}

		//read content of record - only when we have proper record header - validPage = true
		if validPage == true && strings.HasPrefix(line, "{") && strings.Contains(line, "href") {
			validPage = false
			content := readPageContent(line, &urlRecord)
			if content == nil {
				continue
			}

			if len(content.Links) > 0 {
				//save page info to file
				filePage := FilePage{
					Host:          *content.UrlRecord.Host,
					Path:          *content.UrlRecord.Path,
					RawQuery:      *content.UrlRecord.RawQuery,
					Scheme:        *content.UrlRecord.Scheme,
					Title:         strings.ReplaceAll(*content.Title, "|", " "),
					IP:            *content.IP,
					Imported:      *content.Imported,
					InternalLinks: content.InternalLinks,
					ExternalLinks: content.ExternalLinks,
					NoIndex:       *content.NoIndex,
				}
				pageHash := fmt.Sprintf("%x", farm.Hash64([]byte(*content.UrlRecord.Host+*content.UrlRecord.Path+*content.UrlRecord.RawQuery)))
				pageMap[pageHash] = filePage
				for _, link := range content.Links {
					//write to file
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
					linkHash := fmt.Sprintf("%x", farm.Hash64([]byte(*link.Host+*link.Path+*link.RawQuery+*content.UrlRecord.Host+*content.UrlRecord.Path+*content.UrlRecord.RawQuery)))
					linkMap[linkHash] = fileLink
				}
			}
		}
	}

	//saving link file and reseting linkMap
	err = saveLinkFile(fileID, dirOut, linkMap, pageMap)
	if err != nil {
		return err
	}

	//saving page file and reseting pageMap
	err = savePageFile(fileID, dirOut, pageMap)
	if err != nil {
		return err
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning the file: %w", err)
	}

	return nil
}

func readPageContent(line string, sourceUrlRecord *UrlRecord) *WatPage {
	watPage := WatPage{}
	watPage.UrlRecord = sourceUrlRecord

	//parse json and reuse if for Get - 100ms faster per 1M lines
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

	//ignore pages with content problems like chinese characters in headers etc., rel canonical problems, etc.
	if verifyContentQuality(&parsedJSON, &watPage) == false {
		return nil
	}

	watPage.Links, watPage.InternalLinks, watPage.ExternalLinks = parseLinks(linksData, sourceUrlRecord, *watPage.NoFollow)

	return &watPage
}

// GetInfoFromMeta returns noindex and nofollow values from meta tags
func getNoFollowNoIndex(metas string) (int, int) {

	//using int instead of bool to use less space in text file
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
		//if we can not parse json noindex and nofollow are probably not set, so we can ignore it
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
func parseLinks(links string, sourceUrlRecord *UrlRecord, pageNoFollow int) ([]UrlRecord, int, int) {
	internalLinks := 0
	externalLinks := 0

	var urlRecord = UrlRecord{}
	var urlRecords []UrlRecord

	type LinkInfo struct {
		Path string `json:"path"`
		URL  string `json:"url"`
		Text string `json:"text"`
		Rel  string `json:"rel"`
	}

	var linksArray []LinkInfo
	err := jsoniter.Unmarshal([]byte(links), &linksArray)
	if err != nil {
		//probably something is wrong in exported file, ignore for now
	}

	for _, linkData := range linksArray {
		noFollow := pageNoFollow

		//ignore non A tags
		if linkData.Path != "A@/href" {
			continue
		}
		//ignore links without http, https or //
		if strings.HasPrefix(linkData.URL, "http") == false && strings.HasPrefix(linkData.URL, "//") == false {
			internalLinks++
			continue
		}
		if strings.HasPrefix(linkData.Rel, "nofollow") {
			noFollow = 1
		}

		urlRecord = UrlRecord{
			Text:     &linkData.Text,
			NoFollow: &noFollow,
		}
		validRecord := buildUrlRecord(linkData.URL, &urlRecord)
		if validRecord == false {
			continue
		}

		//ignore the same hosts
		if *sourceUrlRecord.Host == *urlRecord.Host {
			internalLinks++
			continue
		}

		//ignore the same domains
		if *sourceUrlRecord.Domain == *urlRecord.Domain {
			externalLinks++
			continue
		}

		if verifyRecordQuality(&urlRecord) == false {
			externalLinks++
			continue
		}

		// link is a file so we ignore it
		if urlRecord.Path == nil || isIgnoredExtension(*urlRecord.Path) {
			continue
		}

		if isIgnoredDomain(*urlRecord.Domain) == true {
			externalLinks++
			continue
		}

		externalLinks++
		urlRecords = append(urlRecords, urlRecord)

	}

	return urlRecords, internalLinks, externalLinks
}

// verifyRecordQuality - verify if record is valid, no blocked TLD, no broken host, no broken query, etc.
func verifyRecordQuality(record *UrlRecord) bool {
	// could not find domain
	if record.Domain == nil {
		return false
	}

	//ignore blocked TLD
	if ignoreTLD(*record.Domain) == true {
		return false
	}
	//validate problems with host
	if validateHost(*record.Host) == false {
		return false
	}

	//validate query length. Over 200 is probably garbage
	if record.RawQuery != nil && len(*record.RawQuery) > 200 {
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
		strings.Contains(host, ";") {
		return false
	}

	//ignore IP addresses - there is no external validation function to make it faster
	if ipRegex.MatchString(host) == true {
		return false
	}

	//ignore hosts without "." they are broken
	if !strings.Contains(host, ".") {
		return false
	}
	return true
}

// buildUrlRecord - build url record from source url, check domain, path, query, etc.
func buildUrlRecord(sourceUrl string, urlRecord *UrlRecord) bool {

	//ignore url with \n
	if strings.Contains(sourceUrl, "\n") {
		return false
	}

	urlRecord.Url = &sourceUrl

	//ignore sourceUrl that can't be parsed
	parsedURL, err := url.Parse(sourceUrl)
	if err != nil {
		return false
	}

	//ignore path with \n
	if strings.Contains(parsedURL.Path, "\n") {
		return false
	}

	//ignore path with | char
	if strings.Contains(parsedURL.Path, "|") {
		return false
	}

	//add "" to Text when it is empty
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

	//ignore query starting with
	if ignoreQuery(*urlRecord.RawQuery) == true {
		emptyString := ""
		urlRecord.RawQuery = &emptyString
	}

	urlRecord.Fragment = &parsedURL.Fragment

	//ignore records without known domain
	domainCacheMutex.RLock()
	domain, exists := domainCache[*urlRecord.Host]
	domainCacheMutex.RUnlock()
	if exists == false {
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
	_, exists := ignoreDomains[domain]
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

func verifyContentQuality(parsedJSON *gjson.Result, watPage *WatPage) bool {

	/* TODO: I might consider ignoring only noindex nofollow pages
	//ignore no index no follow pages
	if *watPage.NoIndex == 1 || *watPage.NoFollow == 1 {
		return false
	}
	return true
	*/

	//ignore no index pages
	if *watPage.NoIndex == 1 {
		return false
	}

	//ignore pages with canonical link pointing to other page
	if checkPageCanonicalLink(parsedJSON, watPage) == false {
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
			//probably something is wrong in exported file, ignore for now
		}

		for _, link := range links {
			if link.Rel == "canonical" && link.URL != "" {
				//parse canonical url
				parsedURL, err := url.Parse(link.URL)
				if err != nil {
					//ignore the page if it has broken canonical link
					//TODO: I might rethink this. Need to check this on more real data
					return false
				}

				//ignore pages with canonical pointing to other host and then analyze only path
				if strings.HasPrefix(link.URL, "http") || strings.HasPrefix(link.URL, "//") {
					//ignore pages with canonical pointing to other host
					if parsedURL.Host != *watPage.UrlRecord.Host {
						return false
					}

					//change URL to path since it is the same host
					link.URL = parsedURL.Path
				}

				//standardize / path
				if link.URL == "" {
					link.URL = "/"
				}

				//ignore pages with canonical pointing to other path
				if link.URL != *watPage.UrlRecord.Path {
					//TODO: we could eventually change source page path to canonical path. Need to check this on more real data
					return false
				}

				//ignore pages with canonical pointing to other query or no query
				if watPage.UrlRecord.RawQuery != nil && *watPage.UrlRecord.RawQuery != "" {
					//TODO: we could eventually change source page query to empty query if we have such on canonical query. Need to check this on more real data
					return false
				}
			}
		}
	}

	return true
}

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
func savePageFile(fileID string, dirOut string, pageMap map[string]FilePage) error {
	fileOutPage, err := os.OpenFile(dirOut+"/page/"+fileID+".txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Error opening page file: %s\n", err)
		return err
	}
	defer fileOutPage.Close()
	writerPage := bufio.NewWriter(fileOutPage)

	for _, content := range pageMap {
		_, err = writerPage.WriteString(fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n",
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
		))
		if err != nil {
			return err
		}
	}

	err = writerPage.Flush()
	if err != nil {
		return err
	}

	//reset pageMap
	pageMap = make(map[string]FilePage)

	return nil
}

// saveLinkFile - save links info to file
func saveLinkFile(fileID string, dirOut string, linkMap map[string]FileLink, pageMap map[string]FilePage) error {

	sortableFileLinkSlice := sortFileLink(linkMap)

	// Open the file for writing, create it if not exists, append to it if it does.
	fileOut, err := os.OpenFile(dirOut+"/"+fileID+".txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", err)
		return err
	}
	defer fileOut.Close()
	writer := bufio.NewWriter(fileOut)

	for _, item := range sortableFileLinkSlice {
		content := linkMap[item.Key]

		page := pageMap[content.PageHash]

		_, err = writer.WriteString(fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%d|%d|%s|%s\n",
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
		))
		if err != nil {
			return err
		}

	}

	// Flush the buffer to disk
	err = writer.Flush()
	if err != nil {
		return err
	}

	//reset linkMap
	linkMap = make(map[string]FileLink)

	return nil
}

// sortFileLink - sort link map by domain, subdomain and path
func sortFileLink(linkMap map[string]FileLink) []SortFileLinkByFields {

	var sortableSlice []SortFileLinkByFields
	for key, value := range linkMap {
		sortableSlice = append(sortableSlice, SortFileLinkByFields{Key: key, Domain: value.LinkDomain, Subdomain: value.LinkHost, Path: value.LinkPath})
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
func genSubdomain(urlRecord *UrlRecord) string {
	var subDomain string
	if *urlRecord.Host == *urlRecord.Domain {
		subDomain = ""
	} else {
		subDomain = strings.TrimSuffix(*urlRecord.Host, "."+*urlRecord.Domain)
	}
	return subDomain
}
