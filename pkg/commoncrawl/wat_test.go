package commoncrawl

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/kris-dev-hub/globallinks/pkg/config"
	"github.com/tidwall/gjson"
)

func TestValidateHost(t *testing.T) {
	testCases := []struct {
		host     string
		expected bool
	}{
		{"example.com", true},
		{"localhost", false},
		{"192.168.0.1", false},  // IP address should return false
		{"example.com%", false}, // Invalid character
		// Add more test cases as needed
	}

	for _, tc := range testCases {
		t.Run(tc.host, func(t *testing.T) {
			result := validateHost(tc.host)
			if result != tc.expected {
				t.Errorf("validateHost(%s) = %v; want %v", tc.host, result, tc.expected)
			}
		})
	}
}

func TestUpdateSegmentImportEnd(t *testing.T) {
	// Prepare a list of segments
	segmentList := &[]WatSegment{
		{Segment: "Segment1", SegmentID: 1},
		{Segment: "Segment2", SegmentID: 2},
	}

	// Segment name to update
	segmentNameToUpdate := "Segment1"

	// Call the function
	err := UpdateSegmentImportEnd(segmentList, segmentNameToUpdate)
	if err != nil {
		t.Errorf("UpdateSegmentImportEnd returned an error: %v", err)
	}

	// Check if the ImportEnded is set for the specified segment
	updated := false
	for _, segment := range *segmentList {
		if segment.Segment == segmentNameToUpdate {
			if segment.ImportEnded == nil {
				t.Errorf("ImportEnded was not set for segment %s", segmentNameToUpdate)
			} else {
				// Check if the time is set within a reasonable window (e.g., last 5 seconds)
				if time.Since(*segment.ImportEnded) > 5*time.Second {
					t.Errorf("ImportEnded time for segment %s is not recent", segmentNameToUpdate)
				}
				updated = true
			}
		}
	}

	if !updated {
		t.Errorf("Segment %s not found in the list", segmentNameToUpdate)
	}
}

func TestUpdateSegmentImportStart(t *testing.T) {
	// Prepare test data
	now := time.Now()
	segmentList := &[]WatSegment{
		{Segment: "Segment1", SegmentID: 1, ImportStarted: &now},
		{Segment: "Segment2", SegmentID: 2},
	}

	// Test updating a segment that has not started yet
	segmentNameToUpdate := "Segment2"
	err := UpdateSegmentImportStart(segmentList, segmentNameToUpdate)
	if err != nil {
		t.Errorf("UpdateSegmentImportStart returned an error for %s: %v", segmentNameToUpdate, err)
	}

	// Verify that the ImportStarted time was set
	for _, segment := range *segmentList {
		if segment.Segment == segmentNameToUpdate {
			if segment.ImportStarted == nil {
				t.Errorf("ImportStarted was not set for %s", segmentNameToUpdate)
			}
		}
	}

	// Test updating a segment that is not in the list
	nonExistentSegment := "NonExistentSegment"
	err = UpdateSegmentImportStart(segmentList, nonExistentSegment)
	if err == nil || err.Error() != "segment not found" {
		t.Errorf("Expected 'segment not found' error for %s, got %v", nonExistentSegment, err)
	}
}

func TestUpdateSegmentLinkImportStatus(t *testing.T) {
	// Setting up test data with a couple of segments and wat files
	segmentList := &[]WatSegment{
		{
			Segment: "Segment1",
			WatFiles: []WatFile{
				{Number: "00010", Path: "somepath1.warc.wat.gz"},
				{Number: "00011", Path: "somepath2.warc.wat.gz"},
			},
		},
		{
			Segment: "Segment2",
			WatFiles: []WatFile{
				{Number: "00020", Path: "somepath3.warc.wat.gz"},
				{Number: "00021", Path: "somepath4.warc.wat.gz"},
			},
		},
	}

	// Define the segment name and file path for the test
	segmentName := "Segment1"
	filePath := "crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/wat/CC-MAIN-20210115134101-20210115164101-00010.warc.wat.gz"

	// Call the function to update the status
	err := UpdateSegmentLinkImportStatus(segmentList, segmentName, filePath)
	if err != nil {
		t.Fatalf("UpdateSegmentLinkImportStatus returned an error: %v", err)
	}

	// Verify that the Imported time was set correctly
	found := false
	for _, segment := range *segmentList {
		if segment.Segment == segmentName {
			for _, file := range segment.WatFiles {
				if file.Number == "00010" {
					if file.Imported == nil {
						t.Errorf("Imported was not set for file %s in segment %s", file.Number, segmentName)
					}
					found = true
				}
			}
		}
	}

	if !found {
		t.Errorf("File with number 00010 not found in segment %s", segmentName)
	}

	// Test with a non-existent segment or file
	err = UpdateSegmentLinkImportStatus(segmentList, "NonExistentSegment", filePath)
	if err == nil || err.Error() != "segment or link not found" {
		t.Errorf("Expected 'segment or link not found' error, got %v", err)
	}
}

func TestSelectSegmentToImport(t *testing.T) {
	now := time.Now()

	// Test data setup
	segmentList := []WatSegment{
		{Segment: "Segment1", SegmentID: 1, ImportEnded: &now}, // already imported
		{Segment: "Segment2", SegmentID: 2},                    // not imported
		{Segment: "Segment3", SegmentID: 3, ImportEnded: &now}, // already imported
	}

	// Test selecting a segment to import
	expectedSegment := "Segment2"
	selectedSegment, err := SelectSegmentToImport(segmentList)
	if err != nil {
		t.Fatalf("SelectSegmentToImport returned an error: %v", err)
	}
	if selectedSegment.Segment != expectedSegment {
		t.Errorf("Expected segment %s, got %s", expectedSegment, selectedSegment.Segment)
	}

	// Test scenario where all segments are imported
	for i := range segmentList {
		segmentList[i].ImportEnded = &now
	}
	_, err = SelectSegmentToImport(segmentList)
	if err == nil || err.Error() != "no segment to import" {
		t.Errorf("Expected 'no segment to import' error, got %v", err)
	}
}

func TestCountFilesInSegmentToProcess(t *testing.T) {
	now := time.Now()

	// Setting up a WatSegment with a mix of imported and non-imported files
	testSegment := WatSegment{
		WatFiles: []WatFile{
			{Number: "00001", Imported: &now}, // Imported file
			{Number: "00002"},                 // Non-imported file
			{Number: "00003", Imported: &now}, // Imported file
			{Number: "00004"},                 // Non-imported file
		},
	}

	// Expected count of files to process
	expectedCount := 2

	// Call the function
	count := CountFilesInSegmentToProcess(testSegment)

	// Verify the result
	if count != expectedCount {
		t.Errorf("Expected %d files to process, got %d", expectedCount, count)
	}
}

func TestGenSubdomain(t *testing.T) {
	host := "www.test.com"
	domain := "test.com"
	urlRecord := URLRecord{Host: &host, Domain: &domain}

	if genSubdomain(&urlRecord) != "www" {
		t.Errorf("Expected www, got %s", genSubdomain(&urlRecord))
	}
}

func TestSortFileLink(t *testing.T) {
	type test struct {
		name     string
		input    map[string]FileLink
		expected []SortFileLinkByFields
	}

	// Define your test cases
	tests := []test{
		{
			name: "basic sorting",
			input: map[string]FileLink{
				"a": {LinkDomain: "example.org", LinkSubDomain: "www", LinkPath: "/path1"},
				"b": {LinkDomain: "example.org", LinkSubDomain: "app", LinkPath: "/path2"},
				"c": {LinkDomain: "example.com", LinkSubDomain: "www", LinkPath: "/path3"},
			},
			expected: []SortFileLinkByFields{
				{Key: "c", Domain: "example.com", Subdomain: "www", Path: "/path3"},
				{Key: "b", Domain: "example.org", Subdomain: "app", Path: "/path2"},
				{Key: "a", Domain: "example.org", Subdomain: "www", Path: "/path1"},
			},
		},
		// Add more test cases here, including edge cases
	}

	// Run the tests
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sortFileLink(tc.input)
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("Test %s failed. Expected %v, got %v", tc.name, tc.expected, result)
			}
		})
	}
}

func TestExtractWatFileNumber(t *testing.T) {
	tests := []struct {
		filename string
		want     string
		wantErr  bool
	}{
		{"example-123.warc.wat.gz", "123", false},
		{"test-456.warc.wat.gz", "456", false},
		{"invalidfile.txt", "", true},
		{"no-number.warc.wat.gz", "", true},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got, err := ExtractWatFileNumber(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractWatFileNumber() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractWatFileNumber() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetScheme(t *testing.T) {
	tests := []struct {
		scheme string
		want   string
	}{
		{"https", "2"},
		{"http", "1"},
		{"ftp", "0"},
		{"", "0"},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			if got := setScheme(tt.scheme); got != tt.want {
				t.Errorf("setScheme(%q) = %q, want %q", tt.scheme, got, tt.want)
			}
		})
	}
}

func TestCheckPageCanonicalLink(t *testing.T) {
	// Define your test cases
	tests := []struct {
		name     string
		jsonData string
		watPage  WatPage
		want     bool
	}{
		{
			name:     "Valid Canonical Link",
			jsonData: `{"Envelope":{"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Head":{"Link":[{"path":"/","url":"http://example.com/page","rel":"canonical","type":""}]}}}}}}`,
			watPage: WatPage{
				URLRecord: &URLRecord{
					Host: &[]string{"example.com"}[0],
					Path: &[]string{"/page"}[0],
				},
			},
			want: true,
		},
		{
			name:     "Invalid Canonical Link - Different Host",
			jsonData: `{"Envelope":{"Payload-Metadata":{"HTTP-Response-Metadata":{"HTML-Metadata":{"Head":{"Link":[{"path":"/","url":"http://example.org/page","rel":"canonical","type":""}]}}}}}}`,
			watPage: WatPage{
				URLRecord: &URLRecord{
					Host: &[]string{"example.com"}[0],
					Path: &[]string{"/page"}[0],
				},
			},
			want: false,
		},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedJSON := gjson.Parse(tt.jsonData)
			if got := checkPageCanonicalLink(&parsedJSON, &tt.watPage); got != tt.want {
				t.Errorf("checkPageCanonicalLink() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifyContentQuality(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		watPage  WatPage
		want     bool
	}{
		{
			name:     "Valid Content - NoIndex 0",
			jsonData: `{"some": "data"}`, // Example JSON data
			watPage: WatPage{
				NoIndex: &[]int{0}[0],
			},
			want: true,
		},
		{
			name:     "Invalid Content - NoIndex 1",
			jsonData: `{"some": "data"}`, // Example JSON data
			watPage: WatPage{
				NoIndex: &[]int{1}[0],
			},
			want: false,
		},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedJSON := gjson.Parse(tt.jsonData)
			if got := verifyContentQuality(&parsedJSON, &tt.watPage); got != tt.want {
				t.Errorf("verifyContentQuality() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIgnoreQuery(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"lang=en", true},
		{"utm_source=google", true},
		{"ref=123", true},
		{"page=1", false},
		{"category=books", false},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			if got := ignoreQuery(tt.query); got != tt.want {
				t.Errorf("ignoreQuery(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestIgnoreTLD(t *testing.T) {
	tests := []struct {
		domain string
		want   bool
	}{
		{"example.cn", true},
		{"website.com", false},
		{"info.co.uk", false},
		{"site.cn", true},
		{"domain.com.cn", true},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.domain, func(t *testing.T) {
			if got := ignoreTLD(tt.domain); got != tt.want {
				t.Errorf("ignoreTLD(%q) = %v, want %v", tt.domain, got, tt.want)
			}
		})
	}
}

func TestIsIgnoredExtension(t *testing.T) {
	fileExtensions = createFileExtensionMap(config.FileExtensions)
	tests := []struct {
		path string
		want bool
	}{
		{"/image.jpg", true},
		{"/document.pdf", true},
		{"/script.php", false},
		{"/photo.jpeg", true},
		{"/test", false},
		{"/picture.JPG", true}, // Test case-insensitivity
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := isIgnoredExtension(tt.path); got != tt.want {
				t.Errorf("isIgnoredExtension(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestIsIgnoredDomain(t *testing.T) {
	ignoreDomains = createDomainMap(config.IgnoreDomains)
	tests := []struct {
		domain string
		want   bool
	}{
		{"clickbank.net", true},
		{"test.com", false},
		{"example.org", false},
		{"CLICKBANK.NET", true}, // Test case-insensitivity if applicable
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.domain, func(t *testing.T) {
			if got := isIgnoredDomain(tt.domain); got != tt.want {
				t.Errorf("isIgnoredDomain(%q) = %v, want %v", tt.domain, got, tt.want)
			}
		})
	}
}

func TestBuildURLRecord(t *testing.T) {
	tests := []struct {
		name       string
		sourceURL  string
		want       bool
		wantRecord URLRecord
	}{
		{
			name:      "Valid URL",
			sourceURL: "http://example.com/path?query=1#fragment",
			want:      true,
			wantRecord: URLRecord{
				URL:       &[]string{"http://example.com/path?query=1#fragment"}[0],
				Scheme:    &[]string{"1"}[0], // Assuming setScheme returns "1" for "http"
				Host:      &[]string{"example.com"}[0],
				Path:      &[]string{"/path"}[0],
				RawQuery:  &[]string{"query=1"}[0],
				Fragment:  &[]string{"fragment"}[0],
				Domain:    &[]string{"example.com"}[0],
				SubDomain: &[]string{""}[0], // Assuming genSubdomain returns empty for this case
				Text:      &[]string{""}[0],
			},
		},
		{
			name:      "Invalid URL - Contains New Line",
			sourceURL: "http://example.com/path\n?query=1#fragment",
			want:      false,
		},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			urlRecord := &URLRecord{}
			if got := buildURLRecord(tt.sourceURL, urlRecord); got != tt.want {
				t.Errorf("buildURLRecord() = %v, want %v", got, tt.want)
			}
			if tt.want && !reflect.DeepEqual(urlRecord, &tt.wantRecord) {
				t.Errorf("buildURLRecord() got record = %v, want %v", urlRecord, tt.wantRecord)
			}
		})
	}
}

func TestVerifyRecordQuality(t *testing.T) {
	tests := []struct {
		name   string
		record URLRecord
		want   bool
	}{
		{
			name: "Valid Record",
			record: URLRecord{
				Domain:   &[]string{"example.com"}[0],
				Host:     &[]string{"www.example.com"}[0],
				RawQuery: &[]string{"query=value"}[0],
			},
			want: true,
		},
		{
			name: "Cn Domain",
			record: URLRecord{
				Host:     &[]string{"www.example.cn"}[0],
				RawQuery: &[]string{"query=value"}[0],
			},
			want: false,
		},
		{
			name: "Long query",
			record: URLRecord{
				Host:     &[]string{"www.example.cn"}[0],
				RawQuery: &[]string{"query=value&a=sdddddddddddddddddddffffffffffffffffffffffffffffffffeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeewwwwwwwwwwwwwwwwwwwwwwwwwwwwwwweeeeeeeeeeeeeeeeeeeewwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwweeeeeeeeeeeeeee"}[0],
			},
			want: false,
		},
		{
			name: "LBroken host",
			record: URLRecord{
				Host:     &[]string{"www.examp[le.com"}[0],
				RawQuery: &[]string{"query=value"}[0],
			},
			want: false,
		},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := verifyRecordQuality(&tt.record); got != tt.want {
				t.Errorf("verifyRecordQuality() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestCreateDataDirectory tests the creation of a new directory.
func TestCreateDataDirectory(t *testing.T) {
	// Create a temporary directory to simulate the environment.
	tempDir, err := os.MkdirTemp("", "testCreateDataDir")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up

	// Define a new subdirectory inside the temporary directory.
	newDir := filepath.Join(tempDir, "newdir")

	// Test creating the new directory.
	dataDir, err := CreateDataDir(newDir)
	if err != nil {
		t.Errorf("CreateDataDirectory() error = %v", err)
	}

	// Check if the directory was created.
	if _, err := os.Stat(dataDir.TmpDir); os.IsNotExist(err) {
		t.Errorf("CreateDataDirectory() did not create the directory")
	}
}

func TestIsCorrectArchiveFormat(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"CC-MAIN-2023-01", true},
		{"CC-MAIN-2023-12", true},
		{"CC-MAIN-2023", false},
		{"CC-MAIN-23-01", false},
		{"CC-MAIN-202301", false},
		{"XX-MAIN-2023-01", false},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := IsCorrectArchiveFormat(tt.input); got != tt.want {
				t.Errorf("IsCorrectArchiveFormat(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetNoFollowNoIndex(t *testing.T) {
	testCases := []struct {
		name             string
		metas            string
		expectedNoIndex  int
		expectedNoFollow int
	}{
		{
			name:             "NoIndex and NoFollow",
			metas:            `[{"name":"robots","content":"noindex, nofollow"}]`,
			expectedNoIndex:  1,
			expectedNoFollow: 1,
		},
		{
			name:             "NoIndex only",
			metas:            `[{"name":"robots","content":"noindex"}]`,
			expectedNoIndex:  1,
			expectedNoFollow: 0,
		},
		{
			name:             "NoFollow only",
			metas:            `[{"name":"robots","content":"nofollow"}]`,
			expectedNoIndex:  0,
			expectedNoFollow: 1,
		},
		{
			name:             "Neither NoIndex nor NoFollow",
			metas:            `[{"name":"robots","content":"index, follow"}]`,
			expectedNoIndex:  0,
			expectedNoFollow: 0,
		},
		{
			name:             "No robots meta tag",
			metas:            `[{"name":"viewport","content":"width=device-width, initial-scale=1"}]`,
			expectedNoIndex:  0,
			expectedNoFollow: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			noIndex, noFollow := getNoFollowNoIndex(tc.metas)
			if noIndex != tc.expectedNoIndex {
				t.Errorf("Expected NoIndex to be %d, got %d", tc.expectedNoIndex, noIndex)
			}
			if noFollow != tc.expectedNoFollow {
				t.Errorf("Expected NoFollow to be %d, got %d", tc.expectedNoFollow, noFollow)
			}
		})
	}
}
