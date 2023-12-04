package fileutils

import (
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestFileExists(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"FileExists", args{"fileutils.go"}, true},
		{"FileNotExists", args{"fileutils2.go"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FileExists(tt.args.filename); got != tt.want {
				t.Errorf("FileExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDownloadFile(t *testing.T) {
	// Set up a test server
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("test data"))
		if err != nil {
			t.Errorf("Failed to write data. DownloadFile() error = %v", err)
		}
	}))
	defer testServer.Close()

	tempDir, err := os.MkdirTemp("", "testDownload")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	outputPath := filepath.Join(tempDir, "downloadedFile.txt")

	if err := DownloadFile(testServer.URL, outputPath, 3); err != nil {
		t.Errorf("DownloadFile() error = %v", err)
	}

	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Errorf("DownloadFile() did not create file")
	}
}

func TestDownloadFile_HttpError(t *testing.T) {
	// Set up a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // Return a 500 Internal Server Error
	}))
	defer server.Close()

	tempDir, err := os.MkdirTemp("", "testDownload")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	outputPath := filepath.Join(tempDir, "downloadedFile.txt")

	// Call your function with the mock server URL
	err = DownloadFile(server.URL, outputPath, 3)
	if err == nil {
		t.Errorf("DownloadFile() expected to return an error, got nil")
	}
}

func TestDownloadFile_Http503(t *testing.T) {
	// Keep track of the number of attempts
	attempts := 0

	// Set up a mock server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts <= 2 {
			// Return 503 for the first two attempts
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			// Return 200 OK on the third attempt
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer mockServer.Close()

	// Temporary file for download
	tmpfile, err := os.CreateTemp("", "test")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	// Run the DownloadFile function with the mock server URL
	err = DownloadFile(mockServer.URL, tmpfile.Name(), 3)
	if err != nil {
		t.Errorf("DownloadFile() returned an error: %v", err)
	}

	// Check if the function made the correct number of attempts
	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

// TestReadGZFileByLine tests reading lines from a gzipped file.
func TestReadGZFileByLine(t *testing.T) {
	// Create a temporary gzipped file with test data.
	content := []byte("line 1\nline 2\nline 3")
	tempFile, err := os.CreateTemp("", "test*.gz")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	gzWriter := gzip.NewWriter(tempFile)
	_, err = gzWriter.Write(content)
	if err != nil {
		t.Fatalf("Failed to write to gzip writer: %v", err)
	}
	gzWriter.Close()
	tempFile.Close()

	// Read lines from the gzipped file.
	lines, err := ReadGZFileByLine(tempFile.Name())
	if err != nil {
		t.Errorf("ReadGZFileByLine() error = %v", err)
	}

	// Check if the content matches.
	expectedLines := []string{"line 1", "line 2", "line 3"}
	for i, line := range lines {
		if line != expectedLines[i] {
			t.Errorf("ReadGZFileByLine() line %d = %v, want %v", i, line, expectedLines[i])
		}
	}
}

// TestDeleteDirectoryIfEmpty tests the deletion of an empty directory.
func TestDeleteDirectoryIfEmpty(t *testing.T) {
	// Create a temporary directory.
	tempDir, err := os.MkdirTemp("", "testDeleteDir")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	// Defer the deletion of the directory in case the test fails.
	defer os.RemoveAll(tempDir)

	// Test the deletion of the empty directory.
	err = DeleteDirectoryIfEmpty(tempDir)
	if err != nil {
		t.Errorf("DeleteDirectoryIfEmpty() error = %v", err)
	}

	// Check if the directory has been deleted.
	if _, err := os.Stat(tempDir); !os.IsNotExist(err) {
		t.Errorf("DeleteDirectoryIfEmpty() did not delete the directory")
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
	err = CreateDataDirectory(newDir)
	if err != nil {
		t.Errorf("CreateDataDirectory() error = %v", err)
	}

	// Check if the directory was created.
	if _, err := os.Stat(newDir); os.IsNotExist(err) {
		t.Errorf("CreateDataDirectory() did not create the directory")
	}

	// Test creating the same directory again (should not result in an error).
	err = CreateDataDirectory(newDir)
	if err != nil {
		t.Errorf("CreateDataDirectory() error when creating existing directory = %v", err)
	}
}

func TestDeleteDirectoryIfEmpty_PermissionError(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testDeleteDir")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	tempDir2, err := os.MkdirTemp(tempDir, "testDeleteDir2")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir2) // Ensure cleanup
	defer os.RemoveAll(tempDir)  // Ensure cleanup

	// Change permissions to prevent deletion
	if err := os.Chmod(tempDir, 0o555); err != nil { // Read and execute permissions
		t.Fatalf("Failed to change permissions of temporary directory: %v", err)
	}

	// attempt to delete the directory and expect an error
	err = DeleteDirectoryIfEmpty(tempDir2)
	if err == nil {
		t.Errorf("Expected error when deleting directory with restricted permissions, got nil")
	}

	// Restore permissions to allow deletion
	if err := os.Chmod(tempDir, 0o755); err != nil {
		t.Fatalf("Failed to restore permissions of temporary directory: %v", err)
	}

	// Clean up
	err = DeleteDirectoryIfEmpty(tempDir2)
	if err != nil {
		t.Errorf("Failed to delete directory after restoring permissions: %v", err)
	}
	err = DeleteDirectoryIfEmpty(tempDir)
	if err != nil {
		t.Errorf("Failed to delete directory after restoring permissions: %v", err)
	}
}

func TestCreateDirectoryIfEmpty_PermissionError(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testDeleteDir")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Ensure cleanup

	// Change permissions to prevent deletion
	if err := os.Chmod(tempDir, 0o555); err != nil { // Read and execute permissions
		t.Fatalf("Failed to change permissions of temporary directory: %v", err)
	}
	err = CreateDataDirectory(tempDir + "/testDeleteDir2")
	if err == nil {
		t.Errorf("Expected error when deleting directory with restricted permissions, got nil")
	}
	defer os.RemoveAll(tempDir + "/testDeleteDir2") // Ensure cleanup

	// Restore permissions to allow deletion
	if err := os.Chmod(tempDir, 0o755); err != nil {
		t.Fatalf("Failed to restore permissions of temporary directory: %v", err)
	}

	err = DeleteDirectoryIfEmpty(tempDir)
	if err != nil {
		t.Errorf("Failed to delete directory after restoring permissions: %v", err)
	}
}
