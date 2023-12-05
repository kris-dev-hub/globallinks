/*
Package fileutils provides utility functions for working with files and directories
*/
package fileutils

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/gzip"
)

// FileExists checks if a file exists
func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// DirExists checks if a directory exists
func DirExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return info.IsDir()
}

// DownloadFile downloads a file from a URL and saves it to the specified path, retry if needed
func DownloadFile(url, outputPath string, maxRetries int) error {
	var resp *http.Response
	var err error
	retryDelay := 3 * time.Second

	for i := 0; i <= maxRetries; i++ {
		resp, err = http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			if resp.StatusCode == http.StatusServiceUnavailable {
				fmt.Println("503 Service Unavailable error received. Retrying...")
				time.Sleep(retryDelay)
				retryDelay *= 2 // Exponential back-off
			}
			err = resp.Body.Close()
			if err != nil {
				fmt.Printf("Error closing response body: %v\n", err)
			}
		} else {
			fmt.Printf("Error during HTTP GET: %v\n", err)
			time.Sleep(retryDelay)
		}
	}

	if err != nil || resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download url %s after retries: %v", url, err)
	}
	defer resp.Body.Close()

	// Create the file where the downloaded data will be stored
	out, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Use io.Copy to write the response body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

// ReadGZFileByLine reads a .gz file line by line and returns a slice of strings
func ReadGZFileByLine(filePath string) ([]string, error) {
	// Open the .gz file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Create a gzip Reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("error creating gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Use a bufio.Scanner to read the file line by line
	scanner := bufio.NewScanner(gzReader)
	var records []string

	// Read each line and append to the records slice
	for scanner.Scan() {
		records = append(records, scanner.Text())
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning the file: %w", err)
	}

	return records, nil
}

// CreateDataDirectory creates the data directory if it does not exist
func CreateDataDirectory(dirOut string) error {
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

// DeleteDirectoryIfEmpty deletes the directory if it is empty - used for cleaning up data directories
func DeleteDirectoryIfEmpty(dirPath string) error {
	// Check if the directory is empty
	remainingFiles, err := filepath.Glob(filepath.Join(dirPath, "*"))
	if err != nil {
		return err
	}
	if len(remainingFiles) == 0 {
		// Directory is empty, delete it
		fmt.Println("Deleting directory: ", dirPath)
		err := os.Remove(dirPath)
		if err != nil {
			return fmt.Errorf("error deleting directory %s: %s", dirPath, err)
		}
	}

	return nil
}
