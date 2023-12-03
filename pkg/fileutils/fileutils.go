package fileutils

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

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
