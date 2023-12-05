package main

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/klauspost/compress/gzip"

	"github.com/kris-dev-hub/globallinks/pkg/fileutils"
)

func main() {
	var err error

	if len(os.Args) < 3 {
		fmt.Println("Require target directory and source file : ./storelinks data/links/compact_01.tar.gz data/linkdb")
		os.Exit(1)
	}

	linkSegmentCompacted := os.Args[1]
	outDir := os.Args[2]

	if !fileutils.FileExists(linkSegmentCompacted) {
		fmt.Println("Source file does not exist")
		os.Exit(1)
	}

	err = fileutils.CreateDataDirectory(outDir)
	if err != nil {
		fmt.Printf("destination dir does not exist or can't be created: %s\n", err)
		os.Exit(1)
	}

	err = splitProcessedData(linkSegmentCompacted, outDir)
	if err != nil {
		log.Fatalf("Could not split files: %v", err)
	}

	// TODO: remove compacted file after we finish all tests
	//	os.Remove(linkSegmentCompacted)
}

// split data into many files sorted by domain names
func splitProcessedData(sortFile string, dirOut string) error {
	// Open the gzipped file
	file, err := os.Open(sortFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	var currentFile *os.File
	var gzipWriter *gzip.Writer
	var currentPrefix string

	scanner := bufio.NewScanner(gzReader)
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
