package config

import (
	"strings"
	"testing"
)

func TestFileExtensions(t *testing.T) {
	if len(FileExtensions) == 0 {
		t.Error("FileExtensions should not be empty")
	}

	// Example of checking for a specific extension
	expectedExtension := ".txt"
	if !contains(FileExtensions, expectedExtension) {
		t.Errorf("FileExtensions should contain %s", expectedExtension)
	}

	// Check format
	for _, ext := range FileExtensions {
		if !strings.HasPrefix(ext, ".") {
			t.Errorf("Invalid format for extension: %s", ext)
		}
	}
}

func TestIgnoreTLD(t *testing.T) {
	if len(IgnoreTLD) == 0 {
		t.Error("IgnoreTLD should not be empty")
	}
}

func TestIgnoreDomains(t *testing.T) {
	if len(IgnoreDomains) == 0 {
		t.Error("IgnoreDomains should not be empty")
	}
}

func TestIgnoreQuery(t *testing.T) {
	if len(IgnoreQuery) == 0 {
		t.Error("IgnoreQuery should not be empty")
	}
}

// Helper function to check if a slice contains a specific string
func contains(slice []string, str string) bool {
	for _, v := range slice {
		if v == str {
			return true
		}
	}
	return false
}
