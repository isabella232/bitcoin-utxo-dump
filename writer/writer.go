package writer

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Writer interface {
	// Write writes the given map values.
	Write(map[string]string) error
	// Close shuts down the writer. Call before exit.
	Close()
}

// validateAndAppendFileName validates the extension of the file.
// if there is no extension provided, it adds it.
func validateAndAppendFileName(file, extension string) (string, error) {
	parts := strings.Split(file, ".")
	if len(parts) == 1 {
		// No extension given, auto append.
		return fmt.Sprintf("%v.%v", parts[0], extension), nil
	}

	if parts[1] != extension {
		return "", fmt.Errorf("incorrect file extension given. expected [%v] but got [%v]", extension, parts[1])
	}

	return file, nil
}

// mustCreateFileAndWriter creates a file and a writer to it or panics if it fails.
func mustCreateFileAndWriter(path, extension string) (*bufio.Writer, *os.File) {
	filePath, err := validateAndAppendFileName(path, extension)
	if err != nil {
		panic(err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	writer := bufio.NewWriter(file)

	return writer, file
}
