package writer

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type CSVWriter struct {
	file    *os.File
	writer  *bufio.Writer
	headers []string // To preserve field order
}

func MustNewCsvWriter(path string, fields string) *CSVWriter {
	writer, file := mustCreateFileAndWriter(path, "csv")

	headers := []string{}
	for _, field := range strings.Split(fields, ",") {
		headers = append(headers, field)
	}

	csvheader := strings.Join(headers, ",")
	fmt.Fprintln(writer, csvheader)

	return &CSVWriter{
		file:    file,
		writer:  writer,
		headers: headers,
	}
}

func (cw *CSVWriter) Write(utxo map[string]string) error {
	csvline := "" // Build output line from given fields
	// [ ] string builder faster?
	for _, header := range cw.headers {
		csvline += utxo[header]
		csvline += ","
	}
	csvline = csvline[:len(csvline)-1] // remove trailing ,

	_, err := fmt.Fprintln(cw.writer, csvline)
	return err
}

func (cw *CSVWriter) Close() {
	cw.writer.Flush()
	cw.file.Close()
}
