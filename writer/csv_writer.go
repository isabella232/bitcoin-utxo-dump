package writer

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Headers []string

func (h *Headers) getHeaderLine() string {
	return strings.Join(*h, ",")
}

type CSVWriter struct {
	file    *os.File
	writer  *bufio.Writer
	headers Headers // To preserve field order
}

func MustNewCsvWriter(path string, fields string) *CSVWriter {
	writer, file := mustCreateFileAndWriter(path, "csv")

	headers := Headers{}
	for _, field := range strings.Split(fields, ",") {
		headers = append(headers, field)
	}

	fmt.Fprintln(writer, headers.getHeaderLine())
	return &CSVWriter{
		file:    file,
		writer:  writer,
		headers: headers,
	}
}

func (cw *CSVWriter) Write(utxo map[string]string) error {
	csvline := "" // Build output line from given fields
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
