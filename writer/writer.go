package writer

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	parwriter "github.com/xitongsys/parquet-go/writer"
)

type UTXO struct {
	Count    *string `parquet:"name=count, type=BYTE_ARRAY"`    // "count":false,
	TxId     *string `parquet:"name=txid, type=BYTE_ARRAY"`     // "txid":false,
	Vout     *string `parquet:"name=vout, type=BYTE_ARRAY"`     // "vout":false,
	Height   *string `parquet:"name=height, type=BYTE_ARRAY"`   // "height":false,
	Coinbase *string `parquet:"name=coinbase, type=BYTE_ARRAY"` //"coinbase":false,
	Amount   *string `parquet:"name=amount, type=BYTE_ARRAY"`   // "amount":false,
	NSize    *string `parquet:"name=nsize, type=BYTE_ARRAY"`    //"nsize":false,
	Script   *string `parquet:"name=script, type=BYTE_ARRAY"`   //"script":false,
	Type     *string `parquet:"name=type, type=BYTE_ARRAY"`     // "type":false,
	Address  *string `parquet:"name=address, type=BYTE_ARRAY"`  //"address":false,
}

type Writer interface {
	Write(utxo map[string]string) string
	Close()
}

type CSVWriter struct {
	file           *os.File
	writer         *bufio.Writer
	fieldsSelected map[string]bool
	headers        []string // To preserve field order
}

func NewCsvWriter(f string, fieldsSelected map[string]bool) *CSVWriter {
	file, err := os.Create(f) // os.OpenFile("filename.txt", os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(file)
	headers := []string{}
	for field, selected := range fieldsSelected {
		if !selected {
			continue
		}
		headers = append(headers, field)
	}

	csvheader := strings.Join(headers, ",")
	fmt.Fprintln(writer, csvheader)

	return &CSVWriter{
		file:           file,
		writer:         writer,
		fieldsSelected: fieldsSelected,
		headers:        headers,
	}
}

func (cw *CSVWriter) Write(utxo map[string]string) string {
	csvline := "" // Build output line from given fields
	// [ ] string builder faster?
	for _, header := range cw.headers {
		csvline += utxo[header]
		csvline += ","
	}
	csvline = csvline[:len(csvline)-1] // remove trailing ,

	fmt.Fprintln(cw.writer, csvline)
	return csvline
}

func (cw *CSVWriter) Close() {
	cw.writer.Flush()
	cw.file.Close()
}

type ParquetWriter struct {
	file           *os.File
	writer         *bufio.Writer
	parWriter      *parwriter.ParquetWriter
	fieldsSelected map[string]bool
}

func NewParquetWriter(f string) *ParquetWriter {
	file, err := os.Create(f) // os.OpenFile("filename.txt", os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(file)
	pw, err := parwriter.NewParquetWriterFromWriter(writer, new(UTXO), 4)
	if err != nil {
		panic(err)
	}

	return &ParquetWriter{
		file:      file,
		writer:    writer,
		parWriter: pw,
	}
}

func (pw *ParquetWriter) Write(utxoMap map[string]string) string {
	getAddr := func(key string) *string {
		val, ok := utxoMap[key]
		if !ok {
			return nil
		}
		return &val
	}

	utxo := &UTXO{
		Count:    getAddr("count"),
		TxId:     getAddr("txid"),
		Vout:     getAddr("vout"),
		Height:   getAddr("height"),
		Coinbase: getAddr("coinbase"),
		Amount:   getAddr("amount"),
		NSize:    getAddr("nsize"),
		Script:   getAddr("script"),
		Type:     getAddr("type"),
		Address:  getAddr("address"),
	}

	if err := pw.parWriter.Write(utxo); err != nil {
		fmt.Printf("Failed to write utxo [%v] to parquet file", utxo)
	}
	return ""
}

func (cw *ParquetWriter) Close() {
	cw.parWriter.WriteStop()
	cw.writer.Flush()
	cw.file.Close()
}
