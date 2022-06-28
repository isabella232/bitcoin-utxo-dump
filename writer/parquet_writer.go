package writer

import (
	"bufio"
	"os"

	"github.com/xitongsys/parquet-go/parquet"
	parwriter "github.com/xitongsys/parquet-go/writer"
)

// Keep the fields as pointers so they are considered optional.
type UTXO struct {
	Count    *string `parquet:"name=count, 		type=BYTE_ARRAY, convertedtype=UTF8"`
	TxId     *string `parquet:"name=txid, 		type=BYTE_ARRAY, convertedtype=UTF8"`
	Vout     *string `parquet:"name=vout, 		type=BYTE_ARRAY, convertedtype=UTF8"`
	Height   *string `parquet:"name=height, 	type=BYTE_ARRAY, convertedtype=UTF8"`
	Coinbase *string `parquet:"name=coinbase, 	type=BYTE_ARRAY, convertedtype=UTF8"`
	Amount   *string `parquet:"name=amount, 	type=BYTE_ARRAY, convertedtype=UTF8"`
	NSize    *string `parquet:"name=nsize, 		type=BYTE_ARRAY, convertedtype=UTF8"`
	Script   *string `parquet:"name=script,		type=BYTE_ARRAY, convertedtype=UTF8"`
	Type     *string `parquet:"name=type, 		type=BYTE_ARRAY, convertedtype=UTF8"`
	Address  *string `parquet:"name=address, 	type=BYTE_ARRAY, convertedtype=UTF8"`
}

type ParquetWriter struct {
	file           *os.File
	writer         *bufio.Writer
	parWriter      *parwriter.ParquetWriter
	fieldsSelected map[string]bool
}

func MustNewParquetWriter(path string) *ParquetWriter {
	writer, file := mustCreateFileAndWriter(path, "parquet")
	pw, err := parwriter.NewParquetWriterFromWriter(writer, new(UTXO), 4)
	if err != nil {
		panic(err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	return &ParquetWriter{
		file:      file,
		writer:    writer,
		parWriter: pw,
	}
}

func (pw *ParquetWriter) Write(utxoMap map[string]string) error {
	utxo := &UTXO{
		Count:    getValuePointer(utxoMap, "count"),
		TxId:     getValuePointer(utxoMap, "txid"),
		Vout:     getValuePointer(utxoMap, "vout"),
		Height:   getValuePointer(utxoMap, "height"),
		Coinbase: getValuePointer(utxoMap, "coinbase"),
		Amount:   getValuePointer(utxoMap, "amount"),
		NSize:    getValuePointer(utxoMap, "nsize"),
		Script:   getValuePointer(utxoMap, "script"),
		Type:     getValuePointer(utxoMap, "type"),
		Address:  getValuePointer(utxoMap, "address"),
	}

	return pw.parWriter.Write(utxo)
}

func (cw *ParquetWriter) Close() {
	cw.parWriter.WriteStop()
	cw.writer.Flush()
	cw.file.Close()
}

// getValuePointer returns a pointer to the map's value or nil.
func getValuePointer(keyToValue map[string]string, key string) *string {
	val, ok := keyToValue[key]
	if !ok {
		return nil
	}
	return &val
}
