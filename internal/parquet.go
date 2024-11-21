package internal

import (
	"fmt"
	"log"
	"os"

	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// FileWriter implementa a interface source.ParquetFile para escrita
type FileWriter struct {
	file *os.File
}

// NewFileWriter cria um novo FileWriter a partir de um arquivo local
func NewFileWriter(filePath string) (*FileWriter, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return &FileWriter{file: f}, nil
}

// Write implementa o método Write da interface source.ParquetFile
func (fw *FileWriter) Write(p []byte) (int, error) {
	return fw.file.Write(p)
}

// Read implementa o método Read da interface source.ParquetFile
func (fw *FileWriter) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("read not supported on FileWriter")
}

// Seek implementa o método Seek da interface source.ParquetFile
func (fw *FileWriter) Seek(offset int64, whence int) (int64, error) {
	return fw.file.Seek(offset, whence)
}

// Close implementa o método Close da interface source.ParquetFile
func (fw *FileWriter) Close() error {
	return fw.file.Close()
}

// Create implementa o método Create da interface source.ParquetFile
func (fw *FileWriter) Create(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("create not supported on FileWriter")
}

// Open não é necessário para escrita, mas precisa existir
func (fw *FileWriter) Open(name string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("open not supported on FileWriter")
}

// LocalFileWriter marca que este é um escritor de arquivo local
func (fw *FileWriter) LocalFileWriter() {}

// WriteParquet escreve os dados em um arquivo Parquet
func WriteParquet(data [][]string, columns []string, parquetFilePath string) {
	// Cria o esquema dinâmico com base nas colunas
	type Record map[string]string

	// Cria um FileWriter para escrita em Parquet
	fw, err := NewFileWriter(parquetFilePath)
	if err != nil {
		log.Fatalf("Failed to create Parquet file: %v", err)
	}
	defer fw.Close()

	// Configura o escritor Parquet com o esquema dinâmico
	pw, err := writer.NewParquetWriter(fw, new(Record), 4)
	if err != nil {
		log.Fatalf("Failed to initialize Parquet writer: %v", err)
	}
	defer pw.WriteStop()

	// Escreve os dados no Parquet
	for _, record := range data {
		row := make(Record)
		for i, col := range columns {
			row[col] = record[i]
		}

		if err := pw.Write(row); err != nil {
			log.Fatalf("Failed to write record to Parquet: %v", err)
		}
	}

	fmt.Printf("Parquet file written successfully: %s\n", parquetFilePath)
}
