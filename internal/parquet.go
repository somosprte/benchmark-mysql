package internal

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"unicode"

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

// / WriteParquet escreve os dados em um arquivo Parquet usando um esquema dinâmico
func WriteParquet(data [][]string, columns []string, parquetFilePath string) {
	// Cria a estrutura dinâmica
	dynamicStruct := createDynamicStruct(columns)

	// Cria um FileWriter para escrita em Parquet
	fw, err := NewFileWriter(parquetFilePath)
	if err != nil {
		log.Fatalf("Failed to create Parquet file: %v", err)
	}
	defer fw.Close()

	// Configura o escritor Parquet
	pw, err := writer.NewParquetWriter(fw, dynamicStruct, 4)
	if err != nil {
		log.Fatalf("Failed to initialize Parquet writer: %v", err)
	}
	defer pw.WriteStop()

	// Escreve os dados no Parquet
	for _, record := range data {
		// Preenche a estrutura dinâmica
		row := reflect.ValueOf(dynamicStruct).Elem()
		for i, col := range columns {
			if i < len(record) {
				field := row.FieldByName(col)
				if field.IsValid() {
					field.SetString(record[i])
				}
			}
		}

		// Escreve o registro no Parquet
		if err := pw.Write(row.Interface()); err != nil {
			log.Fatalf("Failed to write record to Parquet: %v", err)
		}
	}

	fmt.Printf("Parquet file written successfully: %s\n", parquetFilePath)
}

// createDynamicStruct cria dinamicamente uma estrutura Go para o esquema Parquet
func createDynamicStruct(columns []string) interface{} {
	// Converte os nomes das colunas para campos exportados
	fields := []reflect.StructField{}
	for _, col := range columns {
		// Transforma o nome da coluna em um nome de campo exportado
		fieldName := toExportedFieldName(col)

		fields = append(fields, reflect.StructField{
			Name: fieldName,          // Nome do campo (Go exige que comece com letra maiúscula)
			Type: reflect.TypeOf(""), // Tipo de dado (UTF8 = string)
			Tag:  reflect.StructTag(fmt.Sprintf(`parquet:"name=%s, type=UTF8"`, col)),
		})
	}

	// Cria a estrutura dinâmica
	structType := reflect.StructOf(fields)
	return reflect.New(structType).Interface()
}

// toExportedFieldName converte nomes de colunas para nomes de campos exportados
func toExportedFieldName(name string) string {
	// Remove espaços e caracteres especiais
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "_")

	// Garante que o primeiro caractere seja maiúsculo
	runes := []rune(name)
	if len(runes) > 0 && unicode.IsLower(runes[0]) {
		runes[0] = unicode.ToUpper(runes[0])
	}

	return string(runes)
}
