package main

import (
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/somosprte/benchmark-mysql/internal"
)

func main() {
	// Carregar variáveis de ambiente
	if err := godotenv.Load(); err != nil {
		log.Fatalf("Failed to load .env: %v", err)
	}

	// Configurações
	mysqlDSN := os.Getenv("MYSQL_DSN")
	query := os.Getenv("QUERY")
	parquetFilePath := os.Getenv("PARQUET_FILE_PATH")

	if mysqlDSN == "" || query == "" || parquetFilePath == "" {
		log.Fatalf("Missing required environment variables")
	}

	// 1. Consulta no MySQL e geração do Parquet
	log.Println("Running MySQL benchmark and generating Parquet...")
	internal.BenchmarkMySQL(mysqlDSN, query, parquetFilePath)

	// 2. Consulta no MySQL via DuckDB
	log.Println("Running DuckDB benchmark with MySQL...")
	internal.BenchmarkDuckDBWithMySQL(mysqlDSN, query)

	// 3. Consulta no Parquet via DuckDB
	log.Println("Running DuckDB benchmark with Parquet...")
	internal.BenchmarkDuckDBWithParquet(parquetFilePath, "SELECT * FROM read_parquet('%s') WHERE 1=1")
}
