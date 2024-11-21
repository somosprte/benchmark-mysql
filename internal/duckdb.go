package internal

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// BenchmarkDuckDBWithParquet executa uma query em um arquivo Parquet usando DuckDB.
func BenchmarkDuckDBWithParquet(parquetFilePath string, query string) {
	// Inicia o temporizador
	start := time.Now()

	// Conecta ao DuckDB (em memória)
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to initialize DuckDB: %v", err)
	}
	defer conn.Close()

	// Executa a query no arquivo Parquet
	queryWithParquet := fmt.Sprintf(query, parquetFilePath)
	rows, err := conn.Query(queryWithParquet)
	if err != nil {
		log.Fatalf("DuckDB query on Parquet failed: %v", err)
	}
	defer rows.Close()

	// Conta as linhas retornadas
	count := 0
	for rows.Next() {
		count++
	}

	// Calcula o tempo decorrido
	elapsed := time.Since(start)
	fmt.Printf("DuckDB query on Parquet completed in %s (rows fetched: %d)\n", elapsed, count)
}
