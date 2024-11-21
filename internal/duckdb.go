package internal

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// BenchmarkDuckDBWithMySQL executa uma query usando DuckDB conectado ao MySQL.
func BenchmarkDuckDBWithMySQL(mysqlDSN string, query string) {
	// Inicia o temporizador
	start := time.Now()

	// Conecta ao DuckDB (em memória)
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("Failed to initialize DuckDB: %v", err)
	}
	defer conn.Close()

	// Instala e carrega o plugin MySQL no DuckDB
	_, err = conn.Exec("INSTALL mysql; LOAD mysql;")
	if err != nil {
		log.Fatalf("Failed to load MySQL extension in DuckDB: %v", err)
	}

	// Executa a consulta no MySQL diretamente usando mysql_scan
	queryWithMySQL := fmt.Sprintf("SELECT * FROM mysql_scan('%s')", mysqlDSN)
	rows, err := conn.Query(queryWithMySQL)
	if err != nil {
		log.Fatalf("Failed to execute query in DuckDB: %v", err)
	}
	defer rows.Close()

	// Conta as linhas retornadas
	count := 0
	for rows.Next() {
		count++
	}

	// Calcula o tempo decorrido
	elapsed := time.Since(start)
	fmt.Printf("DuckDB query completed in %s (rows fetched: %d)\n", elapsed, count)
}

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
