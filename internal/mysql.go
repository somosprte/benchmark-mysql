package internal

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
)

// BenchmarkMySQL executa uma query no MySQL, mede o tempo de resposta e gera um arquivo Parquet
func BenchmarkMySQL(dsn, query, parquetFilePath string) {
	// Conecta ao MySQL
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer db.Close()

	// Inicia o benchmark da consulta
	start := time.Now()

	// Executa a query
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	// Medir o tempo da consulta
	queryElapsed := time.Since(start)
	fmt.Printf("MySQL query completed in %s\n", queryElapsed)

	// Obtém os nomes das colunas
	columns, err := rows.Columns()
	if err != nil {
		log.Fatalf("Failed to get columns: %v", err)
	}

	// Extrai os dados das linhas e escreve o Parquet
	writeParquet(rows, columns, parquetFilePath)
}

// writeParquet extrai os dados das linhas e gera um arquivo Parquet
func writeParquet(rows *sql.Rows, columns []string, parquetFilePath string) {
	// Prepara os dados para o Parquet
	var data [][]string

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Lê os dados das linhas
	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}

		record := make([]string, len(columns))
		for i, val := range values {
			record[i] = string(val)
		}
		data = append(data, record)
	}

	// Inicia o benchmark de escrita no Parquet
	start := time.Now()

	// Escreve os dados no arquivo Parquet
	WriteParquet(data, columns, parquetFilePath)

	// Calcula o tempo de escrita
	parquetElapsed := time.Since(start)

	// Obter o tamanho do arquivo Parquet
	fileInfo, err := os.Stat(parquetFilePath)
	if err != nil {
		log.Fatalf("Failed to get Parquet file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Exibe as métricas do Parquet
	fmt.Printf("Parquet file written in %s\n", parquetElapsed)
	fmt.Printf("Parquet file size: %.2f KB\n", float64(fileSize)/1024)
}
