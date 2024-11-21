package internal

import (
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// BenchmarkDuckDBWithMySQL executa uma query usando DuckDB conectado ao MySQL.
func BenchmarkDuckDBWithMySQL(mysqlDSN string, query string) {
	// Inicia o temporizador
	start := time.Now()

	// Obtém o DSN do MySQL a partir das variáveis de ambiente
	mysqlDSN = os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		log.Fatal("MYSQL_DSN is not set in the environment variables")
	}

	// Converte o DSN para o formato DuckDB
	duckdbDSN, err := convertMySQLDSNToDuckDB(mysqlDSN)
	if err != nil {
		log.Fatalf("Failed to convert MySQL DSN to DuckDB format: %v", err)
	}
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

	// Conecta ao banco MySQL usando ATTACH
	attachCommand := fmt.Sprintf("ATTACH '%s' AS mysqldb (TYPE MYSQL);", duckdbDSN)
	_, err = conn.Exec(attachCommand)
	if err != nil {
		log.Fatalf("Failed to attach MySQL database to DuckDB: %v", err)
	}

	// Define o escopo para o banco anexado
	_, err = conn.Exec("USE mysqldb;")
	if err != nil {
		log.Fatalf("Failed to use attached MySQL database in DuckDB: %v", err)
	}

	// Executa a query fornecida
	rows, err := conn.Query(query)
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

// convertMySQLDSNToDuckDB converte uma string DSN do MySQL para o formato usado pelo DuckDB
func convertMySQLDSNToDuckDB(mysqlDSN string) (string, error) {
	// Analisa o DSN do MySQL
	if !strings.HasPrefix(mysqlDSN, "mysql://") {
		mysqlDSN = "mysql://" + mysqlDSN
	}

	parsedDSN, err := url.Parse(mysqlDSN)
	if err != nil {
		return "", fmt.Errorf("failed to parse MySQL DSN: %v", err)
	}

	// Extrai componentes
	user := parsedDSN.User.Username()
	password, _ := parsedDSN.User.Password()
	host := parsedDSN.Hostname()
	port := parsedDSN.Port()
	if port == "" {
		port = "3306" // Porta padrão do MySQL
	}
	database := strings.TrimPrefix(parsedDSN.Path, "/")

	// Constrói o DSN para o DuckDB
	duckdbDSN := fmt.Sprintf("host=%s port=%s user=%s password=%s database=%s", host, port, user, password, database)
	return duckdbDSN, nil
}
