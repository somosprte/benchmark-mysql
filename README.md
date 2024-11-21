# Benchmark MySQL

## Description

This application benchmarks MySQL query performance using **DuckDB** and **Parquet files** with **Golang**.

### Features:

1. **Query performance in MySQL**:

   - Executes a query directly on the MySQL database and measures execution time.
   - Generates a Parquet file with the query results.

2. **Query performance in DuckDB using MySQL data**:

   - Uses DuckDB to query MySQL data directly and benchmarks the performance.

3. **Query performance in Parquet files using DuckDB**:
   - Reads the Parquet file generated from MySQL data and executes SQL queries on it using DuckDB.

---

## Installation

### Prerequisites

1. Install **Go** (v1.20 or higher): [Download here](https://go.dev/dl/).
2. Install **DuckDB** (optional for external verification): [Download here](https://duckdb.org/).
3. Have access to a MySQL database.

### Steps

1. Clone the repository:

2. Install the dependencies:
   ```bash
   go mod tidy
   ```

## Run

```bash
go run main.go
```

## Build

If you want to build the application, put the .env file in the same directory as the binary file.

For Windows:

```bash
GOOS=windows GOARCH=amd64 go build -o out/benchmark-mysql.exe main.go
```

For Linux:

```bash
GOOS=linux GOARCH=amd64 go build -o out/benchmark-mysql main.go
```

For MacOS:

```bash
GOOS=darwin GOARCH=amd64 go build -o out/benchmark-mysql main.go
```
