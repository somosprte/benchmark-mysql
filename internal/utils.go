package internal

import (
	"fmt"
	"os"
)

// LoadQueryFromFile carrega a query diretamente de um arquivo SQL
func LoadQueryFromFile(filePath string) (string, error) {
	// Lê o conteúdo do arquivo usando os.ReadFile
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read query file: %v", err)
	}
	return string(content), nil
}
