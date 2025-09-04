// migrations/helpers_test.go
package migrations

import (
	"strconv"
)

// Эти функции дублируют логику из основного кода для тестирования

func parseVersionFromFilename(filename string) int {
	parts := []string{}
	current := ""

	// Находим позицию последней точки перед .sql
	dotPos := -1
	for i := len(filename) - 5; i >= 0; i-- { // -5 чтобы пропустить ".sql"
		if filename[i] == '.' {
			dotPos = i
			break
		}
	}

	// Берем часть до последней точки
	namePart := filename
	if dotPos > 0 {
		namePart = filename[:dotPos]
	}

	// Разделяем по подчеркиванию
	for _, char := range namePart {
		if char == '_' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(char)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	if len(parts) < 1 {
		return 0
	}

	versionStr := parts[0]
	// Убираем ведущие нули
	for len(versionStr) > 1 && versionStr[0] == '0' {
		versionStr = versionStr[1:]
	}
	if versionStr == "" {
		versionStr = "0"
	}

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0
	}

	return version
}
