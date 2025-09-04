// migrations/schema_migrations_test.go
package migrations

import (
	"strconv"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestMigration_ParseVersionFromFilename(t *testing.T) {
	testCases := []struct {
		filename    string
		expected    int
		description string
	}{
		{"001_create_orders.up.sql", 1, "simple version"},
		{"010_add_indexes.up.sql", 10, "two digit version"},
		{"100_final_migration.up.sql", 100, "three digit version"},
		{"000_initial.up.sql", 0, "zero version"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			parts := splitFilename(tc.filename)
			if len(parts) >= 1 {
				versionStr := trimLeftZeros(parts[0])
				if versionStr == "" {
					versionStr = "0"
				}
				version, err := parseInt(versionStr)
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, version)
			}
		})
	}
}

// Вспомогательные функции для тестирования
func splitFilename(filename string) []string {
	// Убираем расширение
	name := filename[:len(filename)-len(".sql")]

	// Разделяем по подчеркиванию
	parts := []string{}
	current := ""
	for _, char := range name {
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
	return parts
}

func trimLeftZeros(s string) string {
	// Убираем ведущие нули
	for len(s) > 1 && s[0] == '0' {
		s = s[1:]
	}
	return s
}

func parseInt(s string) (int, error) {
	result := 0
	for _, char := range s {
		if char < '0' || char > '9' {
			return 0, &strconv.NumError{Func: "Atoi", Num: s, Err: strconv.ErrSyntax}
		}
		result = result*10 + int(char-'0')
	}
	return result, nil
}
