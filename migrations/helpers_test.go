package migrations

import (
	"strconv"
)

func parseVersionFromFilename(filename string) int {
	parts := []string{}
	current := ""

	dotPos := -1
	for i := len(filename) - 5; i >= 0; i-- { // -5 чтобы пропустить ".sql"
		if filename[i] == '.' {
			dotPos = i
			break
		}
	}

	namePart := filename
	if dotPos > 0 {
		namePart = filename[:dotPos]
	}

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
