package migration

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"os"
)

const path = "migration/schema.sql"

// InitializeDatabaseSchema initializes the database schema.
func InitializeDatabaseSchema(db *sql.DB, logger *zap.Logger) error {

	schema, err := os.ReadFile(path)
	if err != nil {

		logger.Error("Error reading SQL initialization file", zap.String("path", path), zap.Error(err))
		return fmt.Errorf("can't read SQL initialization file: %w", err)
	}

	if err := executeSQL(db, string(schema), logger); err != nil {
		return err
	}

	logger.Info("Database schema initialized successfully")
	return nil
}

// executeSQL executes the SQL query in schema
func executeSQL(db *sql.DB, schema string, logger *zap.Logger) error {
	_, err := db.Exec(schema)
	if err != nil {
		logger.Error("Error executing SQL schema", zap.Error(err))
		return fmt.Errorf("can't execute SQL schema: %w", err)
	}
	return nil
}
