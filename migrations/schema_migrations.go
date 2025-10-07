package migrations

import (
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

//Migration

type Migration struct {
	Version   int       `db:"version"`
	Name      string    `db:"name"`
	AppliedAt time.Time `db:"applied_at"`
}

type SchemaMigrations struct {
	db     *sql.DB
	logger *zap.Logger
}

// NewSchemaMigrations создает новый экземпляр SchemaMigrations
func NewSchemaMigrations(db *sql.DB, logger *zap.Logger) *SchemaMigrations {
	return &SchemaMigrations{
		db:     db,
		logger: logger,
	}
}

// CreateTable создает таблицу для отслеживания миграций
func (sm *SchemaMigrations) CreateMigrationTable() error {
	query := `
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );`

	_, err := sm.db.Exec(query)
	if err != nil {
		sm.logger.Error("Failed to create schema_migrations table", zap.Error(err))
		return fmt.Errorf("failed to create schema_migrations table: %w", err)
	}

	sm.logger.Info("Schema migrations table created/verified")
	return nil
}

// IsApplied проверяет, применена ли миграция
func (sm *SchemaMigrations) IsApplied(version int) (bool, error) {
	var count int
	query := "SELECT COUNT(*) FROM schema_migrations WHERE version = $1"

	err := sm.db.QueryRow(query, version).Scan(&count)
	if err != nil {
		sm.logger.Error("Failed to check if migration is applied",
			zap.Int("version", version),
			zap.Error(err))
		return false, fmt.Errorf("failed to check if migration %d is applied: %w", version, err)
	}

	return count > 0, nil
}

// RecordApplied записывает примененную миграцию
func (sm *SchemaMigrations) RecordApplied(version int, name string) error {
	query := `
	INSERT INTO schema_migrations (version, name, applied_at)
	VALUES ($1, $2, CURRENT_TIMESTAMP)`

	_, err := sm.db.Exec(query, version, name)
	if err != nil {
		sm.logger.Error("Failed to record migration",
			zap.Int("version", version),
			zap.String("name", name),
			zap.Error(err))
		return fmt.Errorf("failed to record migration %d: %w", version, err)
	}

	sm.logger.Info("Migration recorded",
		zap.Int("version", version),
		zap.String("name", name))
	return nil
}

// RemoveApplied удаляет запись о примененной миграции (для отката)
func (sm *SchemaMigrations) RemoveApplied(version int) error {
	query := "DELETE FROM schema_migrations WHERE version = $1"

	_, err := sm.db.Exec(query, version)
	if err != nil {
		sm.logger.Error("Failed to remove migration record",
			zap.Int("version", version),
			zap.Error(err))
		return fmt.Errorf("failed to remove migration record %d: %w", version, err)
	}

	sm.logger.Info("Migration record removed", zap.Int("version", version))
	return nil
}

func (sm *SchemaMigrations) GetAppliedVersions() ([]int, error) {
	rows, err := sm.db.Query("SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		sm.logger.Error("Failed to get applied versions", zap.Error(err))
		return nil, fmt.Errorf("failed to get applied versions: %w", err)
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			sm.logger.Error("Failed to scan version", zap.Error(err))
			return nil, fmt.Errorf("failed to scan version: %w", err)
		}
		versions = append(versions, version)
	}

	if err := rows.Err(); err != nil {
		sm.logger.Error("Error iterating rows", zap.Error(err))
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	sm.logger.Info("Retrieved applied versions", zap.Int("count", len(versions)))
	return versions, nil
}
