package migrations

import (
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

//go:embed versions/*.sql
var migrationsFS embed.FS

// MigrationFile представляет файл миграции
type MigrationFile struct {
	Version int
	Name    string
	Path    string
	Content string
	Type    MigrationType // up или down
}

type MigrationType string

const (
	MigrationUp   MigrationType = "up"
	MigrationDown MigrationType = "down"
)

// MigrationManager управляет миграциями
type MigrationManager struct {
	db               *sql.DB
	logger           *zap.Logger
	schemaMigrations *SchemaMigrations
}

// NewMigrationManager создает новый менеджер миграций
func NewMigrationManager(db *sql.DB, logger *zap.Logger) *MigrationManager {
	schemaMigrations := NewSchemaMigrations(db, logger)
	return &MigrationManager{
		db:               db,
		logger:           logger,
		schemaMigrations: schemaMigrations,
	}
}

// Up применяет все непримененные миграции
func (mgr *MigrationManager) Up() error {
	mgr.logger.Info("Starting database migrations (Up)")

	if err := mgr.schemaMigrations.CreateMigrationTable(); err != nil {
		return fmt.Errorf("can't create migrations table: %w", err)
	}

	migrationFiles, err := mgr.getMigrationFiles(MigrationUp)
	if err != nil {
		return fmt.Errorf("can't get migrations files: %w", err)
	}

	sort.Slice(migrationFiles, func(i, j int) bool {
		return migrationFiles[i].Version < migrationFiles[j].Version
	})

	appliedCount := 0
	for _, migration := range migrationFiles {
		applied, err := mgr.schemaMigrations.IsApplied(migration.Version)
		if err != nil {
			return fmt.Errorf("can't check if migration %d is applied: %w", migration.Version, err)
		}

		if !applied {
			mgr.logger.Info("Applying migration (Up)",
				zap.Int("version", migration.Version),
				zap.String("name", migration.Name))

			if err := mgr.executeMigration(&migration); err != nil {
				return fmt.Errorf("can't apply migration %d: %w", migration.Version, err)
			}

			if err := mgr.schemaMigrations.RecordApplied(migration.Version, migration.Name); err != nil {
				return fmt.Errorf("can't record migration %d: %w", migration.Version, err)
			}

			appliedCount++
		}
	}

	mgr.logger.Info("Migrations completed", zap.Int("applied_count", appliedCount))
	return nil
}

// Down откатывает последнюю миграцию
func (mgr *MigrationManager) Down() error {
	mgr.logger.Info("Starting database migrations rollback (Down)")

	versions, err := mgr.schemaMigrations.GetAppliedVersions()
	if err != nil {
		return fmt.Errorf("failed to get applied versions: %w", err)
	}

	if len(versions) == 0 {
		mgr.logger.Info("No migrations to rollback")
		return nil
	}

	lastVersion := versions[len(versions)-1]

	migrationFiles, err := mgr.getMigrationFiles(MigrationDown)
	if err != nil {
		return fmt.Errorf("failed to get migrations files: %w", err)
	}

	var targetMigration *MigrationFile
	for _, migration := range migrationFiles {
		if migration.Version == lastVersion {
			targetMigration = &migration
			break
		}
	}

	if targetMigration == nil {
		return fmt.Errorf("down migration file not found for version %d", lastVersion)
	}

	mgr.logger.Info("Rolling back migration",
		zap.Int("version", targetMigration.Version),
		zap.String("name", targetMigration.Name))

	if err := mgr.executeMigration(targetMigration); err != nil {
		return fmt.Errorf("failed to rollback migration %d: %w", targetMigration.Version, err)
	}

	if err := mgr.schemaMigrations.RemoveApplied(targetMigration.Version); err != nil {
		return fmt.Errorf("failed to remove migration record %d: %w", targetMigration.Version, err)
	}

	mgr.logger.Info("Migration rolled back",
		zap.Int("version", targetMigration.Version),
		zap.String("name", targetMigration.Name))
	return nil
}

// executeMigration выполняет миграцию в транзакции
func (mgr *MigrationManager) executeMigration(migration *MigrationFile) error {
	tx, err := mgr.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			mgr.logger.Info("Transaction rolled back due to error")
		}
	}()

	statements := strings.Split(migration.Content, ";")

	for i, stmt := range statements {
		trimmedStmt := strings.TrimSpace(stmt)
		if trimmedStmt == "" {
			continue
		}

		mgr.logger.Debug("Executing statement",
			zap.Int("statement_number", i+1),
			zap.Int("migration_version", migration.Version))

		if _, err = tx.Exec(trimmedStmt); err != nil {
			mgr.logger.Error("Error executing SQL statement",
				zap.Int("migration_version", migration.Version),
				zap.Int("statement_number", i+1),
				zap.String("statement", trimmedStmt[:min(len(trimmedStmt), 100)]),
				zap.Error(err))
			return fmt.Errorf("failed to execute statement #%d in migration %d: %w", i+1, migration.Version, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// getMigrationFiles получает список файлов миграций
func (mgr *MigrationManager) getMigrationFiles(migrationType MigrationType) ([]MigrationFile, error) {
	//pattern := fmt.Sprintf("versions/*.%s.sql", string(migrationType))
	entries, err := migrationsFS.ReadDir("versions")
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	var migrations []MigrationFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		filename := entry.Name()
		if !strings.HasSuffix(filename, fmt.Sprintf(".%s.sql", string(migrationType))) {
			continue
		}

		parts := strings.Split(filename, "_")
		if len(parts) < 2 {
			continue
		}

		versionStr := strings.TrimLeft(parts[0], "0")
		if versionStr == "" {
			versionStr = "0"
		}

		version, err := strconv.Atoi(versionStr)
		if err != nil {
			mgr.logger.Warn("Invalid migration version in filename",
				zap.String("filename", filename),
				zap.Error(err))
			continue
		}

		content, err := migrationsFS.ReadFile(filepath.Join("versions", filename))
		if err != nil {
			return nil, fmt.Errorf("failed to read migration file %s: %w", filename, err)
		}

		name := strings.TrimSuffix(strings.Join(parts[1:], "_"), fmt.Sprintf(".%s.sql", string(migrationType)))

		migrations = append(migrations, MigrationFile{
			Version: version,
			Name:    name,
			Path:    filepath.Join("versions", filename),
			Content: string(content),
			Type:    migrationType,
		})
	}

	return migrations, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
