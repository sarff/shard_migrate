package database

import (
	"database/sql"
	"fmt"
	log "log/slog"
	"strings"

	"github.com/sarff/shard_migrate/internal/config"
	"golang.org/x/exp/slog"
)

func OpenSourceDB(conf *config.Config) (*sql.DB, error) {
	db, err := sql.Open("sqlite", conf.SourceDB)
	if err != nil {
		return nil, err
	}

	// Optimize settings for the source database
	db.SetMaxOpenConns(conf.Readers + 2)
	db.SetMaxIdleConns(conf.Readers)
	db.Exec("PRAGMA cache_size = 10000")
	db.Exec("PRAGMA mmap_size = 1073741824") // 1GB
	db.Exec("PRAGMA temp_store = MEMORY")

	return db, nil
}

func GetColumnNames(db *sql.DB, tableName string) ([]string, error) {
	rows, err := db.Query("PRAGMA table_info(" + tableName + ")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk)
		if err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, nil
}

func EnsureTable(db *sql.DB, tableName string, columns []string) error {
	// Check if the table exists
	var count int
	err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", tableName).Scan(&count)
	if err != nil {
		return err
	}

	// If the table does not exist, create it and add the indexes
	if count == 0 {
		colDefs := make([]string, len(columns))
		for i, col := range columns {
			colDefs[i] = "\"" + col + "\" TEXT"
		}
		query := "CREATE TABLE IF NOT EXISTS " + tableName + " (" + strings.Join(colDefs, ", ") + ")"
		_, err = db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}

		log.Info("Created table, adding specific indexes...", tableName)

		// indexes
		indexColumns := []string{"ИНН", "MOBILE_NUMBER", "СНИЛС"}

		for _, column := range indexColumns {
			columnExists := false
			for _, col := range columns {
				if col == column {
					columnExists = true
					break
				}
			}

			if columnExists {
				indexName := fmt.Sprintf("idx_%s_%s", tableName, column)
				indexQuery := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(\"%s\")",
					indexName, tableName, column)

				_, err = db.Exec(indexQuery)
				if err != nil {
					log.Info("Warning: failed to create index on %s: %v", column, err)
				} else {
					log.Info("Created index %s on column ", indexName, column)
				}
			} else {
				log.Info("Column %s not found, skipping index creation", column)
			}
		}
	}

	return nil
}

// OpenShardDB opens a shard database with optimized settings
func OpenShardDB(shardPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", shardPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open shard database: %w", err)
	}

	// Optimize settings for shard database
	db.Exec("PRAGMA synchronous = NORMAL") // compromise between OFF and FULL
	db.Exec("PRAGMA foreign_keys = OFF")
	db.Exec("PRAGMA journal_mode = WAL")
	db.Exec("PRAGMA temp_store = MEMORY")
	db.Exec("PRAGMA cache_size = 5000")

	slog.Info("Shard database opened with optimized settings", "path", shardPath)
	return db, nil
}
