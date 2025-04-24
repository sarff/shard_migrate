package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

func OpenSourceDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite", sourceDB)
	if err != nil {
		return nil, err
	}

	// Оптимізація налаштувань для джерельної БД
	db.SetMaxOpenConns(readers + 2)
	db.SetMaxIdleConns(readers)
	db.Exec("PRAGMA cache_size = 10000")
	db.Exec("PRAGMA mmap_size = 1073741824") // 1GB mmap для швидшого доступу
	db.Exec("PRAGMA temp_store = MEMORY")

	return db, nil
}

func getColumnNames(db *sql.DB) ([]string, error) {
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

func ensureTable(db *sql.DB, columns []string) error {
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

		log.Printf("Created table %s, adding specific indexes...", tableName)

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
					log.Printf("Warning: failed to create index on %s: %v", column, err)
				} else {
					log.Printf("Created index %s on column %s", indexName, column)
				}
			} else {
				log.Printf("Column %s not found, skipping index creation", column)
			}
		}
	}

	return nil
}
