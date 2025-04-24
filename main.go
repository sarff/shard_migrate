// cmd/migrate/main.go
package main

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	sourceDB      = "/Users/dmit/Pycha/TG_Andrew/bot/data/clients.db"
	tableName     = "clients"
	shardDir      = "/Users/dmit/Pycha/TG_Andrew/files/shards"
	numShards     = 10 // Shards 0 through 9 for regular numbers
	reservedShard = 10 // Reserve shard (index 10) for empty values
	resumeFrom    = 13912000
	batchSize     = 6000
	readers       = 6
	totalRows     = 1217065012
	garbcolticker = 5 * time.Minute
)

func getShardIndex(value interface{}) int {
	if value == nil {
		return reservedShard
	}

	strValue, ok := value.(string)
	if !ok {
		return reservedShard
	}

	if strings.TrimSpace(strValue) == "" {
		return reservedShard
	}

	h := sha256.Sum256([]byte(strValue))

	hexHash := fmt.Sprintf("%x", h)

	val := new(big.Int)
	val.SetString(hexHash, 16)

	mod := new(big.Int).SetInt64(int64(numShards))
	result := new(big.Int).Mod(val, mod)

	return int(result.Int64())
}

func getShardPath(index int) string {
	return filepath.Join(shardDir, "clients_shard_"+pad(index)+".db")
}

func pad(n int) string {
	if n < 10 {
		return "0" + fmt.Sprint(n)
	}
	return fmt.Sprint(n)
}

func openSourceDB() (*sql.DB, error) {
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

func worker(shardIndex int, columns []string, input <-chan map[string]string, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	colsStr := "\"" + strings.Join(columns, "\", \"") + "\""
	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1]
	shardPath := getShardPath(shardIndex)

	log.Printf("Worker %d starting, shard path: %s", shardIndex, shardPath)

	db, err := sql.Open("sqlite", shardPath)
	if err != nil {
		log.Fatalf("worker %d open error: %v", shardIndex, err)
	}
	defer db.Close()

	// aggresive SQLite for write
	db.Exec("PRAGMA synchronous = OFF")
	db.Exec("PRAGMA journal_mode = MEMORY")
	db.Exec("PRAGMA temp_store = MEMORY")
	db.Exec("PRAGMA cache_size = 5000")
	db.Exec("PRAGMA page_size = 8192")
	db.Exec("PRAGMA locking_mode = EXCLUSIVE")

	err = ensureTable(db, columns)
	if err != nil {
		log.Fatalf("worker %d ensureTable error: %v", shardIndex, err)
	}

	insertSQL := "INSERT INTO " + tableName + " (" + colsStr + ") VALUES (" + placeholders + ")"
	batch := make([][]interface{}, 0, batchSize)

	lastCommitTime := time.Now()
	rowsInserted := 0

	for {
		select {
		case rowMap, ok := <-input:
			if !ok {
				if len(batch) > 0 {
					commitBatch(db, insertSQL, batch)
				}
				log.Printf("Worker %d finished, closing", shardIndex)
				return
			}

			args := make([]interface{}, len(columns))
			for i, col := range columns {
				args[i] = rowMap[col]
			}
			batch = append(batch, args)

			if len(batch) >= batchSize {
				commitBatch(db, insertSQL, batch)
				batch = make([][]interface{}, 0, batchSize) //  Create a new slide to avoid reuse issues

				//  Speed tracking for this shard
				now := time.Now()
				elapsed := now.Sub(lastCommitTime)
				rowsInserted += batchSize

				if elapsed > 5*time.Second {
					rate := float64(rowsInserted) / elapsed.Seconds()
					log.Printf("Shard %d: %.1f rows/sec", shardIndex, rate)
					rowsInserted = 0
					lastCommitTime = now
				}

				// Free up your memory periodically
				if rowsInserted%100000 == 0 {
					runtime.GC()
				}
			}

		case <-stopChan:
			if len(batch) > 0 {
				commitBatch(db, insertSQL, batch)
			}
			log.Printf("Worker %d stopped by signal", shardIndex)
			return
		}
	}
}

func commitBatch(db *sql.DB, insertSQL string, batch [][]interface{}) {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("Begin transaction error: %v", err)
		return
	}

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Printf("Prepare statement error: %v", err)
		tx.Rollback()
		return
	}

	for _, args := range batch {
		_, err := stmt.Exec(args...)
		if err != nil {
			log.Printf("Insert error: %v", err)
		}
	}

	stmt.Close()
	err = tx.Commit()
	if err != nil {
		log.Printf("Commit error: %v", err)
	}
}

func reader(id, startOffset int, columns []string, output chan<- map[string]string, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	db, err := openSourceDB()
	if err != nil {
		log.Fatalf("reader %d open error: %v", id, err)
	}
	defer db.Close()

	log.Printf("Reader %d starting from offset %d", id, startOffset)

	colsStr := "\"" + strings.Join(columns, "\", \"") + "\""
	offset := startOffset

	lastReportTime := time.Now()
	rowsRead := 0

	for {
		select {
		case <-stopChan:
			log.Printf("Reader %d stopped by signal", id)
			return
		default:
			query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d", colsStr, tableName, batchSize, offset)
			rows, err := db.Query(query)
			if err != nil {
				log.Printf("Reader %d query error: %v", id, err)
				time.Sleep(1 * time.Second)
				continue
			}

			count := 0
			for rows.Next() {
				select {
				case <-stopChan:
					rows.Close()
					log.Printf("Reader %d stopped while scanning rows", id)
					return
				default:
					row := make([]interface{}, len(columns))
					rowPtrs := make([]interface{}, len(columns))
					for i := range row {
						rowPtrs[i] = &row[i]
					}

					if err := rows.Scan(rowPtrs...); err != nil {
						log.Printf("Scan error: %v", err)
						continue
					}

					rowMap := make(map[string]string)
					for i, val := range row {
						if val != nil {
							rowMap[columns[i]] = fmt.Sprintf("%v", val)
						} else {
							rowMap[columns[i]] = ""
						}
					}

					output <- rowMap
					count++
					rowsRead++
				}
			}
			rows.Close()

			// Check your reading speed
			now := time.Now()
			elapsed := now.Sub(lastReportTime)
			if elapsed > 5*time.Second {
				rate := float64(rowsRead) / elapsed.Seconds()
				log.Printf("Reader %d: %.1f rows/sec | Offset: %d", id, rate, offset)
				rowsRead = 0
				lastReportTime = now
			}

			if count == 0 {
				log.Printf("Reader %d finished - no more rows at offset %d", id, offset)
				return
			}

			offset += batchSize * readers
		}
	}
}

func main() {
	start := time.Now()

	if err := os.MkdirAll(shardDir, 0755); err != nil {
		log.Fatalf("Failed to create shard directory: %v", err)
	}

	// Get the structure of the table
	refDB, err := openSourceDB()
	if err != nil {
		log.Fatalf("Failed to open source DB: %v", err)
	}
	columns, err := getColumnNames(refDB)
	if err != nil {
		log.Fatalf("Failed to get column names: %v", err)
	}
	refDB.Close()

	log.Printf("Starting migration with %d columns, %d shards, %d readers", len(columns), numShards+1, readers)
	log.Printf("Shards will be numbered from 0 to %d (including reserved shard %d)", numShards, reservedShard)

	stopChan := make(chan struct{})

	// Channels for data to shards (from 0 to numShards, including the backup shard)
	shardChans := make([]chan map[string]string, numShards+1)
	var wg sync.WaitGroup

	//  Run wokers for all shards, including the reserve shard
	for i := 0; i <= numShards; i++ {
		shardChans[i] = make(chan map[string]string, 1000)
		wg.Add(1)
		go worker(i, columns, shardChans[i], &wg, stopChan)
	}

	input := make(chan map[string]string, 5000)

	// start readers
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go reader(i, resumeFrom+i*batchSize, columns, input, &wg, stopChan)
	}

	// Zabbix ))
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		var m runtime.MemStats
		for {
			select {
			case <-ticker.C:
				runtime.ReadMemStats(&m)
				log.Printf("Memory: Alloc=%v MiB, Sys=%v MiB, NumGC=%v",
					m.Alloc/1024/1024, m.Sys/1024/1024, m.NumGC)
			case <-stopChan:
				return
			}
		}
	}()

	// Periodic forced memory clearing
	go func() {
		ticker := time.NewTicker(garbcolticker)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				log.Println("Forcing garbage collection...")
				runtime.GC()
			case <-stopChan:
				return
			}
		}
	}()

	// Progress tracking and timeout handling
	go func() {
		wgDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(wgDone)
		}()

		select {
		case <-wgDone:
			log.Println("All goroutines completed successfully")
		case <-time.After(48 * time.Hour): // Максимальний час виконання
			log.Println("Migration timeout reached, stopping...")
			close(stopChan)
		}
	}()

	// Processing the distribution of rows by shards
	processed := 0
	start_pos := resumeFrom
	reportEvery := batchSize
	lastReportTime := time.Now()
	lastProcessed := 0

	for row := range input {
		// Safely get the value of the MOBILE_NUMBER field for sharding
		var mobileValue interface{} = nil
		if mobileNumber, exists := row["MOBILE_NUMBER"]; exists {
			mobileValue = mobileNumber
		}

		idx := getShardIndex(mobileValue)

		// Additional check to avoid overruns of the array
		if idx >= len(shardChans) {
			log.Printf("Warning: Invalid shard index %d, using reserved shard (%d) instead",
				idx, reservedShard)
			idx = reservedShard
		}

		select {
		case shardChans[idx] <- row:
			processed++

			if processed%reportEvery == 0 {
				now := time.Now()
				elapsed := now.Sub(lastReportTime)
				rowsPerSec := float64(processed-lastProcessed) / elapsed.Seconds()
				progress := float64(processed+start_pos) / float64(totalRows) * 100
				log.Printf("Processed: %d | Total: %d | Remaining: %d | Speed: %.1f rows/sec | Progress: %.1f",
					processed+start_pos, totalRows, totalRows-processed-start_pos, rowsPerSec, progress)
				lastReportTime = now
				lastProcessed = processed
			}
		case <-stopChan:
			log.Println("Received stop signal, closing channels...")
			close(input)
			goto cleanup
		}
	}

cleanup:
	// Close all channels after completion or when stopped
	for _, ch := range shardChans {
		close(ch)
	}

	// waiting for the completion of all the gorutinas
	wg.Wait()

	totalTime := time.Since(start)
	log.Printf("✅ Done migrating. Total processed: %d in %v (%.1f rows/sec)",
		processed+start_pos, totalTime, float64(processed)/totalTime.Seconds())
}
