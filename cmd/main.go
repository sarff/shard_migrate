// cmd/migrate/main.go
package main

import (
	"database/sql"
	"fmt"
	"io"
	log "log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sarff/shard_migrate/internal/config"
	"github.com/sarff/shard_migrate/internal/database"
	"github.com/sarff/shard_migrate/internal/progress"
	"github.com/sarff/shard_migrate/internal/shard"
	"golang.org/x/exp/slog"
	_ "modernc.org/sqlite"
)

func worker(shardIndex int, tableName string, batchSize int, shardDir string, columns []string, input <-chan map[string]string, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	colsStr := "\"" + strings.Join(columns, "\", \"") + "\""
	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1]
	shardPath := shard.GetShardPath(shardIndex, shardDir)

	log.Info("Worker starting:", "Index", shardIndex, "Path", shardPath)

	// Open database connection
	db, err := database.OpenShardDB(shardPath)
	if err != nil {
		slog.Error("Failed to open shard database", "shardIndex", shardIndex, "error", err)
		return
	}
	defer db.Close()

	err = database.EnsureTable(db, tableName, columns)
	if err != nil {
		log.Error("worker ensureTable error: ", shardIndex, err)
		os.Exit(0)
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
				log.Info("Worker finished", "closing", shardIndex)
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
					log.Info("Shard", "Index", shardIndex, "rows/sec", rate)
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
			log.Info("Worker stopped by signal", "ShardIndex", shardIndex)
			return
		}
	}
}

func commitBatch(db *sql.DB, insertSQL string, batch [][]interface{}) {
	tx, err := db.Begin()
	if err != nil {
		log.Error("Begin transaction", "Err", err)
		return
	}

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Error("Prepare statement", "Err", err)
		tx.Rollback()
		return
	}

	for _, args := range batch {
		_, err := stmt.Exec(args...)
		if err != nil {
			log.Error("Insert", "Err", err)
		}
	}

	stmt.Close()
	err = tx.Commit()
	if err != nil {
		log.Info("Commit", "Err", err)
	}
}

func reader(id, startOffset int, columns []string, output chan<- map[string]string, conf *config.Config, wg *sync.WaitGroup, stopChan <-chan struct{}) {
	defer wg.Done()
	db, err := database.OpenSourceDB(conf)
	if err != nil {
		log.Error("reader %d open error: %v", id, err)
		os.Exit(0)
	}
	defer db.Close()

	log.Info("Reader starting #", strconv.Itoa(id), startOffset)

	colsStr := "\"" + strings.Join(columns, "\", \"") + "\""
	offset := startOffset

	lastReportTime := time.Now()
	rowsRead := 0

	for {
		select {
		case <-stopChan:
			log.Info("Reader stopped by signal", "ID", id)
			return
		default:
			query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d", colsStr, conf.TableName, conf.BatchSize, offset)
			rows, err := db.Query(query)
			if err != nil {
				log.Info("Reader %d query error: %v", id, err)
				time.Sleep(1 * time.Second)
				continue
			}

			count := 0
			for rows.Next() {
				select {
				case <-stopChan:
					rows.Close()
					log.Info("Reader stopped while scanning rows", "ID", id)
					return
				default:
					row := make([]interface{}, len(columns))
					rowPtrs := make([]interface{}, len(columns))
					for i := range row {
						rowPtrs[i] = &row[i]
					}

					if err := rows.Scan(rowPtrs...); err != nil {
						log.Warn("Scan", "Err", err)
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
				log.Info("Reader", "ID", id, "rows/sec", rate, "Ofset", offset)
				rowsRead = 0
				lastReportTime = now
			}

			if count == 0 {
				log.Info("Reader finished - no more rows at offset-->", strconv.Itoa(id), offset)
				return
			}

			offset += conf.BatchSize * conf.Readers
		}
	}
}

// setupLogging configures structured logging to both console and file
func setupLogging(conf *config.Config) error {
	// Create logs directory if it doesn't exist
	logsDir := filepath.Join(conf.LogDir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	// Create log file with timestamp
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	logFilePath := filepath.Join(logsDir, fmt.Sprintf("migration_%s.log", timestamp))

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// Create a multi-writer that writes to both stdout and the log file
	multiWriter := io.MultiWriter(os.Stdout, logFile)

	// Configure structured JSON logging
	logHandler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Add timestamp in a more readable format
			if a.Key == slog.TimeKey {
				return slog.Attr{
					Key:   slog.TimeKey,
					Value: slog.StringValue(a.Value.Time().Format(time.RFC3339)),
				}
			}
			return a
		},
	})

	// Set as default logger
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	slog.Info("Logging initialized", "logFile", logFilePath)
	return nil
}

func main() {
	start := time.Now()

	// Завантаження конфігурації з .env
	conf, err := config.LoadConfig()
	if err != nil {
		log.Error("Load config:", "Fatal", err)
		os.Exit(1)
	}

	// Set up structured logging
	if err = setupLogging(conf); err != nil {
		fmt.Printf("Failed to setup logging: %v\n", err)
		os.Exit(1)
	}

	if err = os.MkdirAll(conf.ShardDir, 0o755); err != nil {
		log.Error("Failed to create shard directory", "Fatal", err)
		os.Exit(1)
	}

	// Get the structure of the table
	refDB, err := database.OpenSourceDB(conf)
	if err != nil {
		log.Error("Failed to open source DB", "Fatal", err)
		os.Exit(1)
	}
	columns, err := database.GetColumnNames(refDB, conf.TableName)
	if err != nil {
		log.Error("Failed to get column names", "Fatal", err)
		os.Exit(1)
	}
	refDB.Close()

	existProgress, err := progress.LoadProgress(conf)

	log.Info("Starting migration with", "Len:", len(columns), "NumShards", conf.NumShards+1, "Readers", conf.Readers)
	log.Info("Shards will be numbered from 0 to %d (including reserved shard %d)", conf.NumShards, conf.ReservedShard)

	stopChan := make(chan struct{})

	// Channels for data to shards (from 0 to numShards, including the backup shard)
	shardChans := make([]chan map[string]string, conf.NumShards+1)
	var wg sync.WaitGroup

	//  Run wokers for all shards, including the reserve shard
	for i := 0; i <= conf.NumShards; i++ {
		shardChans[i] = make(chan map[string]string, 1000)
		wg.Add(1)
		go worker(i, conf.TableName, conf.BatchSize, conf.ShardDir, columns, shardChans[i], &wg, stopChan)
	}

	input := make(chan map[string]string, 5000)

	// start readers
	for i := 0; i < conf.Readers; i++ {
		wg.Add(1)
		go reader(i, existProgress.ResumeFrom+i*conf.BatchSize, columns, input, conf, &wg, stopChan)
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
				log.Info("Memory:", "Alloc", m.Alloc/1024/1024, "Sys", m.Sys/1024/1024, "NumGC", m.NumGC)
			case <-stopChan:
				return
			}
		}
	}()

	// Periodic forced memory clearing
	go func() {
		ticker := time.NewTicker(conf.GarbColTicker)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				log.Info("Forcing garbage collection...")
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
			log.Info("All goroutines completed successfully")
		case <-time.After(48 * time.Hour): // Максимальний час виконання
			log.Info("Migration timeout reached, stopping...")
			close(stopChan)
		}
	}()

	// Processing the distribution of rows by shards
	processed := 0
	start_pos := existProgress.ResumeFrom
	reportEvery := conf.BatchSize
	lastReportTime := time.Now()
	lastProcessed := 0

	for row := range input {
		// Safely get the value of the MOBILE_NUMBER field for sharding
		var mobileValue interface{} = nil
		if mobileNumber, exists := row["MOBILE_NUMBER"]; exists {
			mobileValue = mobileNumber
		}

		idx := shard.GetShardIndex(conf, mobileValue)

		// Additional check to avoid overruns of the array
		if idx >= len(shardChans) {
			log.Info("Warning: Invalid shard index %d, using reserved shard (%d) instead",
				idx, conf.ReservedShard)
			idx = conf.ReservedShard
		}

		select {
		case shardChans[idx] <- row:
			processed++

			if processed%reportEvery == 0 {
				now := time.Now()
				elapsed := now.Sub(lastReportTime)
				rowsPerSec := float64(processed-lastProcessed) / elapsed.Seconds()
				progRess := float64(processed+start_pos) / float64(conf.TotalRows) * 100
				log.Info("Processed:", "done", processed+start_pos, "Total", conf.TotalRows, "Remaining", conf.TotalRows-int64(processed)-int64(start_pos),
					"Speed:", rowsPerSec, "Progress:", progRess)

				if err = progress.SaveProgress(conf, existProgress, processed+start_pos); err != nil {
					slog.Error("Failed to save progress", "error", err)
				} else {
					slog.Info("Progress saved", "position", processed+start_pos)
				}
				lastReportTime = now
				lastProcessed = processed
			}
		case <-stopChan:
			log.Info("Received stop signal, closing channels...")
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
	log.Info("✅ Done migrating. Total processed: %d in %v (%.1f rows/sec)",
		processed+start_pos, totalTime, float64(processed)/totalTime.Seconds())
}
