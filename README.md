# SQLite Sharding Migration Tool

A high-performance Go application for migrating large SQLite databases into multiple shards based on mobile phone numbers. This tool is designed to handle billions of records efficiently using parallel processing and optimized SQLite operations.

## Features

- **Parallel Processing**: Utilizes multiple readers to extract data from the source database
- **Automatic Sharding**: Distributes data across multiple SQLite databases based on SHA256 hashing
- **Progress Tracking**: Saves migration progress to allow resuming interrupted processes
- **Memory Management**: Includes automatic garbage collection and memory optimization
- **Structured Logging**: Comprehensive JSON logging with performance metrics
- **Index Management**: Automatically creates necessary indexes on shard databases
- **Reserved Shard**: Special shard for handling null or empty values

## Prerequisites

- Go 1.23+ (tested with Go 1.24.1)
- SQLite3
- Sufficient disk space for both source database and shards

## Installation

```bash
git clone https://github.com/sarff/shard_migrate.git
cd shard_migrate
go mod download
```

## Configuration

The application uses environment variables for configuration. Copy the `.env.example` file to `.env` and adjust the values:

```bash
cp .env.example .env
```

Configuration options:

| Variable | Description | Default Value |
|----------|-------------|---------------|
| SOURCE_DB | Path to source SQLite database | clients.db |
| TABLE_NAME | Name of the table to migrate | clients |
| SHARD_DIR | Directory to store shard databases | /shards |
| LOG_DIR | Directory for log files | ./tmp |
| NUM_SHARDS | Number of shards to create | 10 |
| RESERVED_SHARD | Index of reserved shard for null values | 10 |
| BATCH_SIZE | Number of records per batch | 6000 |
| READERS | Number of parallel readers | 6 |
| TOTAL_ROWS | Total number of rows to migrate | 1217065012 |
| GARB_COL_TICKER_MINUTES | Garbage collection interval (minutes) | 5 |

## Usage

Run the migration:

```bash
go run cmd/main.go
```

The application will:
1. Load configuration from environment variables
2. Create shard databases if they don't exist
3. Set up indexes on INN, MOBILE_NUMBER, and SNILS columns
4. Start parallel readers and workers
5. Begin migrating data with progress logging

## Project Structure

```
.
├── cmd/
│   └── main.go                 # Main application entry point
├── internal/
│   ├── config/
│   │   └── config.go           # Configuration management
│   ├── database/
│   │   └── database.go         # Database operations
│   ├── progress/
│   │   └── progress.go         # Progress tracking
│   └── shard/
│       ├── shard.go            # Sharding logic
│       └── shard_test.go       # Sharding tests
├── tmp/
│   └── logs/                   # Log files directory
├── .env.example                # Example configuration
├── go.mod                      # Go module file
└── go.sum                      # Go dependencies

```

## How It Works

1. **Data Extraction**: Multiple readers extract data from the source database in parallel
2. **Sharding Logic**: Each record is assigned to a shard based on:
    - SHA256 hash of the MOBILE_NUMBER field
    - Modulo operation to determine shard index
    - Records with null/empty values go to the reserved shard
3. **Data Loading**: Workers write batches of data to their respective shard databases
4. **Progress Saving**: Periodically saves progress to resume from last checkpoint if interrupted

## Performance Optimization

- **WAL Mode**: Write-Ahead Logging for better concurrency
- **MMAP**: Memory-mapped I/O for faster reads (1GB)
- **Batch Processing**: Reduces transaction overhead
- **Memory Management**: Periodic garbage collection to prevent memory leaks
- **Concurrent Processing**: Multiple readers and workers for parallel execution

## Monitoring

The application provides:
- Real-time progress updates with rows/second metrics
- Memory usage statistics
- Per-shard performance logging
- Structured JSON logs for easy parsing

## Error Handling

- Graceful shutdown on interruption
- Automatic progress recovery on restart
- Detailed error logging with context
- Transaction rollback on failures

## Limitations

- Designed specifically for SQLite databases
- Requires sufficient memory for parallel operations
- Sharding is based on MOBILE_NUMBER field (must exist in source table)

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is open source and available under the [MIT License](LICENSE).

## Acknowledgments

- Uses the `modernc.org/sqlite` pure Go SQLite driver
- Built with the power of Go's concurrency model
