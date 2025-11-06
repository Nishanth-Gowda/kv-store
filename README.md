# KV Store

A high-performance, in-memory key-value store with Write-Ahead Logging (WAL) support, built in Go. Features LRU eviction, TTL support, and HTTP API.

## Features

- **LRU Cache**: Least Recently Used eviction policy for efficient memory management
- **Write-Ahead Logging (WAL)**: Durable writes with automatic recovery on restart
- **TTL Support**: Time-to-live expiration for cache entries
- **HTTP API**: RESTful API for easy integration
- **Thread-Safe**: Concurrent read/write operations with proper locking
- **Segment Rotation**: Automatic WAL segment rotation and cleanup
- **CRC Verification**: Data integrity checks for WAL entries
- **Automatic Recovery**: Restores cache state from WAL on startup

## Architecture

```
┌─────────────┐
│  HTTP API   │ (Echo Framework)
└──────┬──────┘
       │
┌──────▼──────┐
│ LRU Cache   │ (In-Memory)
└──────┬──────┘
       │
┌──────▼──────┐
│     WAL     │ (Write-Ahead Log)
└─────────────┘
```

### Components

- **Cache**: In-memory LRU cache with configurable capacity
- **WAL**: Write-ahead log for durability with segment rotation
- **HTTP Server**: RESTful API endpoints for cache operations

## Installation

### Prerequisites

- Go 1.24.5 or higher

### Build

```bash
git clone https://github.com/nishanth-gowda/kv-store.git
cd kv-store
go mod download
go build -o kv-store .
```

### Run

```bash
./kv-store
```

The server will start on `http://localhost:8080`

## Usage

### HTTP API

#### Set a Key-Value Pair

```bash
# Without TTL
curl -X POST "http://localhost:8080/set?key=mykey&value=myvalue"

# With TTL (e.g., 5 minutes)
curl -X POST "http://localhost:8080/set?key=mykey&value=myvalue&ttl=5m"
```

**Response**: `200 OK` on success, `400 Bad Request` on invalid input

#### Get a Value

```bash
curl "http://localhost:8080/get?key=mykey"
```

**Response**: 
- `200 OK` with the value on success
- `404 Not Found` if key doesn't exist

#### Delete a Key

```bash
curl -X DELETE "http://localhost:8080/delete?key=mykey"
```

**Response**: `200 OK` on success

### Programmatic Usage

```go
package main

import (
    "github.com/nishanth-gowda/kv-store/cache"
    "time"
)

func main() {
    // Create cache with WAL enabled
    c, err := cache.NewLRUCache(
        1000,              // capacity
        "./wal",           // WAL directory (empty string disables WAL)
        false,             // forceSync (true = fsync on every write)
        10*1024*1024,      // maxFileSize (10MB)
        10,                // maxSegments
    )
    if err != nil {
        panic(err)
    }
    defer c.Close()

    // Set a value with TTL
    err = c.Set("key1", "value1", 5*time.Minute)
    if err != nil {
        panic(err)
    }

    // Get a value
    value, ok := c.Get("key1")
    if ok {
        fmt.Println("Value:", value)
    }

    // Delete a key
    err = c.Delete("key1")
    if err != nil {
        panic(err)
    }
}
```

## Configuration

### Cache Parameters

- **capacity**: Maximum number of entries in the cache
- **walDirectory**: Directory path for WAL files (empty string disables WAL)
- **forceSync**: If `true`, fsync on every write (slower but more durable)
- **maxFileSize**: Maximum size of a WAL segment file before rotation (bytes)
- **maxSegments**: Maximum number of WAL segments to keep

### WAL Configuration

The WAL automatically:
- Rotates segments when they exceed `maxFileSize`
- Cleans up old segments when exceeding `maxSegments`
- Syncs to disk every 100ms (configurable via `syncInterval`)
- Recovers all entries on cache initialization

## Write-Ahead Logging (WAL)

### How It Works

1. **Write Path**: All mutations (SET/DELETE) are written to WAL before updating the in-memory cache
2. **Segment Rotation**: When a segment exceeds `maxFileSize`, a new segment is created
3. **Periodic Sync**: Buffered writes are flushed to disk every 100ms
4. **Recovery**: On startup, all WAL entries are replayed to restore cache state

### WAL Entry Format

Each WAL entry contains:
- **Type**: SET or DELETE operation
- **Sequence Number**: Monotonically increasing sequence for ordering
- **Key**: Cache key
- **Value**: Serialized value (gob encoding)
- **ExpiresAtUnixNano**: Expiration timestamp (0 = no expiration)
- **CRC**: CRC32 checksum for integrity verification

### Segment Files

WAL segments are stored as:
```
wal/
├── wal-segment-0
├── wal-segment-1
├── wal-segment-2
└── ...
```

## Testing

### Run Tests

```bash
go test ./...
```

### Run Benchmarks

```bash
# Run all benchmarks
go test -bench=. ./tests -benchmem

# Run specific benchmark
go test -bench=BenchmarkSetWithWAL ./tests -benchmem

# Run with longer duration for more accurate results
go test -bench=. ./tests -benchmem -benchtime=5s
```

### Available Benchmarks

- `BenchmarkSetWithoutWAL` - Set operations without WAL
- `BenchmarkSetWithWAL` - Set operations with WAL
- `BenchmarkSetWithWALForceSync` - Set with WAL and force sync
- `BenchmarkGet` / `BenchmarkGetWithWAL` - Get operations
- `BenchmarkDelete` / `BenchmarkDeleteWithWAL` - Delete operations
- `BenchmarkSetLargeValues` - Large value handling
- `BenchmarkSetWithTTL` - TTL operations
- `BenchmarkRecovery` - WAL recovery performance
- `BenchmarkMixedWorkload` - Mixed Set/Get/Delete workload
- `BenchmarkLRUEviction` - LRU eviction behavior

## Project Structure

```
kv-store/
├── cache/
│   └── cache.go          # LRU cache implementation
├── wal/
│   └── wal.go            # Write-ahead log implementation
├── utils/
│   └── utils.go          # Utility functions
├── tests/
│   └── main_test.go      # Benchmark tests
├── main.go               # HTTP server and API handlers
├── go.mod                # Go module dependencies
└── README.md             # This file
```

## API Reference

### Cache Methods

#### `NewLRUCache(capacity, walDirectory, forceSync, maxFileSize, maxSegments) (*LRUCache, error)`

Creates a new LRU cache instance.

**Parameters:**
- `capacity`: Maximum number of entries
- `walDirectory`: WAL directory path (empty = disabled)
- `forceSync`: Force fsync on every write
- `maxFileSize`: Maximum WAL segment size in bytes
- `maxSegments`: Maximum number of WAL segments

#### `Set(key string, value any, ttl time.Duration) error`

Sets a key-value pair with optional TTL.

**Returns:** Error if operation fails

#### `Get(key string) (any, bool)`

Retrieves a value by key.

**Returns:** Value and boolean indicating if key exists

#### `Delete(key string) error`

Deletes a key from the cache.

**Returns:** Error if operation fails

#### `Close() error`

Closes the cache and flushes WAL.

**Returns:** Error if close fails

## Performance

### Typical Performance (approximate)

- **Set without WAL**: ~500K-1M ops/sec
- **Set with WAL**: ~50K-100K ops/sec
- **Set with WAL + Force Sync**: ~5K-10K ops/sec
- **Get**: ~1M-2M ops/sec
- **Recovery** (1000 entries): ~10-50ms

*Performance varies based on hardware, value sizes, and workload patterns.*

## Thread Safety

The cache is fully thread-safe:
- Uses `sync.RWMutex` for concurrent access
- Safe for multiple goroutines reading and writing simultaneously
- WAL operations are serialized with mutex protection

## Error Handling

All operations return errors that should be checked:
- WAL write failures
- Serialization errors
- Recovery errors
- Invalid TTL formats

## Limitations

- Values are serialized using `gob` encoding (Go-specific)
- TTL expiration is checked on access (not proactively cleaned)
- WAL recovery replays all entries (no snapshot/compaction yet)
- Cache capacity is fixed at creation time

## Contributing

For issues, questions, or contributions, please open an issue on GitHub.

