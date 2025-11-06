package cache

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"github.com/nishanth-gowda/kv-store/wal"
)

type CacheItem struct {
	value     any
	TTL       time.Duration
	element   *list.Element
	createdAt time.Time
}

type LRUCache struct {
	mu        sync.RWMutex
	entries   map[string]*CacheItem
	evictList *list.List
	capacity  int
	wal       *wal.WAL
}

// NewLRUCache creates a new LRU cache with optional WAL support
// If walDirectory is empty, WAL is disabled
func NewLRUCache(capacity int, walDirectory string, forceSync bool, maxFileSize int, maxSegments int) (*LRUCache, error) {
	cache := &LRUCache{
		entries:   make(map[string]*CacheItem),
		evictList: list.New(),
		capacity:  capacity,
	}

	// Initialize WAL if directory is provided
	if walDirectory != "" {
		walInstance, err := wal.NewWal(walDirectory, forceSync, maxFileSize, maxSegments)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize WAL: %w", err)
		}
		cache.wal = walInstance

		// Recover from WAL
		if err := cache.recoverFromWAL(); err != nil {
			return nil, fmt.Errorf("failed to recover from WAL: %w", err)
		}
	}

	return cache, nil
}

func (cache *LRUCache) Set(key string, value any, ttl time.Duration) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// Serialize value for WAL
	valueBytes, err := serializeValue(value)
	if err != nil {
		return fmt.Errorf("failed to serialize value: %w", err)
	}

	// Calculate expiration timestamp
	var expiresAtUnixNano int64
	if ttl > 0 {
		expiresAtUnixNano = time.Now().Add(ttl).UnixNano()
	}

	// Write to WAL before updating cache
	if cache.wal != nil {
		if err := cache.wal.Append(wal.EntryTypeSET, key, valueBytes, expiresAtUnixNano); err != nil {
			return fmt.Errorf("failed to write to WAL: %w", err)
		}
	}

	// update existing item if it exists and move it to the front of the evict list
	if entry, ok := cache.entries[key]; ok {
		entry.value = value
		entry.TTL = ttl
		entry.createdAt = time.Now()
		cache.evictList.MoveToFront(entry.element)
		return nil
	}

	if len(cache.entries) >= cache.capacity {
		cache.evictLRU()
	}

	// create new item and add to the cache
	entry := &CacheItem{
		value:     value,
		TTL:       ttl,
		createdAt: time.Now(),
	}

	// push new item to the front of the evict list
	element := cache.evictList.PushFront(key)
	// set the element pointer in the item
	entry.element = element

	// add new item to the cache
	cache.entries[key] = entry

	return nil
}

func (cache *LRUCache) evictLRU() {
	element := cache.evictList.Back()
	if element != nil {
		key := cache.evictList.Remove(element).(string)
		delete(cache.entries, key)
	}
}

func (cache *LRUCache) Get(key string) (any, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	entry, ok := cache.entries[key]
	if !ok {
		return nil, false
	}

	// Check TTL expiration
	if entry.TTL > 0 {
		expiresAt := entry.createdAt.Add(entry.TTL)
		if time.Now().After(expiresAt) {
			// Item has expired, remove it
			cache.evictList.Remove(entry.element)
			delete(cache.entries, key)
			return nil, false
		}
	}

	// Item is valid, move to front and return
	cache.evictList.MoveToFront(entry.element)
	return entry.value, true
}

// Delete removes a key from the cache and writes to WAL
func (cache *LRUCache) Delete(key string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	entry, ok := cache.entries[key]
	if !ok {
		return nil // Key doesn't exist, nothing to delete
	}

	// Write DELETE to WAL
	if cache.wal != nil {
		if err := cache.wal.Append(wal.EntryTypeDELETE, key, nil, 0); err != nil {
			return fmt.Errorf("failed to write DELETE to WAL: %w", err)
		}
	}

	// Remove from cache
	cache.evictList.Remove(entry.element)
	delete(cache.entries, key)

	return nil
}

// Close closes the WAL if it exists
func (cache *LRUCache) Close() error {
	if cache.wal != nil {
		return cache.wal.Close()
	}
	return nil
}

// serializeValue serializes a value to bytes using gob encoding
func serializeValue(value any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// deserializeValue deserializes bytes to a value using gob decoding
func deserializeValue(data []byte) (any, error) {
	var value any
	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// recoverFromWAL recovers the cache state from WAL entries
func (cache *LRUCache) recoverFromWAL() error {
	if cache.wal == nil {
		return nil
	}

	entries, err := cache.wal.ReadAll()
	if err != nil {
		return err
	}

	now := time.Now()

	// Replay entries in order
	for _, entry := range entries {
		switch entry.Type {
		case wal.EntryTypeSET:
			// Check if entry has expired
			if entry.ExpiresAtUnixNano > 0 {
				if now.UnixNano() >= entry.ExpiresAtUnixNano {
					// Entry has expired, skip it
					continue
				}
			}

			// Deserialize value
			value, err := deserializeValue(entry.Value)
			if err != nil {
				// Log error but continue with other entries
				fmt.Printf("Warning: failed to deserialize value for key %s: %v\n", entry.Key, err)
				continue
			}

			// Calculate TTL from expiration timestamp
			var ttl time.Duration
			var createdAt time.Time
			if entry.ExpiresAtUnixNano > 0 {
				expiresAt := time.Unix(0, entry.ExpiresAtUnixNano)
				ttl = expiresAt.Sub(now)
				createdAt = now
			}

			// Add to cache (without writing to WAL to avoid recursion)
			if len(cache.entries) >= cache.capacity {
				cache.evictLRU()
			}

			cacheItem := &CacheItem{
				value:     value,
				TTL:       ttl,
				createdAt: createdAt,
			}

			element := cache.evictList.PushFront(entry.Key)
			cacheItem.element = element
			cache.entries[entry.Key] = cacheItem

		case wal.EntryTypeDELETE:
			// Remove from cache if it exists
			if cacheEntry, exists := cache.entries[entry.Key]; exists {
				cache.evictList.Remove(cacheEntry.element)
				delete(cache.entries, entry.Key)
			}
		}
	}

	return nil
}
