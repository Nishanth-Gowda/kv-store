package main

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

type CacheItem struct {
	value   any
	TTL     time.Duration
	element *list.Element
}

type LRUCache struct {
	mu        sync.RWMutex
	entries   map[string]*CacheItem
	evictList *list.List
	capacity  int
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		entries:   make(map[string]*CacheItem),
		evictList: list.New(),
		capacity:  capacity,
	}
}

func (cache *LRUCache) Set(key string, value any, ttl time.Duration) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	// update existing item if it exists and move it to the front of the evict list
	if entry, ok := cache.entries[key]; ok {
		entry.value = value
		entry.TTL = ttl
		cache.evictList.MoveToFront(entry.element)
		return
	}

	if len(cache.entries) >= cache.capacity {
		cache.evictLRU()
	}

	// create new item and add to the cache
	entry := &CacheItem{
		value: value,
		TTL:   ttl,
	}

	// push new item to the front of the evict list
	element := cache.evictList.PushFront(key)
	// set the element pointer in the item
	entry.element = element

	// add new item to the cache
	cache.entries[key] = entry

}

func (cache *LRUCache) evictLRU() {
	element := cache.evictList.Back()
	if element != nil {
		key := cache.evictList.Remove(element).(string)
		delete(cache.entries, key)
	}
}

func (cache *LRUCache) Get(key string) (any, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	entry, ok := cache.entries[key]
	if !ok {
		return nil, false
	}

	if entry.TTL > 0 && time.Now().After(time.Now().Add(entry.TTL)) {
		return nil, false
	}

	cache.evictList.MoveToFront(entry.element)
	return entry.value, true
}

func main() {
	cache := NewLRUCache(3)
	cache.Set("1", "one", 1*time.Second)
	fmt.Println(cache.Get("1"))
	cache.Set("2", "two", 2*time.Second)
	cache.Set("3", "three", 3*time.Second)

	time.Sleep(2 * time.Second)
	cache.Get("1")
	cache.Get("2")
	cache.Get("3")
}
