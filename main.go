package main

import (
	"fmt"
	"time"

	"github.com/nishanth-gowda/kv-store/cache"
)

func main() {
	// Create cache with WAL enabled
	c, err := cache.NewLRUCache(3, "./wal", false, 10*1024*1024, 10) // 10MB max file size, 10 max segments
	if err != nil {
		fmt.Printf("Error creating cache: %v\n", err)
		return
	}
	defer c.Close()

	// Set some values
	if err := c.Set("1", "one", 1*time.Second); err != nil {
		fmt.Printf("Error setting key: %v\n", err)
	}
	fmt.Println(c.Get("1"))

	if err := c.Set("2", "two", 2*time.Second); err != nil {
		fmt.Printf("Error setting key: %v\n", err)
	}
	if err := c.Set("3", "three", 3*time.Second); err != nil {
		fmt.Printf("Error setting key: %v\n", err)
	}

	time.Sleep(2 * time.Second)
	c.Get("1")
	c.Get("2")
	c.Get("3")
}
