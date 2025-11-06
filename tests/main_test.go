package main_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nishanth-gowda/kv-store/cache"
)

// BenchmarkSetWithoutWAL benchmarks Set operations without WAL
func BenchmarkSetWithoutWAL(b *testing.B) {
	c, err := cache.NewLRUCache(1000, "", false, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := c.Set(key, value, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkSetWithWAL benchmarks Set operations with WAL enabled
func BenchmarkSetWithWAL(b *testing.B) {
	walDir := "./bench_wal"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	c, err := cache.NewLRUCache(1000, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := c.Set(key, value, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkSetWithWALForceSync benchmarks Set operations with WAL and force sync
func BenchmarkSetWithWALForceSync(b *testing.B) {
	walDir := "./bench_wal_sync"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	c, err := cache.NewLRUCache(1000, walDir, true, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := c.Set(key, value, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkGet benchmarks Get operations
func BenchmarkGet(b *testing.B) {
	c, err := cache.NewLRUCache(1000, "", false, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := c.Set(key, value, 0); err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000)
			_, _ = c.Get(key)
			i++
		}
	})
}

// BenchmarkGetWithWAL benchmarks Get operations with WAL enabled
func BenchmarkGetWithWAL(b *testing.B) {
	walDir := "./bench_wal_get"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	c, err := cache.NewLRUCache(1000, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	// Pre-populate cache
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := c.Set(key, value, 0); err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000)
			_, _ = c.Get(key)
			i++
		}
	})
}

// BenchmarkDelete benchmarks Delete operations
func BenchmarkDelete(b *testing.B) {
	c, err := cache.NewLRUCache(10000, "", false, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			// Set first
			if err := c.Set(key, value, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			// Then delete
			if err := c.Delete(key); err != nil {
				b.Fatalf("Delete failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkDeleteWithWAL benchmarks Delete operations with WAL
func BenchmarkDeleteWithWAL(b *testing.B) {
	walDir := "./bench_wal_delete"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	c, err := cache.NewLRUCache(10000, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			// Set first
			if err := c.Set(key, value, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			// Then delete
			if err := c.Delete(key); err != nil {
				b.Fatalf("Delete failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkSetLargeValues benchmarks Set operations with large values
func BenchmarkSetLargeValues(b *testing.B) {
	c, err := cache.NewLRUCache(100, "", false, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	// Create a large value (1KB)
	largeValue := make([]byte, 1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			if err := c.Set(key, largeValue, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkSetLargeValuesWithWAL benchmarks Set operations with large values and WAL
func BenchmarkSetLargeValuesWithWAL(b *testing.B) {
	walDir := "./bench_wal_large"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	c, err := cache.NewLRUCache(100, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	// Create a large value (1KB)
	largeValue := make([]byte, 1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			if err := c.Set(key, largeValue, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkSetWithTTL benchmarks Set operations with TTL
func BenchmarkSetWithTTL(b *testing.B) {
	c, err := cache.NewLRUCache(1000, "", false, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			ttl := time.Duration(i%100) * time.Second
			if err := c.Set(key, value, ttl); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkSetWithTTLAndWAL benchmarks Set operations with TTL and WAL
func BenchmarkSetWithTTLAndWAL(b *testing.B) {
	walDir := "./bench_wal_ttl"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	c, err := cache.NewLRUCache(1000, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			ttl := time.Duration(i%100) * time.Second
			if err := c.Set(key, value, ttl); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkRecovery benchmarks recovery from WAL
func BenchmarkRecovery(b *testing.B) {
	walDir := "./bench_wal_recovery"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	// Create cache and populate it
	c, err := cache.NewLRUCache(1000, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}

	// Populate with data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := c.Set(key, value, 0); err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}

	// Close cache
	if err := c.Close(); err != nil {
		b.Fatalf("Close failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Recover from WAL
		recoveredCache, err := cache.NewLRUCache(1000, walDir, false, 10*1024*1024, 10)
		if err != nil {
			b.Fatalf("Failed to recover cache: %v", err)
		}

		// Verify recovery by checking a known key exists
		_, exists := recoveredCache.Get("key-0")
		if !exists {
			b.Fatalf("Recovery failed: key-0 not found")
		}

		if err := recoveredCache.Close(); err != nil {
			b.Fatalf("Close failed: %v", err)
		}
	}
}

// BenchmarkMixedWorkload benchmarks a mixed workload of Set, Get, and Delete
func BenchmarkMixedWorkload(b *testing.B) {
	c, err := cache.NewLRUCache(1000, "", false, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			op := i % 3

			switch op {
			case 0:
				// Set operation
				_ = c.Set(key, value, 0)
			case 1:
				// Get operation
				_, _ = c.Get(key)
			case 2:
				// Delete operation
				_ = c.Delete(key)
			}
			i++
		}
	})
}

// BenchmarkMixedWorkloadWithWAL benchmarks a mixed workload with WAL
func BenchmarkMixedWorkloadWithWAL(b *testing.B) {
	walDir := "./bench_wal_mixed"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	c, err := cache.NewLRUCache(1000, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			op := i % 3

			switch op {
			case 0:
				// Set operation
				_ = c.Set(key, value, 0)
			case 1:
				// Get operation
				_, _ = c.Get(key)
			case 2:
				// Delete operation
				_ = c.Delete(key)
			}
			i++
		}
	})
}

// BenchmarkLRUEviction benchmarks LRU eviction behavior
func BenchmarkLRUEviction(b *testing.B) {
	// Small capacity to force evictions
	c, err := cache.NewLRUCache(100, "", false, 0, 0)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := c.Set(key, value, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkLRUEvictionWithWAL benchmarks LRU eviction with WAL
func BenchmarkLRUEvictionWithWAL(b *testing.B) {
	walDir := "./bench_wal_eviction"
	os.RemoveAll(walDir)
	defer os.RemoveAll(walDir)

	// Small capacity to force evictions
	c, err := cache.NewLRUCache(100, walDir, false, 10*1024*1024, 10)
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if err := c.Set(key, value, 0); err != nil {
				b.Fatalf("Set failed: %v", err)
			}
			i++
		}
	})
}
