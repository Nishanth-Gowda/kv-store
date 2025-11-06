package main

import (
	"log"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/nishanth-gowda/kv-store/cache"
)

func main() {

	c, err := cache.NewLRUCache(3, "./wal", false, 10*1024*1024, 10)
	if err != nil {
		log.Fatalf("Error creating cache: %v", err)
	}
	defer c.Close()

	// Create Echo instance
	e := echo.New()

	e.POST("/set", SetHandler(c))
	e.GET("/get", GetHandler(c))
	e.DELETE("/delete", DeleteHandler(c))
	e.Start(":8080")
}

// SetHandler returns a handler function for POST /set
func SetHandler(cache *cache.LRUCache) echo.HandlerFunc {
	return func(c echo.Context) error {
		key := c.QueryParam("key")
		value := c.QueryParam("value")
		ttl := c.QueryParam("ttl")

		if key == "" || value == "" {
			return c.String(http.StatusBadRequest, "key and value are required")
		}

		var ttlDuration time.Duration
		var err error
		if ttl != "" {
			ttlDuration, err = time.ParseDuration(ttl)
			if err != nil {
				return c.String(http.StatusBadRequest, "Invalid TTL format")
			}
		}

		if err := cache.Set(key, value, ttlDuration); err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.String(http.StatusOK, "OK")
	}
}

// GetHandler returns a handler function for GET /get
func GetHandler(cache *cache.LRUCache) echo.HandlerFunc {
	return func(c echo.Context) error {
		key := c.QueryParam("key")
		if key == "" {
			return c.String(http.StatusBadRequest, "key is required")
		}

		value, ok := cache.Get(key)
		if !ok {
			return c.String(http.StatusNotFound, "Key not found")
		}

		return c.String(http.StatusOK, value.(string))
	}
}

// DeleteHandler returns a handler function for DELETE /delete
func DeleteHandler(cache *cache.LRUCache) echo.HandlerFunc {
	return func(c echo.Context) error {
		key := c.QueryParam("key")
		if key == "" {
			return c.String(http.StatusBadRequest, "key is required")
		}

		if err := cache.Delete(key); err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.String(http.StatusOK, "OK")
	}
}
