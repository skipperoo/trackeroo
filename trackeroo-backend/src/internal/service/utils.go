// Package service provides access to DB, cache, login validation and redis.
package service

import (
	"os"
)

func GetenvOrDefault(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}
