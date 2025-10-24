package trackeroo

import (
	"context"
	"os"

	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	ctx         = context.Background()
)

func InitRedisClient() {
	redisURI := os.Getenv("REDIS_URI")
	if redisURI != "" {
		opt, err := redis.ParseURL(redisURI)
		if err != nil {
			Error("Failed to parse REDIS_URI: %v\n", err)
			return
		}
		redisClient = redis.NewClient(opt)

		if err := redisClient.Ping(ctx).Err(); err != nil {
			Error("Failed to connect to Redis: %v\n", err)
			redisClient = nil
		}
	}
	if redisClient != nil {
		Info("Saving checkpoint to %s", redisURI)
	}
}
