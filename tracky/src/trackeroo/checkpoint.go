package trackeroo

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	ctx         = context.Background()
)

func InitCheckpoint() {
	redisURI := os.Getenv("REDIS_URI")
	if redisURI != "" {
		opt, err := redis.ParseURL(redisURI)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse REDIS_URI: %v\n", err)
			return
		}
		redisClient = redis.NewClient(opt)

		// Test connection
		if err := redisClient.Ping(ctx).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to Redis: %v\n", err)
			redisClient = nil
		}
	}
}

func ExistsCheckpoint(filename string) bool {
	if redisClient != nil {
		exists, err := redisClient.Exists(ctx, filename).Result()
		return err == nil && exists > 0
	}

	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func SaveCheckpoint(checkpoint *Checkpoint, filename string) error {
	if checkpoint == nil {
		return fmt.Errorf("cannot save nil checkpoint")
	}

	jsonData, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	if redisClient != nil {
		return redisClient.Set(ctx, filename, jsonData, 0).Err()
	}

	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintf(file, "%s\n", jsonData)
	return nil
}

func LoadCheckpoint(filename string) (*Checkpoint, error) {
	var jsonData []byte
	var err error

	if redisClient != nil {
		jsonData, err = redisClient.Get(ctx, filename).Bytes()
		if err != nil {
			return nil, err
		}
	} else {
		file, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		var checkpoint Checkpoint
		err = json.NewDecoder(file).Decode(&checkpoint)
		if err != nil {
			return nil, err
		}
		return &checkpoint, nil
	}

	var checkpoint Checkpoint
	err = json.Unmarshal(jsonData, &checkpoint)
	if err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func DeleteCheckpoint(filename string) error {
	if redisClient != nil {
		return redisClient.Del(ctx, filename).Err()
	}
	return os.Remove(filename)
}
