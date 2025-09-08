// Package service provides access to DB, cache, login validation and redis.
package service

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"trackeroo-backend/internal/logger"
	"trackeroo-backend/internal/model"

	"github.com/redis/go-redis/v9"
)

var (
	redisClient   *redis.Client = nil
	fallbackCache *SimpleCache  = nil
	useFallback   bool          = false
)

func InitConnectivityCache() {
	ctx := context.Background()
	if redisClient == nil {
		options := redis.Options{
			Addr: GetenvOrDefault("REDIS_URI", "redis:6379"),
		}
		redisClient = redis.NewClient(&options)
		_, err := redisClient.Ping(ctx).Result()
		if err != nil {
			logger.Warning("Could not connect to Redis: %v", err)
			logger.Warning("Falling back to SimpleCache")
			useFallback = true
		} else {
			logger.Info("Connected to Redis!")
		}
	}
	if fallbackCache == nil {
		fallbackCache = NewSimpleCache()
	}
}

func DeinitConnectivityCache() {
	if !useFallback {
		redisClient.Close()
	}
}

func getRedisClient() (*redis.Client, *SimpleCache) {
	if useFallback {
		return nil, fallbackCache
	}

	// Always returning the fallbackCache so that it can be used
	// if something goes wrong with redis
	return redisClient, fallbackCache
}

func mapToStruct(m map[string]string) (model.DeviceStatus, error) {
	connected, err := strconv.ParseBool(m["connected"])
	if err != nil {
		return model.DeviceStatus{}, err
	}

	lastConn, err := strconv.ParseInt(m["last_connection"], 10, 64)
	if err != nil {
		return model.DeviceStatus{}, err
	}

	lastDisconn, err := strconv.ParseInt(m["last_disconnection"], 10, 64)
	if err != nil {
		return model.DeviceStatus{}, err
	}

	return model.DeviceStatus{
		Connected:         connected,
		LastConnection:    time.Unix(lastConn, 0),
		LastDisconnection: time.Unix(lastDisconn, 0),
		LastIP:            m["last_ip"],
	}, nil
}

func structToMap(status model.DeviceStatus) map[string]string {
	return map[string]string{
		"connected":          strconv.FormatBool(status.Connected),
		"last_connection":    strconv.FormatInt(status.LastConnection.Unix(), 10),
		"last_disconnection": strconv.FormatInt(status.LastDisconnection.Unix(), 10),
		"last_ip":            status.LastIP,
	}
}

func getLastRedisKey(ctx context.Context, rdb *redis.Client, devID, key string) (string, error) {
	data, err := rdb.HGet(ctx, devID, key).Result()
	if err != nil && err != redis.Nil {
		logger.Warning("Cannot get %s %s: %v", devID, key, err)
	} else {
		if err != nil {
			return data, err
		}
	}
	return data, err
}

func SetDeviceStatus(ctx context.Context, devID, ip string, timestamp time.Time, connected bool) {
	rdb, fbc := getRedisClient()
	status := model.DeviceStatus{
		Connected: connected,
		LastIP:    ip,
	}
	if connected {
		status.LastConnection = timestamp
		status.LastDisconnection = time.Unix(0, 0)
	} else {
		status.LastConnection = time.Unix(0, 0)
		status.LastDisconnection = timestamp
	}
	if rdb != nil {
		var (
			data string
			err  error
			t    time.Time
		)
		if connected {
			data, err = getLastRedisKey(ctx, rdb, devID, "last_disconnection")
		} else {
			data, err = getLastRedisKey(ctx, rdb, devID, "last_connection")
		}
		if err != nil {
			i, err := strconv.ParseInt(data, 10, 64)
			if err == nil {
				t = time.Unix(i, 0)
				if connected {
					status.LastDisconnection = t
				} else {
					status.LastConnection = t
				}
			}
		}
		rdb.HSet(ctx, devID, structToMap(status))
	} else {
		data := fbc.Get(devID)
		s, ok := data.(model.DeviceStatus)
		if ok {
			if connected {
				status.LastDisconnection = s.LastDisconnection
			} else {
				status.LastConnection = s.LastConnection
			}
		}
		fbc.Set(devID, status)
	}
}

func GetDeviceStatus(ctx context.Context, devID string) (model.DeviceStatus, error) {
	rdb, fbc := getRedisClient()
	var (
		data   any
		status model.DeviceStatus
		err    error
		ok     bool
	)
	if rdb != nil {
		data, err = rdb.HGetAll(ctx, devID).Result()
		if err != nil {
			return model.DeviceStatus{}, err
		}
		d, _ := data.(map[string]string)
		status, err = mapToStruct(d)
	} else {
		data := fbc.Get(devID)
		status, ok = data.(model.DeviceStatus)
		if !ok {
			err = fmt.Errorf("cannot find device %s", devID)
			status = model.DeviceStatus{}
		}
	}
	return status, err
}

func GetDevicesStatus(ctx context.Context, devIDs []string) (map[string]model.DeviceStatus, error) {
	rdb, fbc := getRedisClient()
	result := make(map[string]model.DeviceStatus)
	var firstErr error

	if rdb != nil {
		pipe := rdb.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, len(devIDs))
		for i, id := range devIDs {
			cmds[i] = pipe.HGetAll(ctx, id)
		}
		_, err := pipe.Exec(ctx)
		if err != nil {
			firstErr = err
		}

		for i, cmd := range cmds {
			d, err := cmd.Result()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			if len(d) == 0 {
				result[devIDs[i]] = model.DeviceStatus{}
				continue
			}
			status, err := mapToStruct(d)
			if err != nil && firstErr == nil {
				firstErr = err
			}
			result[devIDs[i]] = status
		}
	} else {
		for _, id := range devIDs {
			data := fbc.Get(id)
			if data != nil {
				result[id] = model.DeviceStatus{}
				continue
			}
			status, ok := data.(model.DeviceStatus)
			if !ok {
				result[id] = model.DeviceStatus{}
				continue
			}
			result[id] = status
		}
	}

	return result, firstErr
}
