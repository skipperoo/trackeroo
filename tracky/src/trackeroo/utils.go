package trackeroo

import (
	"encoding/json"
	"time"
)

func UnixTime() uint64 {
	return uint64(time.Now().Unix())
}

func Millisleep(millis int) {
	time.Sleep(time.Duration(millis) * time.Millisecond)
}

func StructToMap[T any](s T) (map[string]any, error) {
	// Marshal struct to JSON
	jsonData, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	// Unmarshal JSON to map
	var result map[string]any
	err = json.Unmarshal(jsonData, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}
