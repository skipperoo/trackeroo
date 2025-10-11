package trackeroo

import (
	"encoding/json"
	"time"
)

type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

type StatVar[T Number] struct {
	Avg   float64 `json:"avg"`
	Min   T       `json:"min"`
	Max   T       `json:"max"`
	Count int     `json:"count"`
}

func NewStatVar[T Number]() StatVar[T] {
	return StatVar[T]{}
}

func (sv *StatVar[T]) Add(value T) {
	if sv.Count == 0 {
		sv.Avg = 0.0
		sv.Min = value
		sv.Max = value
	} else {
		sv.Avg += (float64(value) - sv.Avg) / float64(sv.Count+1)
		if value < sv.Min {
			sv.Min = value
		}
		if value > sv.Max {
			sv.Max = value
		}
	}
	sv.Count++
}

func (sv *StatVar[T]) Get() map[string]any {
	res := map[string]any{
		"avg":   sv.Avg,
		"min":   sv.Min,
		"max":   sv.Max,
		"count": sv.Count,
	}
	sv.Avg = 0.0
	sv.Min = 0
	sv.Max = 0
	sv.Count = 0
	return res
}

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
