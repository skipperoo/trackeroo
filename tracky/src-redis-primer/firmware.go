package main

import (
	"math/rand"
	"os"
	"sync"
	"time"
	"tracky/trackeroo"
)

type ValuableSensors struct {
	Alarm          bool    `json:"alarm"`
	Vibration      float64 `json:"vibration"`
	RearHatchOpen  bool    `json:"rear_hatch_open"`
	FrontHatchOpen bool    `json:"front_hatch_open"`
	Collision      bool    `json:"collision"`
}

type FoodSensors struct {
	RearHatchOpen  bool    `json:"rear_hatch_open"`
	FrontHatchOpen bool    `json:"front_hatch_open"`
	Temperature    float64 `json:"temperature"`
	Humidity       float64 `json:"humidity"`
	Pressure       float64 `json:"pressure"`
}

type Payload struct {
	TS                 int64                `json:"ts"`
	Speed              float64              `json:"speed"`
	SpeedLimit         float64              `json:"speed_limit"`
	SpeedStats         map[string]any       `json:"speed_stats"`
	Position           trackeroo.Coordinate `json:"position"`
	DeviceName         string               `json:"device_name"`
	DeviceType         string               `json:"device_type"`
	DeviceID           string               `json:"device_id"`
	Status             string               `json:"status"`
	Sensors            any                  `json:"sensors,omitempty"`
	Start              trackeroo.Coordinate `json:"start"`
	End                trackeroo.Coordinate `json:"end"`
	InstantConsumption float64              `json:"instant_consumption"`
	ConsumptionStats   map[string]any       `json:"consumption_stats"`
	DeltaDistance      float64              `json:"delta_distance"`
}

func normalValuablesData(position trackeroo.DrivePosition) ValuableSensors {
	return ValuableSensors{
		Alarm:          false,
		Vibration:      rand.Float64() * 100,
		RearHatchOpen:  position.Status == trackeroo.ARRIVED,
		FrontHatchOpen: position.Status == trackeroo.ARRIVED,
		Collision:      false,
	}
}

func robberyValuablesData() ValuableSensors {
	return ValuableSensors{
		Alarm:          true,
		Vibration:      100 + rand.Float64()*300,
		RearHatchOpen:  true,
		FrontHatchOpen: rand.Float32() < 0.5,
		Collision:      true,
	}
}

func normalFoodData(position trackeroo.DrivePosition) FoodSensors {
	minTemperature := 2.0
	minHumidity := 60.0
	minPressure := 1013.25 // Millibars
	return FoodSensors{
		Temperature:    minTemperature + rand.Float64()*2,
		Humidity:       minHumidity + rand.Float64()*10,
		Pressure:       minPressure + rand.Float64()*10,
		RearHatchOpen:  position.Status == trackeroo.ARRIVED,
		FrontHatchOpen: position.Status == trackeroo.ARRIVED,
	}
}

func alteredFoodData(position trackeroo.DrivePosition) FoodSensors {
	minTemperature := 4.0
	minHumidity := 70.0
	minPressure := 1013.25 // Millibars
	return FoodSensors{
		Temperature:    minTemperature + rand.Float64()*7,
		Humidity:       minHumidity + rand.Float64()*30,
		Pressure:       minPressure + rand.Float64()*100,
		RearHatchOpen:  rand.Float64() < 0.05,
		FrontHatchOpen: position.Status == trackeroo.ARRIVED,
	}
}

func Init() {
	debugLevel := os.Getenv("DEBUG_LEVEL")
	level := trackeroo.INFO
	switch debugLevel {
	case "DEBUG":
		level = trackeroo.DEBUG
	case "INFO":
		level = trackeroo.INFO
	case "WARN":
	case "WARNING":
		level = trackeroo.WARNING
	case "ERROR":
		level = trackeroo.ERROR
	default:
		level = trackeroo.INFO
	}
	trackeroo.InitLogger(level, "")
}

func Terminate() {
}

func Loop() {
	rand.Seed(time.Now().UnixNano())
	trackeroo.InitRedisClient()
	overpassClient := trackeroo.NewOverpassClient(os.Getenv("OVERPASS_URL"))

	regions, err := overpassClient.GetRegions()
	if err != nil {
		trackeroo.Error("Cannot retrieve regions: %+v", err)
	}
	cities := make([]string, 0)
	for _, region := range regions {
		c, _ := overpassClient.GetCities(region)
		cities = append(cities, c...)
	}
	wg := sync.WaitGroup{}
	i := 0
	for i = 0; i < len(cities); i += 4 {
		tmp := cities[i : i+4]
		for j := range 4 {
			wg.Add(1)
			go func() {
				overpassClient.GetStreets(tmp[j], 100, 2000)
				wg.Done()
			}()
		}
		wg.Wait()
	}
	tmp := cities[i-4:]
	if len(tmp) > 0 {
		for j := range len(cities) {
			wg.Add(1)
			go func() {
				overpassClient.GetStreets(tmp[j], 100, 2000)
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
