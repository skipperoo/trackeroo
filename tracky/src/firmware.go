package main

import (
	"math/rand"
	"os"
	"strconv"
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
	TS       int64                `json:"ts"`
	Speed    float64              `json:"speed"`
	Position trackeroo.Coordinate `json:"position"`
	Status   string               `json:"status"`
	Sensors  any                  `json:"sensors,omitempty"`
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
		Vibration:      rand.Float64() * 300,
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

func main() {

	trackeroo.InitLogger(trackeroo.INFO, "")
	trackeroo.InitQueue("data")
	taskManager := trackeroo.NewTaskManager()
	go taskManager.Start()
	creds, _ := trackeroo.GetCredentials()
	deviceType := creds.DeviceType
	routingService := trackeroo.NewRoutingService(
		os.Getenv("GEOCODING_SERVICE_URL"),
		os.Getenv("ROUTING_SERVICE_URL"),
	)
	pubPeriod := time.Second * 2
	if period := os.Getenv("PUBLISH_PERIOD"); period != "" {
		p, err := strconv.Atoi(period)
		if err == nil {
			pubPeriod = time.Duration(p) * time.Second
		}
	}
	lastPublish := time.Now()

	lastStatus := ""
	lastEnd := ""
	normalRun := true
	for {
		route := trackeroo.GetRoute(lastEnd)

		lastEnd = route[len(route)-1]
		drivingSimulator, err := trackeroo.NewDrivingSimulator(routingService, route, 60, 100)
		if err != nil {
			trackeroo.Error("Error initializing driving simulator %v", err)
			continue
		}

		positionChan := drivingSimulator.SimulateDrive()
		if rand.Float64() < 0.05 || os.Getenv("NORMAL_RUN") == "false" {
			normalRun = false
		}
		for position := range positionChan {
			if time.Since(lastPublish) > pubPeriod || lastStatus != position.Status {
				lastStatus = position.Status
				payload := Payload{
					TS:    position.Timestamp.Unix(),
					Speed: position.Speed,
					Position: trackeroo.Coordinate{
						Lat: position.Lat,
						Lng: position.Lng,
					},
					Status: position.Status,
				}

				switch deviceType {
				case trackeroo.VALUABLES:
					vsensors := normalValuablesData(position)
					if !normalRun {
						vsensors = robberyValuablesData()
					}
					payload.Sensors = vsensors
				case trackeroo.FOOD:
					fsensors := normalFoodData(position)
					if !normalRun {
						fsensors = alteredFoodData(position)
					}
					payload.Sensors = fsensors
				}
				payloadMap, err := trackeroo.StructToMap(payload)
				if err != nil {
					trackeroo.Error("Error converting payload to map %v", err)
					continue
				}
				trackeroo.EnqueueData("data", "d", payloadMap)
				lastPublish = time.Now()
			}
			trackeroo.Millisleep(100)
		}
		/* Routing terminated, waiting before next route */
		time.Sleep(time.Second * 120)
	}
}
