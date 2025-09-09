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

var taskManager *trackeroo.TaskManager

func Init() {
	trackeroo.InitLogger(trackeroo.INFO, "")
	trackeroo.InitQueue("data")
	taskManager = trackeroo.NewTaskManager()
	go taskManager.Start()
}

func Terminate() {
	taskManager.Shutdown(0)
}

func Loop() {
	creds, _ := trackeroo.GetCredentials()
	deviceType := creds.DeviceType
	routingService := trackeroo.NewRoutingService(
		os.Getenv("GEOCODING_SERVICE_URL"),
		os.Getenv("ROUTING_SERVICE_URL"),
	)
	pubPeriod := time.Millisecond * 2000
	if period := os.Getenv("PUBLISH_PERIOD"); period != "" {
		p, err := strconv.Atoi(period)
		if err == nil {
			pubPeriod = time.Duration(p) * time.Millisecond
		}
	}
	lastPublish := time.Now()
	isPirate := os.Getenv("PIRATE") == "true" || os.Getenv("PIRATE") == "1"
	trackeroo.Info("Is pirate: %t", isPirate)
	lastStatus := ""
	lastEnd := ""
	normalRun := true
	deltaDistance := 0.0
	speedVar := trackeroo.NewStatVar[float64]()
	consumptionVar := trackeroo.NewStatVar[float64]()
	for {
		trackeroo.Info("Getting route from %s", lastEnd)
		route := trackeroo.GetRoute(lastEnd)
		trackeroo.Info("Route: %+v", route)
		drivingSimulator, err := trackeroo.NewDrivingSimulator(routingService, route, 60, 100, isPirate, creds.DeviceType)
		if err != nil {
			trackeroo.Error("Error initializing driving simulator %v", err)
			continue
		}
		lastEnd = route[len(route)-1]

		positionChan := drivingSimulator.SimulateDrive()
		if rand.Float64() < 0.05 || os.Getenv("NORMAL_RUN") == "false" {
			normalRun = false
		}
		for position := range positionChan {
			deltaDistance += position.Distance
			speedVar.Add(position.Speed)
			consumptionVar.Add(position.Consumption)
			if time.Since(lastPublish) > pubPeriod || lastStatus != position.Status {
				lastStatus = position.Status
				payload := Payload{
					TS:         position.Timestamp.Unix(),
					Speed:      position.Speed,
					SpeedStats: speedVar.Get(),
					SpeedLimit: position.SpeedLimit,
					DeviceName: creds.Name,
					DeviceType: creds.DeviceType,
					Position: trackeroo.Coordinate{
						Lat: position.Lat,
						Lng: position.Lng,
					},
					Status:             position.Status,
					Start:              position.Start,
					End:                position.End,
					InstantConsumption: position.Consumption,
					ConsumptionStats:   consumptionVar.Get(),
					DeltaDistance:      deltaDistance,
				}
				deltaDistance = 0

				switch deviceType {
				case trackeroo.VALUABLES:
					vsensors := normalValuablesData(position)
					/* For the sake of simplicity the robbery happens when the vehicle arrives */
					if !normalRun && position.Status == trackeroo.ARRIVED {
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
		if !normalRun {
			/* Wait some more */
			time.Sleep(time.Second * 120)
		}
	}
}
