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
	rand.Seed(time.Now().UnixNano())
	creds, _ := trackeroo.GetCredentials()
	lastRouteFile := "/data/last_route.json"
	deviceType := creds.DeviceType
	overpassClient := trackeroo.NewOverpassClient(os.Getenv("OVERPASS_URL"))
	streetProvider := trackeroo.NewCachedStreetProvider(os.Getenv("OVERPASS_URL"), overpassClient)
	routingService := trackeroo.NewRoutingService(
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
	isRegional := os.Getenv("REGIONAL") == "true"
	isUrban := os.Getenv("URBAN") == "true"
	if isUrban {
		isRegional = true
	}
	var cities []string
	if isRegional {
		regions, err := overpassClient.GetRegions()
		if err != nil || len(regions) == 0 {
			trackeroo.Error("Error getting regions, falling back to Toscana: %v", err)
			regions = []string{"Toscana"}
		}
		cities, err = overpassClient.GetCities(regions[rand.Intn(len(regions))])
		if err != nil || len(cities) == 0 {
			trackeroo.Error("Error getting cities, falling back to default cities: %v", err)
			cities = []string{"Firenze", "Pisa", "Siena", "Lucca", "Vinci", "Prato", "Montecatini", "Arezzo", "Grosseto", "Massa"}
		}

		if isUrban {
			city := cities[rand.Intn(len(cities))]
			cities = []string{city}
		}
	} else {
		regions, err := overpassClient.GetRegions()
		if err != nil || len(regions) == 0 {
			trackeroo.Error("Error getting regions, falling back to Toscana: %v", err)
			regions = []string{"Toscana"}
		}
		cities = make([]string, 0)
		for _, region := range regions {
			c, _ := overpassClient.GetCities(region)
			cities = append(cities, c...)
		}
	}
	trackeroo.Info("Is pirate: %t", isPirate)
	lastStatus := ""
	lastEnd := trackeroo.Street{}
	normalRun := true
	deltaDistance := 0.0
	speedVar := trackeroo.NewStatVar[float64]()
	consumptionVar := trackeroo.NewStatVar[float64]()
	for {
		var route []trackeroo.Street
		var err error
		if trackeroo.ExistsPrevRoute(lastRouteFile) {
			route, err = trackeroo.LoadRoute(lastRouteFile)
			if err != nil {
				trackeroo.Error("Error loading route %v, discarding file", err)
				trackeroo.DeleteRoute(lastRouteFile)
				continue
			}
			lastEnd = route[1]
		} else {
			trackeroo.Info("Getting route from %+v - REGIONAL: %t - URBAN: %t", lastEnd, isRegional, isUrban)
			var cityErr string
			route, err, cityErr = trackeroo.GetRoute(streetProvider, cities, lastEnd, isUrban)
			if err != nil {
				trackeroo.Error("Error getting route from %s %v", cityErr, err)
				trackeroo.Warning("Removing %s", cityErr)
				for i, city := range cities {
					if city == cityErr {
						cities = append(cities[:i], cities[i+1:]...)
					}
				}
				continue
			}
		}
		trackeroo.Info("Route: %+v -> %+v", route[0], route[1])
		err = trackeroo.SaveRoute(route, lastRouteFile)
		if err != nil {
			trackeroo.Error("Error saving route %v", err)
		} else {
			trackeroo.Info("Route saved to %s", lastRouteFile)
		}

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
		trackeroo.DeleteRoute(lastRouteFile)
		time.Sleep(time.Second * 120)
		if !normalRun {
			/* Wait some more */
			time.Sleep(time.Second * 120)
		}
	}
}
