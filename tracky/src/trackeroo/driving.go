package trackeroo

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"time"
)

const (
	ARRIVED      = "arrived"
	DRIVING      = "driving"
	STOPPED      = "stopped"
	SLOWING      = "slowing"
	ACCELERATING = "accelerating"
)

var consumptionMultipliers = map[string]float32{
	"valuable":          1.3, // heavier, armored vans/trucks
	"food":              1.1, // refrigerated transport adds load
	"private_transport": 1.0, // baseline
	"public_transport":  1.8, // buses have much higher consumption
}

type Coordinate struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lon"`
}

type RouteSegment struct {
	Coordinates []Coordinate
	SpeedKmh    float64
	ShouldStop  bool
}

type OSRMResponse struct {
	Code   string `json:"code"`
	Routes []struct {
		Geometry struct {
			Coordinates [][]float64 `json:"coordinates"`
		} `json:"geometry"`
		Distance float64 `json:"distance"`
		Duration float64 `json:"duration"`
		Legs     []struct {
			Steps []struct {
				Geometry struct {
					Coordinates [][]float64 `json:"coordinates"`
				} `json:"geometry"`
				Distance float64 `json:"distance"`
				Duration float64 `json:"duration"`
				Name     string  `json:"name"`
				Maneuver struct {
					Type     string    `json:"type"`
					Modifier string    `json:"modifier"`
					Location []float64 `json:"location"`
				} `json:"maneuver"`
			} `json:"steps"`
		} `json:"legs"`
	} `json:"routes"`
}

type DrivePosition struct {
	Coordinate
	Timestamp   time.Time
	Speed       float64 // km/h
	SpeedLimit  float64
	Status      string // "driving", "stopped", "slowing", "accelerating"
	Start       Coordinate
	End         Coordinate
	Consumption float64
	Distance    float64
}

type RoutingService struct {
	NominatimURL string
	OSRMURL      string
	httpClient   *http.Client
}

func NewRoutingService(osrmURL string) *RoutingService {
	return &RoutingService{
		OSRMURL:    osrmURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// GetRoute gets routing directions from point A to B using OSRM
func (rs *RoutingService) GetRoute(from, to Coordinate) ([]RouteSegment, error) {
	requestURL := fmt.Sprintf(
		"%s/route/v1/driving/%f,%f;%f,%f?geometries=geojson&overview=full&steps=true",
		rs.OSRMURL, from.Lng, from.Lat, to.Lng, to.Lat)

	resp, err := rs.httpClient.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get route: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var osrmResp OSRMResponse
	if err := json.Unmarshal(body, &osrmResp); err != nil {
		return nil, fmt.Errorf("failed to parse OSRM response: %w", err)
	}

	if osrmResp.Code != "Ok" || len(osrmResp.Routes) == 0 {
		return nil, fmt.Errorf("no routes found")
	}

	var segments []RouteSegment

	for _, leg := range osrmResp.Routes[0].Legs {
		for _, step := range leg.Steps {
			speed := 0.0
			if step.Duration > 0 {
				speed = (step.Distance / step.Duration) * 3.6
			}

			coords := make([]Coordinate, len(step.Geometry.Coordinates))
			for i, c := range step.Geometry.Coordinates {
				coords[i] = Coordinate{Lat: c[1], Lng: c[0]}
			}
			shouldStop := false
			switch step.Maneuver.Type {
			case "turn", "roundabout", "end_of_road":
				shouldStop = true
			}
			segments = append(segments, RouteSegment{
				Coordinates: coords,
				SpeedKmh:    speed,
				ShouldStop:  shouldStop,
			})
		}
	}

	return segments, nil
}

func getConsumption(speed float64, devType string) float64 {
	if speed == 0 {
		return 0.0
	}
	threshold := 80.0
	consumption := 5 + 60.7/speed
	if speed >= threshold {
		consumption += 5 * math.Log(speed/threshold)
	}
	return consumption * float64(consumptionMultipliers[devType])
}

// DrivingSimulator simulates realistic car driving
type DrivingSimulator struct {
	Route               []RouteSegment
	DefaultAverageSpeed float64 // km/h
	SpeedVariation      float64 // percentage (0.0-1.0)
	StopProbability     float64 // probability of stopping per segment (0.0-1.0)
	DevType             string
	StopDuration        struct {
		Min time.Duration
		Max time.Duration
	}
	UpdateInterval time.Duration
	IsPirate       bool
	rand           *rand.Rand
	Distance       float64
}

func NewDrivingSimulator(routingService *RoutingService, checkpoint *Checkpoint, avgSpeed float64, updateIntervalMs int, isPirate bool, devType string) (*DrivingSimulator, error) {
	route := make([]RouteSegment, 0)
	for i := 0; i < len(checkpoint.Poles)-1; i++ {
		start := checkpoint.Poles[i].Coordinate
		end := checkpoint.Poles[i+1].Coordinate
		r, err := routingService.GetRoute(start, end)
		if err != nil {
			return nil, err
		}
		checkpointIndex := -1

		for i, pos := range r {
			if checkpoint == nil {
				break
			}
			if haversineDistance(checkpoint.LastPosition, pos.Coordinates[0]) < 1 {
				checkpointIndex = i
				Info("Found checkpoint %+v at index %d", checkpoint, checkpointIndex)
				break
			}
		}
		if checkpointIndex >= 0 {
			r = r[checkpointIndex:]
		}
		route = append(route, r...)
	}

	return &DrivingSimulator{
		Route:               route,
		DefaultAverageSpeed: avgSpeed,
		SpeedVariation:      0.03, // 3% speed variation
		StopProbability:     0.05, // 5% chance of stopping per segment
		StopDuration: struct {
			Min time.Duration
			Max time.Duration
		}{
			Min: 2 * time.Second,
			Max: 5 * time.Second,
		},
		UpdateInterval: time.Duration(updateIntervalMs) * time.Millisecond,
		rand:           rand.New(rand.NewSource(time.Now().UnixNano())),
		IsPirate:       isPirate,
		DevType:        devType,
	}, nil
}

// SimulateDrive simulates driving along a route and sends positions through a channel
func (ds *DrivingSimulator) SimulateDrive() <-chan DrivePosition {
	positionChan := make(chan DrivePosition, 100)
	lastCoord := ds.Route[len(ds.Route)-1].Coordinates[len(ds.Route[len(ds.Route)-1].Coordinates)-1]
	lastSpeed := 0.0
	go func() {
		defer close(positionChan)

		if len(ds.Route) < 2 {
			return
		}

		currentTime := time.Now()
		stopCompensation := 1
		consumptionCompensation := 1.0
		speedLimit := 0.0
		for _, segment := range ds.Route {
			for i := 0; i < len(segment.Coordinates)-1; i++ {
				currentPos := segment.Coordinates[i]
				nextPos := segment.Coordinates[i+1]

				// Calculate distance between points
				distance := haversineDistance(currentPos, nextPos)

				// Check if we should stop
				if i == 0 && segment.ShouldStop {
					// Send stopped position
					positionChan <- DrivePosition{
						Coordinate:  currentPos,
						Timestamp:   currentTime,
						Speed:       0,
						SpeedLimit:  speedLimit,
						Status:      STOPPED,
						Start:       ds.Route[0].Coordinates[0],
						End:         lastCoord,
						Consumption: 0,
						Distance:    0,
					}

					// Random stop duration
					stopTime := ds.StopDuration.Min + time.Duration(
						ds.rand.Float64()*float64(ds.StopDuration.Max-ds.StopDuration.Min))
					time.Sleep(stopTime)
					currentTime = time.Now()
					stopCompensation = 3
					lastSpeed = 0
				}

				// Calculate current speed with variation
				baseSpeed := ds.DefaultAverageSpeed
				if segment.SpeedKmh > 0 {
					baseSpeed = segment.SpeedKmh
				}
				if stopCompensation > 1 {
					baseSpeed /= float64(stopCompensation)
					stopCompensation -= 1
				}
				speedLimit = baseSpeed
				speedVariation := 1.0 + (ds.rand.Float64()-0.5)*2*ds.SpeedVariation
				currentSpeed := baseSpeed * speedVariation
				if ds.IsPirate {
					currentSpeed *= 1.40
				}

				// Calculate time to travel this segment
				travelTimeHours := distance / currentSpeed
				segmentDuration := time.Duration(travelTimeHours * float64(time.Hour))

				// Interpolate positions along the segment
				steps := max(int(segmentDuration/ds.UpdateInterval), 1)

				status := DRIVING
				consumptionCompensation = 1
				speedDifference := math.Abs(currentSpeed - lastSpeed)
				tolerance := currentSpeed * 0.05 // 5% tolerance

				if speedDifference > tolerance {
					if lastSpeed < currentSpeed {
						status = ACCELERATING
						consumptionCompensation = 1.3
					} else {
						status = SLOWING
						consumptionCompensation = 0.5
					}
				}
				lastSpeed = currentSpeed

				for step := 0; step <= steps; step++ {
					progress := float64(step) / float64(steps)

					interpolatedPos := ds.interpolatePosition(currentPos, nextPos, progress)

					positionChan <- DrivePosition{
						Coordinate:  interpolatedPos,
						Timestamp:   currentTime,
						Speed:       currentSpeed,
						SpeedLimit:  speedLimit,
						Status:      status,
						Start:       ds.Route[0].Coordinates[0],
						End:         lastCoord,
						Consumption: getConsumption(currentSpeed, ds.DevType) * consumptionCompensation,
						Distance:    distance,
					}
					time.Sleep(ds.UpdateInterval)
					currentTime = time.Now()
				}
			}
		}

		// Send final position
		positionChan <- DrivePosition{
			Coordinate:  lastCoord,
			Timestamp:   currentTime,
			Speed:       0,
			SpeedLimit:  speedLimit,
			Status:      ARRIVED,
			Start:       ds.Route[0].Coordinates[0],
			End:         lastCoord,
			Consumption: 0,
			Distance:    0,
		}
	}()

	return positionChan
}

// haversineDistance calculates the great circle distance between two points
func haversineDistance(pos1, pos2 Coordinate) float64 {
	const R = 6371 // Earth's radius in kilometers

	lat1Rad := pos1.Lat * math.Pi / 180
	lat2Rad := pos2.Lat * math.Pi / 180
	deltaLat := (pos2.Lat - pos1.Lat) * math.Pi / 180
	deltaLng := (pos2.Lng - pos1.Lng) * math.Pi / 180

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*
			math.Sin(deltaLng/2)*math.Sin(deltaLng/2)

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}

// interpolatePosition interpolates between two positions
func (ds *DrivingSimulator) interpolatePosition(pos1, pos2 Coordinate, progress float64) Coordinate {
	return Coordinate{
		Lat: pos1.Lat + (pos2.Lat-pos1.Lat)*progress,
		Lng: pos1.Lng + (pos2.Lng-pos1.Lng)*progress,
	}
}

// Helper function to parse float from string
func parseFloat(s string) (float64, error) {
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err
}
