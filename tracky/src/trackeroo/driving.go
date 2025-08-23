package trackeroo

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

const (
	ARRIVED      = "arrived"
	DRIVING      = "driving"
	STOPPED      = "stopped"
	SLOWING      = "slowing"
	ACCELERATING = "accelerating"
)

type Coordinate struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lon"`
}

type NominatimResponse []struct {
	Lat         string `json:"lat"`
	Lon         string `json:"lon"`
	DisplayName string `json:"display_name"`
}

type OSRMResponse struct {
	Code   string `json:"code"`
	Routes []struct {
		Geometry struct {
			Coordinates [][]float64 `json:"coordinates"`
		} `json:"geometry"`
		Distance float64 `json:"distance"`
		Duration float64 `json:"duration"`
	} `json:"routes"`
}

type DrivePosition struct {
	Coordinate
	Timestamp time.Time
	Speed     float64 // km/h
	Status    string  // "driving", "stopped", "slowing", "accelerating"
}

type RoutingService struct {
	NominatimURL string
	OSRMURL      string
	httpClient   *http.Client
}

func NewRoutingService(nominatimURL, osrmURL string) *RoutingService {
	return &RoutingService{
		NominatimURL: nominatimURL,
		OSRMURL:      osrmURL,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (rs *RoutingService) Geocode(address string) (*Coordinate, error) {
	encodedAddress := url.QueryEscape(address)
	requestURL := fmt.Sprintf("%s/search?q=%s&format=json&limit=1", rs.NominatimURL, encodedAddress)

	resp, err := rs.httpClient.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("failed to geocode address: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nominatim API returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var nominatimResp NominatimResponse
	if err := json.Unmarshal(body, &nominatimResp); err != nil {
		return nil, fmt.Errorf("failed to parse nominatim response: %w", err)
	}

	if len(nominatimResp) == 0 {
		return nil, fmt.Errorf("no results found for address: %s", address)
	}

	// Parse coordinates
	lat, err := parseFloat(nominatimResp[0].Lat)
	if err != nil {
		return nil, fmt.Errorf("invalid latitude: %w", err)
	}

	lng, err := parseFloat(nominatimResp[0].Lon)
	if err != nil {
		return nil, fmt.Errorf("invalid longitude: %w", err)
	}

	return &Coordinate{Lat: lat, Lng: lng}, nil
}

// GetRoute gets routing directions from point A to B using OSRM
func (rs *RoutingService) GetRoute(from, to *Coordinate) ([]Coordinate, error) {
	requestURL := fmt.Sprintf("%s/route/v1/driving/%f,%f;%f,%f?geometries=geojson&overview=full",
		rs.OSRMURL, from.Lng, from.Lat, to.Lng, to.Lat)

	resp, err := rs.httpClient.Get(requestURL)
	if err != nil {
		return nil, fmt.Errorf("failed to get route: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OSRM API returned status %d", resp.StatusCode)
	}

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

	// Convert coordinates
	coordinates := osrmResp.Routes[0].Geometry.Coordinates
	route := make([]Coordinate, len(coordinates))

	for i, coord := range coordinates {
		route[i] = Coordinate{Lat: coord[1], Lng: coord[0]} // OSRM returns [lng, lat]
	}

	return route, nil
}

// DrivingSimulator simulates realistic car driving
type DrivingSimulator struct {
	Route           []Coordinate
	AverageSpeed    float64 // km/h
	SpeedVariation  float64 // percentage (0.0-1.0)
	StopProbability float64 // probability of stopping per segment (0.0-1.0)
	StopDuration    struct {
		Min time.Duration
		Max time.Duration
	}
	UpdateInterval time.Duration
	rand           *rand.Rand
}

func NewDrivingSimulator(routingService *RoutingService, waypoints []string, avgSpeed float64, updateIntervalMs int) (*DrivingSimulator, error) {
	route := make([]Coordinate, 0)
	for i := 0; i < len(waypoints)-1; i++ {
		start, err := routingService.Geocode(waypoints[i])
		if err != nil {
			return nil, fmt.Errorf("Error geocoding address %s: %v", waypoints[i], err)
		}
		end, err := routingService.Geocode(waypoints[i+1])
		if err != nil {
			return nil, fmt.Errorf("Error geocoding address %s: %v", waypoints[i], err)
		}
		r, err := routingService.GetRoute(start, end)
		if err != nil {
			return nil, err
		}
		route = append(route, r...)
	}

	return &DrivingSimulator{
		Route:           route,
		AverageSpeed:    avgSpeed,
		SpeedVariation:  0.2,  // 20% speed variation
		StopProbability: 0.10, // 5% chance of stopping per segment
		StopDuration: struct {
			Min time.Duration
			Max time.Duration
		}{
			Min: 5 * time.Second,
			Max: 20 * time.Second,
		},
		UpdateInterval: time.Duration(updateIntervalMs) * time.Millisecond,
		rand:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// SimulateDrive simulates driving along a route and sends positions through a channel
func (ds *DrivingSimulator) SimulateDrive() <-chan DrivePosition {
	positionChan := make(chan DrivePosition, 100)

	go func() {
		defer close(positionChan)

		if len(ds.Route) < 2 {
			return
		}

		currentTime := time.Now()
		stopCompensation := 1
		for i := 0; i < len(ds.Route)-1; i++ {
			currentPos := ds.Route[i]
			nextPos := ds.Route[i+1]

			// Calculate distance between points
			distance := ds.haversineDistance(currentPos, nextPos)

			// Check if we should stop
			if ds.rand.Float64() < ds.StopProbability {
				// Send stopped position
				positionChan <- DrivePosition{
					Coordinate: currentPos,
					Timestamp:  currentTime,
					Speed:      0,
					Status:     STOPPED,
				}

				// Random stop duration
				stopTime := ds.StopDuration.Min + time.Duration(
					ds.rand.Float64()*float64(ds.StopDuration.Max-ds.StopDuration.Min))
				time.Sleep(stopTime)
				currentTime = time.Now()
				stopCompensation = 3
			}

			// Calculate current speed with variation
			baseSpeed := ds.AverageSpeed
			if stopCompensation > 1 {
				baseSpeed /= float64(stopCompensation)
				stopCompensation -= 1
			}
			speedVariation := 1.0 + (ds.rand.Float64()-0.5)*2*ds.SpeedVariation
			currentSpeed := baseSpeed * speedVariation

			// Ensure reasonable speed limits
			if currentSpeed < 5 {
				currentSpeed = 5
			} else if currentSpeed > 120 {
				currentSpeed = 120
			}

			// Calculate time to travel this segment
			travelTimeHours := distance / currentSpeed
			segmentDuration := time.Duration(travelTimeHours * float64(time.Hour))

			// Interpolate positions along the segment
			steps := int(segmentDuration / ds.UpdateInterval)
			if steps < 1 {
				steps = 1
			}

			for step := 0; step <= steps; step++ {
				progress := float64(step) / float64(steps)

				interpolatedPos := ds.interpolatePosition(currentPos, nextPos, progress)

				status := DRIVING
				if step == 0 && i > 0 {
					status = ACCELERATING
				} else if step == steps {
					status = SLOWING
				}

				positionChan <- DrivePosition{
					Coordinate: interpolatedPos,
					Timestamp:  currentTime,
					Speed:      currentSpeed,
					Status:     status,
				}
				time.Sleep(ds.UpdateInterval)
				currentTime = time.Now()
			}
		}

		// Send final position
		positionChan <- DrivePosition{
			Coordinate: ds.Route[len(ds.Route)-1],
			Timestamp:  currentTime,
			Speed:      0,
			Status:     ARRIVED,
		}
	}()

	return positionChan
}

// haversineDistance calculates the great circle distance between two points
func (ds *DrivingSimulator) haversineDistance(pos1, pos2 Coordinate) float64 {
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
