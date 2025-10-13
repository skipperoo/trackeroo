package trackeroo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

type OverpassElement struct {
	Type     string            `json:"type"`
	ID       int64             `json:"id"`
	Tags     map[string]string `json:"tags"`
	Geometry []Coordinate      `json:"geometry"`
}

type OverpassResponse struct {
	Elements []OverpassElement `json:"elements"`
}

type OverpassClient struct {
	BaseURL string
	Client  *http.Client
}

type Location struct {
	Coordinate Coordinate
	StreetName string
	City       string
}

func streetToLocation(city string, street OverpassElement) Location {
	return Location{
		Coordinate: street.Geometry[rand.Intn(len(street.Geometry))],
		StreetName: street.Tags["name"],
		City:       city,
	}
}

func NewOverpassClient(baseURL string) *OverpassClient {
	return &OverpassClient{
		BaseURL: baseURL,
		Client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *OverpassClient) GetStreets(city string, limit int) ([]OverpassElement, error) {
	if limit == 0 {
		limit = 100
	}
	radiusMeters := 2000 // 2km default

	query := fmt.Sprintf(`
[out:json][timeout:25];
area["ISO3166-1"="IT"][admin_level=2]->.italy;
node["name"="%s"]["place"~"city|town"](area.italy)->.citynode;
(
  way(around.citynode:%d)["highway"~"primary|secondary|tertiary|residential|unclassified"]["name"]["highway"!~"motorway|trunk|motorway_link|trunk_link"](area.italy);
);
out tags geom %d;
`, city, radiusMeters, limit)

	resp, err := c.Client.Post(c.BaseURL, "application/x-www-form-urlencoded", bytes.NewBufferString(query))
	if err != nil {
		return nil, fmt.Errorf("failed to query Overpass API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Overpass API returned status %d: %s", resp.StatusCode, string(body))
	}

	var overpassResp OverpassResponse
	if err := json.NewDecoder(resp.Body).Decode(&overpassResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(overpassResp.Elements) == 0 {
		return nil, fmt.Errorf("no streets found for city: %s", city)
	}

	return overpassResp.Elements, nil
}

func (c *OverpassClient) GetRandomStreet(city string) (Location, error) {
	streets, err := c.GetStreets(city, 100)
	if err != nil {
		return Location{}, err
	}
	randStreet := streets[rand.Intn(len(streets))]
	return streetToLocation(city, randStreet), nil
}

type CachedStreetProvider struct {
	client *OverpassClient
	cache  map[string][]OverpassElement
}

func NewCachedStreetProvider(baseURL string) *CachedStreetProvider {
	return &CachedStreetProvider{
		client: NewOverpassClient(baseURL),
		cache:  make(map[string][]OverpassElement),
	}
}

func (p *CachedStreetProvider) GetRandomStreet(city string) (Location, error) {
	if streets, ok := p.cache[city]; ok && len(streets) > 0 {
		return streetToLocation(city, streets[rand.Intn(len(streets))]), nil
	}

	streets, err := p.client.GetStreets(city, 100)
	if err != nil {
		return Location{}, err
	}

	p.cache[city] = streets

	return streetToLocation(city, streets[rand.Intn(len(streets))]), nil
}

// GetRoute generates a route with random streets
// If using cached provider, pass cities to select from
func GetRoute(provider *CachedStreetProvider, cities []string, lastEnd Location) ([]Location, error) {
	var route []Location

	if lastEnd.StreetName != "" {
		route = append(route, lastEnd)
	} else {
		city := cities[rand.Intn(len(cities))]
		randStreet, err := provider.GetRandomStreet(city)
		if err != nil {
			return nil, err
		}
		route = append(route, randStreet)
	}

	city := cities[rand.Intn(len(cities))]
	randStreet, err := provider.GetRandomStreet(city)
	if err != nil {
		return nil, err
	}
	route = append(route, randStreet)

	return route, nil
}
