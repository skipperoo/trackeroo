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

func NewOverpassClient(baseURL string) *OverpassClient {
	return &OverpassClient{
		BaseURL: baseURL,
		Client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *OverpassClient) GetStreets(city string, limit int, radiusMeters int) ([]OverpassElement, error) {
	if limit == 0 {
		limit = 100
	}

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

func (c *OverpassClient) GetRandomStreet(city string, radiusMeters int) (Street, error) {
	streets, err := c.GetStreets(city, 100, radiusMeters)
	if err != nil {
		return Street{}, err
	}
	randStreet := streets[rand.Intn(len(streets))]
	return stringToStreet(city, randStreet), nil
}

func (c *OverpassClient) GetRegions() ([]string, error) {

	query := `[out:json][timeout:25];
area["ISO3166-1"="IT"][admin_level=2]->.italy;
relation["boundary"="administrative"]["admin_level"=4](area.italy);
out tags;`

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
		return nil, fmt.Errorf("no regions found")
	}
	regions := make([]string, 0)
	for _, element := range overpassResp.Elements {
		if element.Type == "relation" {
			regions = append(regions, element.Tags["name"])
		}
	}
	return regions, nil
}

func (c *OverpassClient) GetCities(region string) ([]string, error) {
	query := fmt.Sprintf(`'[out:json][timeout:25];
relation["boundary"="administrative"]["name"="%s"]["admin_level"=4]->.reg;
.reg map_to_area->.region;
node["place"~"city|town"](area.region);
out tags;'`, region)
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
		return nil, fmt.Errorf("no cities found")
	}
	cities := make([]string, 0)
	for _, element := range overpassResp.Elements {
		if element.Type == "node" {
			cities = append(cities, element.Tags["name"])
		}
	}
	return cities, nil
}
