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
	Type string            `json:"type"`
	ID   int64             `json:"id"`
	Tags map[string]string `json:"tags"`
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

func (c *OverpassClient) GetStreets(city string, limit int) ([]string, error) {
	if limit == 0 {
		limit = 100
	}
	radiusMeters := 2000 // 2km default

	query := fmt.Sprintf(`
[out:json][timeout:25];
area["ISO3166-1"="IT"][admin_level=2]->.italy;
node["name"="%s"]["place"~"city|town"](area.italy)->.citynode;
(
  way(around.citynode:%d)["highway"]["name"]["highway"!~"motorway|trunk|motorway_link|trunk_link"](area.italy);
);
out tags %d;
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

	var streets []string
	for _, element := range overpassResp.Elements {
		if name, ok := element.Tags["name"]; ok && name != "" {
			postcode := element.Tags["addr:postcode"]

			if postcode != "" {
				streets = append(streets, fmt.Sprintf("%s, %s, Italy, %s", name, city, postcode))
			} else {
				streets = append(streets, fmt.Sprintf("%s, %s, Italy", name, city))
			}
		}
	}

	if len(streets) == 0 {
		return nil, fmt.Errorf("no streets found for city: %s", city)
	}

	return streets, nil
}

func (c *OverpassClient) GetRandomStreet(city string) (string, error) {
	streets, err := c.GetStreets(city, 100)
	if err != nil {
		return "", err
	}
	return streets[rand.Intn(len(streets))], nil
}

type CachedStreetProvider struct {
	client *OverpassClient
	cache  map[string][]string
}

func NewCachedStreetProvider(baseURL string) *CachedStreetProvider {
	return &CachedStreetProvider{
		client: NewOverpassClient(baseURL),
		cache:  make(map[string][]string),
	}
}

func (p *CachedStreetProvider) GetRandomStreet(city string) (string, error) {
	if streets, ok := p.cache[city]; ok && len(streets) > 0 {
		return streets[rand.Intn(len(streets))], nil
	}

	streets, err := p.client.GetStreets(city, 100)
	if err != nil {
		return "", err
	}

	p.cache[city] = streets

	return streets[rand.Intn(len(streets))], nil
}

// GetRoute generates a route with random streets
// If using cached provider, pass cities to select from
func GetRoute(provider *CachedStreetProvider, cities []string, lastEnd string) ([]string, error) {
	var route []string

	if lastEnd != "" {
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
