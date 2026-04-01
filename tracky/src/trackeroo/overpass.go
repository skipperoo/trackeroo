package trackeroo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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
			Timeout: 120 * time.Second,
		},
	}
}

func (c *OverpassClient) runQuery(query string) (OverpassResponse, error) {
	const maxRetries = 10

	form := url.Values{}
	form.Set("data", query)

	var lastErr error
	var overpassResp OverpassResponse

	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := c.Client.Post(
			c.BaseURL,
			"application/x-www-form-urlencoded",
			strings.NewReader(form.Encode()),
		)
		if err != nil {
			lastErr = fmt.Errorf("failed to query Overpass API: %w", err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("Overpass API returned status %d: %s", resp.StatusCode, string(bodyBytes))

			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			break
		}

		contentType := resp.Header.Get("Content-Type")
		if strings.Contains(contentType, "text/html") || bytes.HasPrefix(bodyBytes, []byte("<?xml")) || bytes.HasPrefix(bodyBytes, []byte("<!DOCTYPE")) {
			lastErr = fmt.Errorf("Overpass API returned HTML error instead of JSON: %s", string(bodyBytes))
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		if err := json.Unmarshal(bodyBytes, &overpassResp); err != nil {
			lastErr = fmt.Errorf("failed to decode response: %w", err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		return overpassResp, nil
	}

	return OverpassResponse{}, lastErr
}

func (c *OverpassClient) GetStreets(city string, limit int, radiusMeters int) ([]OverpassElement, error) {
	if limit == 0 {
		limit = 100
	}

	cityKey := fmt.Sprintf("tracky::cities::%s::limits::%d::radius::%d", city, limit, radiusMeters)
	if redisClient != nil {
		cached, err := redisClient.Get(ctx, cityKey).Result()
		if err == nil {
			var elements []OverpassElement
			if err := json.Unmarshal([]byte(cached), &elements); err == nil {
				Info("Retrieved %d streets from Redis cache", len(elements))
				return elements, nil
			}
			Warning("Failed to unmarshal cached streets: %v", err)
		} else if err != redis.Nil {
			Warning("Redis get error: %v", err)
		}
	}

	query := fmt.Sprintf(`
[out:json][timeout:120];
area["ISO3166-1"="IT"][admin_level=2]->.italy;
node["name"="%s"]["place"~"city|town"](area.italy)->.citynode;
(
  way(around.citynode:%d)["highway"~"primary|secondary|tertiary|residential|unclassified"]["name"]["highway"!~"motorway|trunk|motorway_link|trunk_link"](area.italy);
);
out tags geom %d;
`, city, radiusMeters, limit)

	overpassResp, err := c.runQuery(query)
	if err != nil {
		return nil, err
	}

	if len(overpassResp.Elements) == 0 {
		return nil, fmt.Errorf("no streets found for city: %s", city)
	}

	if redisClient != nil {
		data, err := json.Marshal(overpassResp.Elements)
		if err == nil {
			if err := redisClient.Set(ctx, cityKey, data, 0).Err(); err != nil {
				Warning("Failed to cache regions in Redis: %v", err)
			} else {
				Info("Cached %d streets (city: %s, limit: %d, radius: %d) in Redis", len(overpassResp.Elements), city, limit, radiusMeters)
			}
		}
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
	if redisClient != nil {
		cached, err := redisClient.Get(ctx, "tracky::regions").Result()
		if err == nil {
			var regions []string
			if err := json.Unmarshal([]byte(cached), &regions); err == nil {
				Info("Retrieved %d regions from Redis cache", len(regions))
				return regions, nil
			}
			Warning("Failed to unmarshal cached regions: %v", err)
		} else if err != redis.Nil {
			Warning("Redis get error: %v", err)
		}
	}
	query := `[out:json][timeout:120];
area["ISO3166-1"="IT"][admin_level=2]->.italy;
relation["boundary"="administrative"]["admin_level"=4]["ISO3166-2"~"^IT-"](area.italy);
out tags;`

	overpassResp, err := c.runQuery(query)
	if err != nil {
		return nil, err
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
	if redisClient != nil {
		data, err := json.Marshal(regions)
		if err == nil {
			if err := redisClient.Set(ctx, "tracky::regions", data, 0).Err(); err != nil {
				Warning("Failed to cache regions in Redis: %v", err)
			} else {
				Info("Cached %d regions in Redis", len(regions))
			}
		}
	}
	return regions, nil
}

func (c *OverpassClient) GetCities(region string) ([]string, error) {
	regionKey := fmt.Sprintf("tracky::regions::%s", region)
	if redisClient != nil {
		cached, err := redisClient.Get(ctx, regionKey).Result()
		if err == nil {
			var cities []string
			if err := json.Unmarshal([]byte(cached), &cities); err == nil {
				Info("Retrieved %d cities (%s) from Redis cache", len(cities), region)
				return cities, nil
			}
			Warning("Failed to unmarshal cached cities: %v", err)
		} else if err != redis.Nil {
			Warning("Redis get error: %v", err)
		}
	}
	query := fmt.Sprintf(`[out:json][timeout:120];
relation["boundary"="administrative"]["name"="%s"]["admin_level"=4]->.reg;
.reg map_to_area->.region;
node["place"~"city|town"](area.region);
out tags;`, region)

	overpassResp, err := c.runQuery(query)
	if err != nil {
		return nil, err
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

	if redisClient != nil {
		data, err := json.Marshal(cities)
		if err == nil {
			if err := redisClient.Set(ctx, regionKey, data, 0).Err(); err != nil {
				Warning("Failed to cache regions in Redis: %v", err)
			} else {
				Info("Cached %d cities in Redis", len(cities))
			}
		}
	}
	return cities, nil
}
