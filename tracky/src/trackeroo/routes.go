package trackeroo

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
)

type Street struct {
	Coordinate Coordinate `json:"coordinate"`
	StreetName string     `json:"street_name"`
	City       string     `json:"city"`
}

func stringToStreet(city string, street OverpassElement) Street {
	return Street{
		Coordinate: street.Geometry[rand.Intn(len(street.Geometry))],
		StreetName: street.Tags["name"],
		City:       city,
	}
}

type CachedStreetProvider struct {
	client *OverpassClient
	cache  map[string][]OverpassElement
}

func NewCachedStreetProvider(baseURL string, overpassClient *OverpassClient) *CachedStreetProvider {
	return &CachedStreetProvider{
		client: overpassClient,
		cache:  make(map[string][]OverpassElement),
	}
}

func (p *CachedStreetProvider) GetRandomStreet(city string, isUrban bool) (Street, error) {
	Info("Getting random street from %s", city)
	if streets, ok := p.cache[city]; ok && len(streets) > 0 {
		Info("Cache hit for %s", city)
		return stringToStreet(city, streets[rand.Intn(len(streets))]), nil
	}
	radiusMeters := 2000
	if isUrban {
		radiusMeters = 10000
	}
	streets, err := p.client.GetStreets(city, 100, radiusMeters)
	if err != nil {
		return Street{}, err
	}

	p.cache[city] = streets

	return stringToStreet(city, streets[rand.Intn(len(streets))]), nil
}

func GetRoute(provider *CachedStreetProvider, cities []string, lastEnd Street, isUrban bool) ([]Street, error, string) {
	var route []Street

	if lastEnd.StreetName != "" {
		route = append(route, lastEnd)
	} else {
		city := cities[rand.Intn(len(cities))]
		randStreet, err := provider.GetRandomStreet(city, isUrban)
		if err != nil {
			return nil, err, city
		}
		route = append(route, randStreet)
	}

	city := cities[rand.Intn(len(cities))]
	randStreet, err := provider.GetRandomStreet(city, isUrban)
	if err != nil {
		return nil, err, city
	}
	route = append(route, randStreet)

	return route, nil, ""
}

func ExistsPrevRoute(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func SaveRoute(route []Street, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	jsonData, err := json.Marshal(route)
	if err != nil {
		return err
	}
	fmt.Fprintf(file, "%s\n", jsonData)

	return nil
}

func LoadRoute(filename string) ([]Street, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var route []Street
	err = json.NewDecoder(file).Decode(&route)
	if err != nil {
		return nil, err
	}

	return route, nil
}

func DeleteRoute(filename string) error {
	return os.Remove(filename)
}
