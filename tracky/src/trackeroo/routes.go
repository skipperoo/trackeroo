package trackeroo

import (
	"math/rand"
)

type Street struct {
	Coordinate Coordinate
	StreetName string
	City       string
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

func (p *CachedStreetProvider) GetRandomStreet(city string) (Street, error) {
	Info("Getting random street from %s", city)
	if streets, ok := p.cache[city]; ok && len(streets) > 0 {
		Info("Cache hit for %s", city)
		return stringToStreet(city, streets[rand.Intn(len(streets))]), nil
	}

	streets, err := p.client.GetStreets(city, 100)
	if err != nil {
		return Street{}, err
	}

	p.cache[city] = streets

	return stringToStreet(city, streets[rand.Intn(len(streets))]), nil
}

func GetRoute(provider *CachedStreetProvider, cities []string, lastEnd Street) ([]Street, error, string) {
	var route []Street

	if lastEnd.StreetName != "" {
		route = append(route, lastEnd)
	} else {
		city := cities[rand.Intn(len(cities))]
		randStreet, err := provider.GetRandomStreet(city)
		if err != nil {
			return nil, err, city
		}
		route = append(route, randStreet)
	}

	city := cities[rand.Intn(len(cities))]
	randStreet, err := provider.GetRandomStreet(city)
	if err != nil {
		return nil, err, city
	}
	route = append(route, randStreet)

	return route, nil, ""
}
