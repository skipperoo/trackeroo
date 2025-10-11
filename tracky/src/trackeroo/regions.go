package trackeroo

import (
	"math/rand"
	"time"
)

var regions = map[string][]string{
	"Abruzzo": {
		"L'Aquila", "Teramo", "Pescara", "Chieti", "Sulmona", "Avezzano",
		"Lanciano", "Giulianova", "Roseto degli Abruzzi", "Vasto",
	},
	"Basilicata": {
		"Potenza", "Matera", "Melfi", "Policoro", "Pisticci",
		"Venosa", "Bernalda", "Rionero in Vulture", "Maratea", "Lavello",
	},
	"Calabria": {
		"Catanzaro", "Cosenza", "Reggio Calabria", "Crotone", "Vibo Valentia",
		"Lamezia Terme", "Corigliano-Rossano", "Gioia Tauro", "Siderno", "Paola",
	},
	"Campania": {
		"Napoli", "Salerno", "Avellino", "Benevento", "Caserta",
		"Pompei", "Torre del Greco", "Pozzuoli", "Giugliano", "Nocera Inferiore",
	},
	"Emilia-Romagna": {
		"Bologna", "Modena", "Parma", "Reggio Emilia", "Ferrara",
		"Rimini", "Ravenna", "Forlì", "Cesena", "Piacenza",
	},
	"Friuli-Venezia Giulia": {
		"Trieste", "Udine", "Pordenone", "Gorizia", "Monfalcone",
		"Cervignano del Friuli", "Codroipo", "Latisana", "Cividale del Friuli", "San Daniele del Friuli",
	},
	"Lazio": {
		"Roma", "Latina", "Frosinone", "Viterbo", "Rieti",
		"Civitavecchia", "Tivoli", "Guidonia Montecelio", "Anzio", "Fiumicino",
	},
	"Liguria": {
		"Genova", "Savona", "La Spezia", "Imperia", "Sanremo",
		"Rapallo", "Chiavari", "Alassio", "Ventimiglia", "Levanto",
	},
	"Lombardia": {
		"Milano", "Bergamo", "Brescia", "Como", "Monza",
		"Pavia", "Mantova", "Varese", "Cremona", "Lecco",
	},
	"Marche": {
		"Ancona", "Macerata", "Pesaro", "Urbino", "Ascoli Piceno",
		"Fermo", "Civitanova Marche", "Senigallia", "Jesi", "Fabriano",
	},
	"Molise": {
		"Campobasso", "Isernia", "Termoli", "Venafro", "Agnone",
		"Bojano", "Larino", "Trivento", "Guglionesi", "Montenero di Bisaccia",
	},
	"Piemonte": {
		"Torino", "Alessandria", "Asti", "Biella", "Cuneo",
		"Novara", "Vercelli", "Verbania", "Ivrea", "Moncalieri",
	},
	"Puglia": {
		"Bari", "Lecce", "Taranto", "Foggia", "Brindisi",
		"Andria", "Barletta", "Trani", "Manfredonia", "Gallipoli",
	},
	"Sardegna": {
		"Cagliari", "Sassari", "Nuoro", "Oristano", "Olbia",
		"Alghero", "Carbonia", "Iglesias", "Tempio Pausania", "Quartú Sant'Elena",
	},
	"Sicilia": {
		"Palermo", "Catania", "Messina", "Siracusa", "Trapani",
		"Ragusa", "Enna", "Caltanissetta", "Agrigento", "Marsala",
	},
	"Toscana": {
		"Firenze", "Pisa", "Siena", "Arezzo", "Lucca",
		"Livorno", "Massa", "Prato", "Grosseto", "Viareggio",
	},
	"Trentino-Alto Adige": {
		"Trento", "Bolzano", "Merano", "Rovereto", "Bressanone",
		"Brunico", "Vipiteno", "Pergine Valsugana", "Arco", "Laives",
	},
	"Umbria": {
		"Perugia", "Terni", "Foligno", "Spoleto", "Assisi",
		"Città di Castello", "Gubbio", "Orvieto", "Todi", "Bastia Umbra",
	},
	"Valle d'Aosta": {
		"Aosta", "Courmayeur", "Saint-Vincent", "Châtillon", "Pont-Saint-Martin",
		"Sarre", "Gressoney-Saint-Jean", "Cogne", "La Thuile", "Pré-Saint-Didier",
	},
	"Veneto": {
		"Venezia", "Verona", "Vicenza", "Padova", "Treviso",
		"Rovigo", "Belluno", "Mestre", "Chioggia", "Bassano del Grappa",
	},
}

func GetRandomRegion() []string {
	rand.Seed(time.Now().UnixNano())

	// Extract keys
	keys := make([]string, 0, len(regions))
	for k := range regions {
		keys = append(keys, k)
	}

	// Pick random key
	return regions[keys[rand.Intn(len(keys))]]
}

func GetAllCities() []string {
	var cities []string
	for _, region := range regions {
		cities = append(cities, region...)
	}
	return cities
}
