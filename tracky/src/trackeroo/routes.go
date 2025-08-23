package trackeroo

import "math/rand"

var PisaRoutes [][]string = [][]string{
	// 2-stop routes
	{"Piazza dei Miracoli, Pisa, Italy", "Piazza Garibaldi, Pisa, Italy"},
	{"Via Santa Maria, Pisa, Italy", "Corso Italia, Pisa, Italy"},
	{"Lungarno Pacinotti, Pisa, Italy", "Via Roma, Pisa, Italy"},
	{"Borgo Stretto, Pisa, Italy", "Piazza delle Vettovaglie, Pisa, Italy"},
	{"Via San Martino, Pisa, Italy", "Via del Brennero, Pisa, Italy"},
	{"Stazione Pisa Centrale, Pisa, Italy", "Piazza dei Miracoli, Pisa, Italy"},

	// 3-stop routes
	{"Via Santa Maria, Pisa, Italy", "Piazza Garibaldi, Pisa, Italy", "Corso Italia, Pisa, Italy"},
	{"Piazza dei Miracoli, Pisa, Italy", "Borgo Stretto, Pisa, Italy", "Lungarno Pacinotti, Pisa, Italy"},
	{"Via Roma, Pisa, Italy", "Piazza delle Vettovaglie, Pisa, Italy", "Via San Francesco, Pisa, Italy"},
	{"Stazione Pisa Centrale, Pisa, Italy", "Via Carducci, Pisa, Italy", "Via del Brennero, Pisa, Italy"},
	{"Via Benedetto Croce, Pisa, Italy", "Piazza Garibaldi, Pisa, Italy", "Via Nicola Pisano, Pisa, Italy"},
	{"Lungarno Mediceo, Pisa, Italy", "Via Giuseppe Mazzini, Pisa, Italy", "Via Andrea Pisano, Pisa, Italy"},

	// 4-stop routes
	{"Piazza dei Miracoli, Pisa, Italy", "Via Santa Maria, Pisa, Italy", "Borgo Stretto, Pisa, Italy", "Piazza Garibaldi, Pisa, Italy"},
	{"Stazione Pisa Centrale, Pisa, Italy", "Via Roma, Pisa, Italy", "Corso Italia, Pisa, Italy", "Lungarno Pacinotti, Pisa, Italy"},
	{"Via del Brennero, Pisa, Italy", "Via Carducci, Pisa, Italy", "Piazza delle Vettovaglie, Pisa, Italy", "Via San Francesco, Pisa, Italy"},
	{"Via Giuseppe Mazzini, Pisa, Italy", "Via Francesco Crispi, Pisa, Italy", "Via Benedetto Croce, Pisa, Italy", "Via Nicola Pisano, Pisa, Italy"},
	{"Lungarno Mediceo, Pisa, Italy", "Ponte di Mezzo, Pisa, Italy", "Via Curtatone e Montanara, Pisa, Italy", "Via Andrea Pisano, Pisa, Italy"},
	{"Via Aurelia Nord, Pisa, Italy", "Viale delle Piagge, Pisa, Italy", "Via Bonanno Pisano, Pisa, Italy", "Via Vittorio Veneto, Pisa, Italy"},
}

func GetRoute(lastEnd string) []string {
	var route []string
	randRoutes := PisaRoutes[rand.Intn(len(PisaRoutes))]
	if lastEnd != "" {
		randRoutes = append(route, lastEnd)
	}

	route = append(route, randRoutes...)
	return route
}
