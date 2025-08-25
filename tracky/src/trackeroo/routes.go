package trackeroo

import "math/rand"

var streets = []string{
	// Pisa (56127)
	"Piazza dei Cavalieri, Pisa, Italy, 56126",
	"Borgo Stretto, Pisa, Italy, 56127",
	"Via San Frediano, Pisa, Italy, 56126",
	"Lungarno Mediceo, Pisa, Italy, 56127",
	"Via San Martino, Pisa, Italy, 56125",
	"Via di Gargalone, Pisa, Italy, 56127",
	"Via Asmara, Pisa, Italy, 56127",
	"Via Putignano, Pisa, Italy, 56127",
	"Via Fagiana, Pisa, Italy, 56127",
	"Via Santa Bona, Pisa, Italy, 56127",

	// Lucca (55100)
	"Via Fillungo, Lucca, Italy, 55100",
	"Piazza Napoleone, Lucca, Italy, 55100",
	"Via Santa Croce, Lucca, Italy, 55100",
	"Via San Paolino, Lucca, Italy, 55100",
	"Via Guinigi, Lucca, Italy, 55100",
	"Via del Cimitero, Lucca, Italy, 55100",
	"Via di Vicopelago, Lucca, Italy, 55100",
	"Via dei Bollori, Lucca, Italy, 55100",
	"Via delle Fornacette, Lucca, Italy, 55100",
	"Via Stefano Tofanelli, Lucca, Italy, 55100",

	// Firenze (50123)
	"Via dei Calzaiuoli, Firenze, Italy, 50123",
	"Via Tornabuoni, Firenze, Italy, 50123",
	"Borgo San Lorenzo, Firenze, Italy, 50123",
	"Via della Vigna Nuova, Firenze, Italy, 50123",
	"Via Ghibellina, Firenze, Italy, 50122",
	"Via del Gelsomino, Firenze, Italy, 50125",
	"Via Livorno, Firenze, Italy, 50142",
	"Via Gherardo Starnina, Firenze, Italy, 50142",
	"Via della Casella, Firenze, Italy, 50142",
	"Via Antonio del Pollaiolo, Firenze, Italy, 50142",

	// Livorno (57123)
	"Via Grande, Livorno, Italy, 57123",
	"Piazza della Repubblica, Livorno, Italy, 57123",
	"Via Magenta, Livorno, Italy, 57123",
	"Scali delle Cantine, Livorno, Italy, 57123",
	"Via Ricasoli, Livorno, Italy, 57123",
	"Via di Quercianella, Livorno, Italy, 57128",
	"Via del Littorale, Livorno, Italy, 57128",
	"Viale di Antignano, Livorno, Italy, 57128",
	"Via del Pastore, Livorno, Italy, 57128",
	"Via Uberto Mondolfi, Livorno, Italy, 57128",

	// Pontedera (56025)
	"Corso Matteotti, Pontedera, Italy, 56025",
	"Piazza Curtatone, Pontedera, Italy, 56025",
	"Via Roma, Pontedera, Italy, 56025",
	"Via Dante Alighieri, Pontedera, Italy, 56025",
	"Via Verdi, Pontedera, Italy, 56025",
	"Via di Gello, Pontedera, Italy, 56025",
	"Via di Lavaiano, Pontedera, Italy, 56025",
	"Via dell’Industria, Pontedera, Italy, 56025",
	"Via delle Colombaie, Pontedera, Italy, 56025",
	"Via della Fornace, Pontedera, Italy, 56025",

	// Poggibonsi (53036)
	"Via della Repubblica, Poggibonsi, Italy, 53036",
	"Via Trento, Poggibonsi, Italy, 53036",
	"Via San Gimignano, Poggibonsi, Italy, 53036",
	"Via Borgaccio, Poggibonsi, Italy, 53036",
	"Via Sardegna, Poggibonsi, Italy, 53036",
	"Via dell’Ospedale, Poggibonsi, Italy, 53036",
	"Via Abruzzo, Poggibonsi, Italy, 53036",
	"Via Lazio, Poggibonsi, Italy, 53036",
	"Via Sicilia, Poggibonsi, Italy, 53036",
	"Via Calabria, Poggibonsi, Italy, 53036",

	// Viareggio (55049)
	"Viale Giosuè Carducci, Viareggio, Italy, 55049",
	"Piazza Mazzini, Viareggio, Italy, 55049",
	"Via Cesare Battisti, Viareggio, Italy, 55049",
	"Via Santa Maria Goretti, Viareggio, Italy, 55049",
	"Via Coppino, Viareggio, Italy, 55049",
	"Via delle Cavalle, Viareggio, Italy, 55049",
	"Via Marina di Levante, Viareggio, Italy, 55049",
	"Via dei Partigiani, Viareggio, Italy, 55049",
	"Via Santa Gemma Galgani, Viareggio, Italy, 55049",
	"Via della Ferrovia, Viareggio, Italy, 55049",

	// Camaiore (55041)
	"Via Vittorio Emanuele, Camaiore, Italy, 55041",
	"Via XX Settembre, Camaiore, Italy, 55041",
	"Via Roma, Camaiore, Italy, 55041",
	"Via del Secco, Camaiore, Italy, 55041",
	"Via dei Ghivizzani, Camaiore, Italy, 55041",
	"Via Mentana, Camaiore, Italy, 55041",
	"Via Montecassino, Camaiore, Italy, 55041",
	"Via Alessandro Volta, Camaiore, Italy, 55041",
	"Via dei Papaveri, Camaiore, Italy, 55041",
	"Via Montemagno, Camaiore, Italy, 55041",

	// Siena (53100)
	"Via di Città, Siena, Italy, 53100",
	"Banchi di Sopra, Siena, Italy, 53100",
	"Via dei Rossi, Siena, Italy, 53100",
	"Via Pantaneto, Siena, Italy, 53100",
	"Via della Sapienza, Siena, Italy, 53100",
	"Strada Grossetana, Siena, Italy, 53100",
	"Strada Massetana Romana, Siena, Italy, 53100",
	"Via Paolo Mascagni, Siena, Italy, 53100",
	"Strada del Ruffolo, Siena, Italy, 53100",
	"Via di Fiera Vecchia, Siena, Italy, 53100",
}

func GetRoute(lastEnd string) []string {
	var route []string
	if lastEnd != "" {
		route = append(route, lastEnd)
	} else {
		randStreet := streets[rand.Intn(len(streets))]
		route = append(route, randStreet)
	}
	randStreet := streets[rand.Intn(len(streets))]
	route = append(route, randStreet)
	return route
}
