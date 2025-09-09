package main

import (
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/rand"
	mrand "math/rand"
	"time"
)

const (
	VALUABLES         = "valuable"
	FOOD              = "food"
	PRIVATE_TRANSPORT = "private_transport"
	PUBLIC_TRANSPORT  = "public_transport"
	OTHER             = "other"
)

// Container-style names for randomization
var containerNames = []string{
	"amazing_turing", "boring_wozniak", "clever_newton", "dreamy_tesla",
	"eager_darwin", "friendly_curie", "gracious_hawking", "happy_einstein",
	"intelligent_jobs", "jolly_gates", "kind_torvalds", "loving_lovelace",
	"mystifying_feynman", "naughty_dijkstra", "optimistic_babbage", "peaceful_pascal",
	"quirky_shannon", "relaxed_turing", "serene_hopper", "trusting_knuth",
	"upbeat_ritchie", "vibrant_thompson", "wonderful_wirth", "xenodochial_carmack",
	"youthful_stallman", "zealous_berners", "admiring_bohr", "adoring_morse",
	"affectionate_bell", "agitated_planck", "amazing_galileo", "angry_maxwell",
	"boring_heisenberg", "brave_schrodinger", "busy_pauli", "charming_dirac",
	"clever_fermi", "compassionate_oppenheimer", "competent_rutherford", "condescending_volta",
	"confident_faraday", "cool_ohm", "cranky_ampere", "crazy_coulomb",
	"curious_feynman", "dazzling_maxwell", "determined_kelvin", "distracted_planck",
	"dreamy_euler", "eager_gauss", "ecstatic_riemann", "elastic_fourier",
	"elegant_lagrange", "elated_laplace", "eloquent_leibniz", "enchanting_poincare",
	"energetic_hilbert", "epic_cantor", "exciting_godel", "exotic_turing",
	"fabulous_ramanujan", "faithful_hardy", "fancy_erdos", "fascinated_noether",
	"fearless_galois", "fervent_abel", "flamboyant_jacobi", "focused_cauchy",
	"friendly_weierstrass", "frosty_dedekind", "funny_peano", "furious_russell",
	"gallant_whitehead", "gentle_church", "gifted_kleene", "goofy_markov",
	"graceful_chebyshev", "great_kolmogorov", "grieving_wiener", "groovy_shannon",
	"happy_nyquist", "hardcore_bell", "heartwarming_bose", "heuristic_fermi",
	"hopeful_bardeen", "hungry_cooper", "hyper_shockley", "inspiring_bardeen",
	"interesting_watson", "inventive_crick", "iron_franklin", "jaunty_pauling",
	"jovial_mendeleev", "keen_bohr", "laughing_rutherford", "lucid_heisenberg",
	"magical_dirac", "magnificent_feynman", "merry_schwinger", "modest_dyson",
	"motivated_penrose", "nervous_hawking", "noble_weinberg", "nostalgic_salam",
	"objective_glashow", "optimized_higgs", "original_yang", "outstanding_lee",
	"patient_wu", "pedantic_pauli", "phenomenal_born", "pious_planck",
	"playful_compton", "polite_millikan", "practical_michelson", "proud_morley",
	"puzzled_fizeau", "quizzical_doppler", "romantic_hertz", "sad_marconi",
	"serene_tesla", "sharp_edison", "silly_westinghouse", "sleepy_siemens",
	"stoic_ohm", "strange_ampere", "suspicious_volta", "sweet_galvani",
	"tender_faraday", "thirsty_henry", "thoughtful_weber", "thrilled_gauss",
}

var deviceTypes = []string{VALUABLES, FOOD, PRIVATE_TRANSPORT, PUBLIC_TRANSPORT, OTHER}

func GenPrivateKey() (string, error) {
	privateKey := make([]byte, 32)
	_, err := crand.Read(privateKey)
	if err != nil {
		return "", err
	}
	encodedKey := base64.StdEncoding.EncodeToString(privateKey)
	return encodedKey, nil
}

func generateRandomID() string {
	// Use current timestamp + random bytes for uniqueness
	timestamp := time.Now().UnixNano()
	randomBytes := make([]byte, 8)
	rand.Read(randomBytes)
	return fmt.Sprintf("trk-%x%x", timestamp, randomBytes[:4])
}

func generateRandomTime() string {
	// Generate a random time within the last 30 days
	now := time.Now()
	randomDays := mrand.Intn(30)
	randomHours := mrand.Intn(24)
	randomMinutes := mrand.Intn(60)

	randomTime := now.AddDate(0, 0, -randomDays).Add(-time.Duration(randomHours) * time.Hour).Add(-time.Duration(randomMinutes) * time.Minute)
	return randomTime.Format("2006-01-02T15:04:05Z")
}

func generateJSObject() (string, error) {
	privateKey, err := GenPrivateKey()
	if err != nil {
		return "", err
	}

	name := containerNames[mrand.Intn(len(containerNames))]
	deviceType := deviceTypes[mrand.Intn(len(deviceTypes))]
	id := generateRandomID()
	connected := mrand.Intn(2) == 1 // Random boolean
	lastMessage := generateRandomTime()
	createdAt := generateRandomTime()

	jsObject := fmt.Sprintf(`{
    _id: "%s",
    name: "%s",
    status: { connected: %t, last_message: "%s" },
    device_type: "%s",
    private_key: "%s",
    created_at: ISODate("%s"),
}`, id, name, connected, lastMessage, deviceType, privateKey, createdAt)

	return jsObject, nil
}

func generateJSObjectList(count int) error {
	usedNames := make(map[string]bool)
	usedIDs := make(map[string]bool)

	fmt.Println("[")
	for i := 0; i < count; i++ {
		var name, id string

		// Ensure unique name
		for {
			name = containerNames[mrand.Intn(len(containerNames))]
			if !usedNames[name] {
				usedNames[name] = true
				break
			}
		}

		// Ensure unique ID
		for {
			id = generateRandomID()
			if !usedIDs[id] {
				usedIDs[id] = true
				break
			}
		}

		privateKey, err := GenPrivateKey()
		if err != nil {
			return err
		}

		deviceType := deviceTypes[mrand.Intn(len(deviceTypes))]
		connected := mrand.Intn(2) == 1 // Random boolean
		lastMessage := generateRandomTime()
		createdAt := generateRandomTime()

		jsObject := fmt.Sprintf(`{
    _id: "%s",
    name: "%s",
    status: { connected: %t, last_message: "%s" },
    device_type: "%s",
    private_key: "%s",
    created_at: ISODate("%s"),
}`, id, name, connected, lastMessage, deviceType, privateKey, createdAt)

		fmt.Print(jsObject)
		if i < count-1 {
			fmt.Println(",")
		} else {
			fmt.Println()
		}
	}
	fmt.Println("]")
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Generate 5 objects by default (max 50 due to name uniqueness)
	count := 100

	if count > len(containerNames) {
		fmt.Printf("Warning: Requested %d objects but only %d unique names available. Using %d objects.\n",
			count, len(containerNames), len(containerNames))
		count = len(containerNames)
	}

	fmt.Printf("// Generated %d JS objects with no duplicates:\n", count)
	err := generateJSObjectList(count)
	if err != nil {
		fmt.Printf("Error generating objects: %v\n", err)
	}
}
