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

var adjectives = []string{
	"admiring", "adoring", "affectionate", "agitated", "amazing",
	"angry", "awesome", "beautiful", "blissful", "bold",
	"boring", "brave", "busy", "calm", "charming",
	"clever", "cool", "compassionate", "competent", "condescending",
	"confident", "cranky", "crazy", "curious", "dazzling",
	"determined", "distracted", "dreamy", "eager", "ecstatic",
	"elastic", "elated", "elegant", "eloquent", "enchanting",
	"energetic", "epic", "exciting", "exotic", "fabulous",
	"faithful", "fancy", "fascinated", "fearless", "fervent",
	"flamboyant", "focused", "friendly", "frosty", "funny",
	"furious", "gallant", "gentle", "gifted", "goofy",
	"graceful", "gracious", "great", "grieving", "groovy",
	"happy", "hardcore", "heartwarming", "heuristic", "hopeful",
	"hungry", "hyper", "inspiring", "intelligent", "interesting",
	"inventive", "iron", "jaunty", "jolly", "jovial",
	"keen", "kind", "laughing", "loving", "lucid",
	"magical", "magnificent", "merry", "modest", "motivated",
	"mystifying", "naughty", "nervous", "noble", "nostalgic",
	"objective", "optimistic", "optimized", "original", "outstanding",
	"patient", "peaceful", "pedantic", "phenomenal", "pious",
	"playful", "polite", "practical", "proud", "puzzled",
	"quirky", "quizzical", "relaxed", "romantic", "sad",
	"serene", "sharp", "silly", "sleepy", "stoic",
	"strange", "suspicious", "sweet", "tender", "thirsty",
	"thoughtful", "thrilled", "trusting", "upbeat", "vibrant",
	"wonderful", "xenodochial", "youthful", "zealous",
}

var names = []string{
	"abel", "ampere", "babbage", "bardeen", "bell",
	"berners", "bohr", "born", "bose", "cantor",
	"carmack", "cauchy", "chebyshev", "church", "compton",
	"cooper", "coulomb", "crick", "curie", "darwin",
	"dedekind", "dijkstra", "dirac", "doppler", "dyson",
	"edison", "einstein", "erdos", "euler", "faraday",
	"fermi", "feynman", "fizeau", "fourier", "franklin",
	"galileo", "galois", "galvani", "gates", "gauss",
	"glashow", "godel", "hardy", "hawking", "heisenberg",
	"henry", "hertz", "higgs", "hilbert", "hopper",
	"jacobi", "jobs", "kelvin", "kleene", "knuth",
	"kolmogorov", "lagrange", "laplace", "lee", "leibniz",
	"lovelace", "marconi", "markov", "maxwell", "mendeleev",
	"michelson", "millikan", "morley", "morse", "newton",
	"noether", "nyquist", "ohm", "oppenheimer", "pascal",
	"pauli", "pauling", "peano", "penrose", "planck",
	"poincare", "ramanujan", "riemann", "ritchie", "russell",
	"rutherford", "salam", "schrodinger", "schwinger", "shannon",
	"shockley", "siemens", "stallman", "tesla", "thompson",
	"torvalds", "turing", "volta", "watson", "weber",
	"weinberg", "weierstrass", "westinghouse", "whitehead", "wiener",
	"wirth", "wozniak", "wu", "yang",
}

func generateRandomName() string {
	// Seed the random number generator (do this once in your main function, not every time)
	rand.Seed(time.Now().UnixNano())

	adjective := adjectives[rand.Intn(len(adjectives))]
	name := names[rand.Intn(len(names))]

	return fmt.Sprintf("%s_%s", adjective, name)
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

	name := generateRandomName()
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
			name = generateRandomName()
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
		createdAt := generateRandomTime()

		jsObject := fmt.Sprintf(`{
    _id: "%s",
    name: "%s",
    status: { connected: false, last_message: "1970-01-01T00:00:00Z" },
    device_type: "%s",
    private_key: "%s",
    created_at: ISODate("%s"),
}`, id, name, deviceType, privateKey, createdAt)

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

	count := 100
	fmt.Printf("// Generated %d JS objects with no duplicates:\n", count)
	err := generateJSObjectList(count)
	if err != nil {
		fmt.Printf("Error generating objects: %v\n", err)
	}
}
