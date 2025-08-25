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
	count := 30

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
