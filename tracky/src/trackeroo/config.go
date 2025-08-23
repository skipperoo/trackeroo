package trackeroo

import (
	"encoding/base64"
	"encoding/json"
	"os"

	jwt "github.com/golang-jwt/jwt/v5"
)

const (
	VALUABLES         = "valuable"
	FOOD              = "food"
	PRIVATE_TRANSPORT = "private_transport"
	PUBLIC_TRANSPORT  = "public_transport"
	OTHER             = "other"
)

type Credentials struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	DeviceType string `json:"device_type"`
	PrivateKey string `json:"private_key"`
	MqttHost   string `bson:"mqtt_host" json:"mqtt_host"`
	MqttPort   int    `bson:"mqtt_port" json:"mqtt_port"`
	MqttMode   string `bson:"mqtt_mode" json:"mqtt_mode"`
	CaCert     string `json:"ca_cert"`
}

var Creds *Credentials

func GetCredentials() (*Credentials, error) {
	if Creds != nil {
		return Creds, nil
	}
	cfg, err := os.ReadFile("credentials/tdevice.json")
	if err != nil {
		return nil, err
	}
	credentials := new(Credentials)
	json.Unmarshal(cfg, credentials)
	Creds = credentials
	return Creds, nil
}

func GetToken(key string, exp int, iat int, sub string) (string, error) {
	t := jwt.NewWithClaims(jwt.SigningMethodHS256,
		jwt.MapClaims{
			"exp": int(UnixTime()) + exp,
			"iat": int(UnixTime()) - iat,
			"sub": sub,
		})
	secret, _ := base64.StdEncoding.DecodeString(key)
	token, err := t.SignedString(secret)
	return token, err
}

func CreateCaFile() error {
	credentials, err := GetCredentials()
	if err != nil {
		return err
	}
	f, err := os.Create("credentials/cacert.pem")
	if err != nil {
		return err
	}
	f.Write([]byte(credentials.CaCert))
	f.Close()
	return nil
}
