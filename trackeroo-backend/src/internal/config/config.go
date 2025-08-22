package config

import (
	"os"
	"strconv"
	"trackeroo-backend/internal/logger"
)

type Config struct {
	MongoUri  string
	LogLevel  string
	MqttHost  string
	MqttPort  int
	MqttsPort int
}

var Cfg *Config

func LoadConfig() {
	MongoUri := os.Getenv("MONGO_URI")
	if MongoUri == "" {
		logger.Fatal("MONGO_URI is not set")
	}
	LogLevel := os.Getenv("LOG_LEVEL")
	if LogLevel == "" {
		logger.Fatal("LOG_LEVEL is not set")
	}
	MqttHost := os.Getenv("MQTT_HOST")
	if MqttHost == "" {
		logger.Fatal("MQTT_HOST is not set")
	}
	MqttPort, err := strconv.Atoi(os.Getenv("MQTT_PORT"))
	if err != nil {
		logger.Fatal("Cannot parse MQTT_PORT: %v", err)
	}
	MqttsPort, err := strconv.Atoi(os.Getenv("MQTTS_PORT"))
	if err != nil {
		logger.Fatal("Cannot parse MQTTS_PORT: %v", err)
	}

	Cfg = &Config{
		MongoUri:  MongoUri,
		LogLevel:  LogLevel,
		MqttHost:  MqttHost,
		MqttPort:  MqttPort,
		MqttsPort: MqttsPort,
	}
}
