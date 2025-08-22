package model

import (
	"time"
)

const (
	VALUABLES         = "valuable"
	FOOD              = "food"
	PRIVATE_TRANSPORT = "private_transport"
	PUBLIC_TRANSPORT  = "public_transport"
	OTHER             = "other"
)

type DeviceStatus struct {
	Connected   bool      `bson:"connected" json:"connected"`
	LastMessage time.Time `bson:"last_message" json:"last_message"`
}

type Device struct {
	ID         string       `bson:"_id,omitempty" json:"id"`
	Name       string       `bson:"name" json:"name"`
	Status     DeviceStatus `bson:"status" json:"status"`
	DeviceType string       `bson:"device_type" json:"device_type"`
	PrivateKey string       `bson:"private_key" json:"private_key"`
	CreatedAt  time.Time    `bson:"created_at" json:"created_at"`
}

type DeviceCreation struct {
	Name       string `bson:"name" json:"name"`
	DeviceType string `bson:"device_type" json:"device_type"`
}

type DeviceCredentials struct {
	ID         string `bson:"_id,omitempty" json:"id"`
	Name       string `bson:"name" json:"name"`
	DeviceType string `bson:"device_type" json:"device_type"`
	PrivateKey string `bson:"private_key" json:"private_key"`
	MqttHost   string `bson:"mqtt_host" json:"mqtt_host"`
	MqttPort   int    `bson:"mqtt_port" json:"mqtt_port"`
	MqttMode   string `bson:"mqtt_mode" json:"mqtt_mode"`
	CACert     string `bson:"ca_cert" json:"ca_cert"`
}
