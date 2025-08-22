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
	Type       string       `bson:"type" json:"type"`
	PrivateKey string       `bson:"private_key" json:"private_key"`
	CreatedAt  time.Time    `bson:"created_at" json:"created_at"`
}

type DeviceCreation struct {
	Name string `bson:"name" json:"name"`
	Type string `bson:"type" json:"type"`
}
