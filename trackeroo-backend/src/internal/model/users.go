package model

import (
	"time"
)

type User struct {
	ID           string    `bson:"_id,omitempty" json:"id"`
	Username     string    `bson:"username" json:"username"`
	Role         string    `bson:"role" json:"role"`
	PasswordHash string    `bson:"password_hash" json:"-"`
	LastLogin    time.Time `bson:"last_login" json:"last_login"`
	SessionToken string    `bson:"session_token" json:"session_token"`
	IssuedAt     time.Time `bson:"issued_at" json:"issued_at"`
	ExpiresAt    time.Time `bson:"expires_at" json:"expires_at"`
}
