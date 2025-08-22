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

func DefaultUsers() []User {
	user1 := User{
		ID:           "",
		Username:     "leonardo",
		Role:         "admin",
		PasswordHash: "$2b$12$pVXfJaREXTdMyri7Z61KjebMVwCCo0aientxTAis/G57NTc/WW6CO",
		LastLogin:    time.Now(),
		SessionToken: "",
		IssuedAt:     time.Now(),
		ExpiresAt:    time.Now().Add(time.Hour * 24),
	}

	user2 := User{
		ID:           "",
		Username:     "simone",
		Role:         "admin",
		PasswordHash: "$2b$12$WT0QBYqqOdYY4i2SlIKFueoz5AM6wcIBqf9BV3Sz8AUD1nit.bWJu",
		LastLogin:    time.Now(),
		SessionToken: "",
		IssuedAt:     time.Now(),
		ExpiresAt:    time.Now().Add(time.Hour * 24),
	}
	return []User{user1, user2}
}
