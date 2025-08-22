package service

import (
	"context"
	"trackeroo-backend/internal/model"

	"golang.org/x/crypto/bcrypt"
)

func ValidateLogin(ctx context.Context, data model.Login) bool {
	if data.Username == "" || data.Password == "" {
		return false
	}
	var user model.User
	var err error
	if user, err = GetUserByName(ctx, data.Username); err != nil {
		Error("Failed to get user by name: %v", err)
		return false
	}
	Debug("Checking login information for: %s", user.Username)
	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(data.Password))
	if err != nil {
		return false
	}
	return true
}
