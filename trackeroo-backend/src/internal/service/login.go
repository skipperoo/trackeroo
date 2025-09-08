// Package service provides access to DB, cache, login validation and redis.
package service

import (
	"context"
	"trackeroo-backend/internal/logger"
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
		logger.Error("Failed to get user by name: %v", err)
		return false
	}
	logger.Debug("Checking login information for: %s", user.Username)
	err = bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(data.Password))
	logger.Warning("Failed to authenticate %s: %v", data.Username, err)
	return err == nil
}
