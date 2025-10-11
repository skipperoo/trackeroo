package middleware

import (
	"encoding/json"
	"net/http"
	"trackeroo-backend/internal/logger"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"
)

func Auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("Checking auth...")
		valid, err := service.ValidateUserJWT(r.Header.Get("Authorization"), "admin")

		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			logger.Error("Error validating JWT: %v", err)
			json.NewEncoder(w).Encode(model.Error{Error: "Unauthorized!"})
			return
		}
		if !valid {
			w.WriteHeader(http.StatusUnauthorized)
			logger.Error("JWT is not valid")
			json.NewEncoder(w).Encode(model.Error{Error: "Unauthorized!"})
			return
		}
		logger.Debug("User is authenticated")
		next.ServeHTTP(w, r)
	})
}

func MqttAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("Checking mqtt auth...")
		ctx := r.Context()
		username := r.Header.Get("Username")
		key, err := service.GetDeviceKey(ctx, username)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			logger.Error("Error getting the device key (id %s): %v", username, err)
			json.NewEncoder(w).Encode(model.Error{Error: "Unauthorized!"})
			return
		}
		if res, claims, err := service.ValidateDeviceJWT(key, r.Header.Get("Authorization")); err != nil || !res || claims["sub"] != username {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(model.Error{Error: "Unauthorized!"})
			if err == nil {
				logger.Error("Failed to validate device JWT for %s", username)
			} else if claims["sub"] != username {
				logger.Error("Invalid username in JWT: should be %s, found %s", username, claims["sub"])
			} else {
				logger.Error("Failed to validate device JWT for %s: %v", username, err)
			}
			return
		}
		logger.Debug("Device %s authenticated", username)
		next.ServeHTTP(w, r)
	})
}
