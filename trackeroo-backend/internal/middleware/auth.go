package middleware

import (
	"encoding/json"
	"net/http"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"
)

func Auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		service.Debug("Checking auth...")
		valid, err := service.ValidateJWT(r.Header.Get("Authorization"), "admin")

		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			service.Error("Error validating JWT: %v", err)
			json.NewEncoder(w).Encode(model.Error{Error: "Unauthorized!"})
			return
		}
		if !valid {
			w.WriteHeader(http.StatusUnauthorized)
			service.Error("JWT is not valid")
			json.NewEncoder(w).Encode(model.Error{Error: "Unauthorized!"})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(model.Details{Details: "Ok"})

		next.ServeHTTP(w, r)
	})
}
