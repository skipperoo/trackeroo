package handler

import (
	"encoding/json"
	"net/http"
	"time"
	"trackeroo-backend/internal/logger"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"
)

func HandleLogin(w http.ResponseWriter, r *http.Request) {
	var data model.Login
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	logger.Info("Login request by: %+v", data)
	if !service.ValidateLogin(r.Context(), data) {
		w.WriteHeader(http.StatusUnauthorized)

		json.NewEncoder(w).Encode(model.Error{Error: "Unauthorized"})
		return
	}
	user, _ := service.GetUserByName(r.Context(), data.Username)
	jwtToken, claims, err := service.GenerateJWT(user)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	user.LastLogin = time.Now()
	user.SessionToken = jwtToken
	user.ExpiresAt, _ = claims["exp"].(time.Time)
	user.IssuedAt, _ = claims["iat"].(time.Time)
	service.UpdateUser(r.Context(), user)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(model.LoginResponse{Token: jwtToken})
}
