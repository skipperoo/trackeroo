package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"
)

func PublishPayload(w http.ResponseWriter, r *http.Request) {
	username := r.Header.Get("Username")
	if username == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(model.Error{Error: "Please use a valid username"})
		return
	}
	topic := r.PathValue("topic")
	if topic != "data" {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(model.Error{Error: "Topic not allowed"})
		return
	}
	tag := r.PathValue("tag")
	if tag == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(model.Error{Error: "Tag cannot be empty"})
		return
	}

	payload, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(model.Error{Error: fmt.Sprintf("Cannot read payload: %v", err)})
		return
	}
	service.EnqueueData("data", username, tag, string(payload))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(model.Details{Details: "ok!"})
}
