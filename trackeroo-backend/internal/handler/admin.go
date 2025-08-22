package handler

import (
	"encoding/json"
	"net/http"
	"trackeroo-backend/internal/model"
)

func NothingToSee(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(model.Details{Details: "Nothing to see here..."})
}
