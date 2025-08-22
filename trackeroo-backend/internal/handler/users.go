package handler

import (
	"encoding/json"
	"net/http"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"

	"go.mongodb.org/mongo-driver/bson"
)

func GetUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	usersCollection := service.MongoClient.Database("trakeroo-backend").Collection("users")
	cursor, err := usersCollection.Find(ctx, bson.M{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var devices []model.Device
	if err := cursor.All(ctx, &devices); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(devices)
}

func CreateUser(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

func GetUser(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	id := r.PathValue("id")
	device, err := service.GetUser(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(device)
}

func UpdateUser(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

func DeleteUser(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	err := service.DeleteUser(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
