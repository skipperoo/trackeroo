package handler

import (
	"encoding/json"
	"net/http"
	"time"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"

	"go.mongodb.org/mongo-driver/bson"
)

func GetDevices(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	devicesCollection := service.MongoClient.Database("trakeroo-backend").Collection("devices")
	cursor, err := devicesCollection.Find(ctx, bson.M{})
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

func CreateDevice(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	devicesCollection := service.MongoClient.Database("trakeroo-backend").Collection("devices")

	var req model.DeviceCreation
	var device model.Device
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	device.Name = req.Name
	device.Type = req.Type
	device.CreatedAt = time.Now()
	res, err := devicesCollection.InsertOne(ctx, device)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	device.ID = res.InsertedID.(string)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(device)
}

func GetDevice(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	id := r.PathValue("id")
	device, err := service.GetDevice(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(device)
}

func UpdateDevice(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

func DeleteDevice(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	err := service.DeleteDevice(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
