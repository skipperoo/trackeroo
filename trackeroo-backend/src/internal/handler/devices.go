package handler

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"
	"trackeroo-backend/internal/config"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"
)

func GetDevices(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	devices, err := service.GetDevices(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(devices)
}

func CreateDevice(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req model.DeviceCreation
	var device model.Device
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	device.Name = req.Name
	device.DeviceType = req.DeviceType
	device.CreatedAt = time.Now()
	key, err := service.GenPrivateKey()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	device.PrivateKey = key
	dev, err := service.InsertDevice(ctx, device)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(dev)
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

func GetDeviceCredentials(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	params := r.URL.Query()
	secureParameter := params.Get("secure")
	secure := false
	if strings.ToLower(secureParameter) == "true" {
		secure = true
	}
	id := r.PathValue("id")
	creds, err := service.GetDeviceCredentials(ctx, id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	creds.MqttHost = config.Cfg.MqttHost
	if secure {
		creds.MqttPort = config.Cfg.MqttsPort
		creds.MqttMode = "secure"
		caCert, err := os.ReadFile("/certs/ca_certificate.pem")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
			return
		}
		creds.CACert = string(caCert)
	} else {
		creds.MqttPort = config.Cfg.MqttPort
		creds.MqttMode = "insecure"
	}
	device, err := service.GetDevice(ctx, id)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	creds.DeviceType = device.DeviceType

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(creds)
}

func UpdateDevice(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

func DeleteDevice(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	err := service.DeleteDevice(ctx, id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(model.Error{Error: err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
