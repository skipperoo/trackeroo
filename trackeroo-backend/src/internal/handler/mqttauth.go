package handler

import (
	"fmt"
	"net/http"
	"strings"
	"trackeroo-backend/internal/logger"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"
)

func UserAuth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	/* Every response should be 200 */
	w.WriteHeader(http.StatusOK)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Cannot parse form", http.StatusBadRequest)
		return
	}

	form := model.MqttUserAuthRequest{
		Username: r.FormValue("username"),
		Password: r.FormValue("password"),
	}
	if form.Username == "" || form.Password == "" {
		fmt.Fprint(w, "deny")
		return
	}
	logger.Debug("Authenticating device: %s ***", form.Username)
	_, err := service.GetUserByName(ctx, form.Username)
	if err == nil {
		logger.Info("Found admin user %s, validating...", form.Username)
		login := model.Login{
			Username: form.Username,
			Password: form.Password,
		}
		valid := service.ValidateLogin(ctx, login)
		logger.Debug("Valid: %v", valid)
		if !valid {
			fmt.Fprint(w, "deny")
			return
		}
		fmt.Fprint(w, "allow administrator")
		return
	}
	key, err := service.GetDeviceKey(ctx, form.Username)
	if err != nil {
		fmt.Fprint(w, "deny")
		return
	}
	if res, claims, err := service.ValidateDeviceJWT(key, form.Password); err != nil || !res || claims["sub"] != form.Username {
		fmt.Fprint(w, "deny")
		if err == nil {
			logger.Error("Failed to validate device JWT for %s", form.Username)
		} else if claims["sub"] != form.Username {
			logger.Error("Invalid username in JWT: should be %s, found %s", form.Username, claims["sub"])
		} else {
			logger.Error("Failed to validate device JWT for %s: %v", form.Username, err)
		}
		return
	}
	logger.Debug("Device %s authenticated", form.Username)
	fmt.Fprint(w, "allow")
}

func TopicAuth(w http.ResponseWriter, r *http.Request) {
	/* Every response should be 200 */
	w.WriteHeader(http.StatusOK)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Cannot parse form", http.StatusBadRequest)
		return
	}

	form := model.MqttTopicAuthRequest{
		Username:   r.FormValue("username"),
		Vhost:      r.FormValue("vhost"),
		Resource:   r.FormValue("resource"),
		Topic:      r.FormValue("routing_key"),
		Name:       r.FormValue("name"),
		Permission: r.FormValue("permission"),
	}

	if form.Username == "apps" {
		logger.Debug("App %s authenticated for topic %s", form.Username, form.Topic)
		fmt.Fprint(w, "allow")
		return
	}
	logger.Debug("Authenticating device %s for topic %s vhost %s resource %s permission %s", form.Username, form.Topic, form.Vhost, form.Resource, form.Permission)
	if strings.Contains(form.Topic, form.Username) {
		logger.Debug("Device %s authenticated for topic %s", form.Username, form.Topic)
		fmt.Fprint(w, "allow")
		return
	}
	logger.Debug("Device %s not authenticated for topic %s", form.Username, form.Topic)
	fmt.Fprint(w, "deny")
}

func ResourceAuth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "allow")
}

func VhostAuth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "allow")
}
