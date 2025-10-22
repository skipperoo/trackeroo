package handler

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"trackeroo-backend/internal/logger"
	"trackeroo-backend/internal/model"
	"trackeroo-backend/internal/service"

	"go.mongodb.org/mongo-driver/mongo"
)

var isUserCache = service.NewSimpleCache()

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
		valid := service.ValidateLogin(ctx, form)
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
	allowedTopics := []string{"data", "up", "dn"}
	ctx := r.Context()
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

	isUserVal := isUserCache.Get(form.Username)
	if isUserVal == nil {
		_, err := service.GetUserByName(ctx, form.Username)
		switch err {
		case mongo.ErrNoDocuments:
			isUserCache.Set(form.Username, false)
			isUserVal = any(false)
		case nil:
			isUserCache.Set(form.Username, true)
			isUserVal = any(true)
		default:
			logger.Error("Cannot retrieve %s: %v", form.Username, err)
			return
		}
	}
	isUser := isUserVal.(bool)

	if isUser {
		logger.Debug("App %s authenticated for topic %s", form.Username, form.Topic)
		fmt.Fprint(w, "allow")
		return
	}
	logger.Debug("Authenticating device %s for topic %s vhost %s resource %s permission %s", form.Username, form.Topic, form.Vhost, form.Resource, form.Permission)
	parts := strings.Split(form.Topic, ".")
	if len(parts) != 4 {
		logger.Debug("Device %s not authenticated for topic %s", form.Username, form.Topic)
		fmt.Fprint(w, "deny")
		return
	}

	topic := parts[1]
	if !slices.Contains(allowedTopics, topic) {
		logger.Debug("Device %s not authenticated for topic %s", form.Username, form.Topic)
		fmt.Fprint(w, "deny")
		return
	}

	devID := parts[2]
	if devID == form.Username {
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
