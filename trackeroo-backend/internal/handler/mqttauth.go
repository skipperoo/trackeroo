package handler

import (
	"fmt"
	"net/http"
	"strings"
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
	service.Info("Authenticating device: %s %s", form.Username, form.Password)
	key, err := service.GetDeviceKey(ctx, form.Username)
	if err != nil {
		fmt.Fprint(w, "deny")
		return
	}
	if res, claims, err := service.ValidateDeviceJWT(key, form.Password); err != nil || !res || claims["sub"] != form.Username {
		fmt.Fprint(w, "deny")
		if err == nil {
			service.Error("Failed to validate device JWT for %s", form.Username)
		} else if claims["sub"] != form.Username {
			service.Error("Invalid username in JWT: should be %s, found %s", form.Username, claims["sub"])
		} else {
			service.Error("Failed to validate device JWT for %s: %v", form.Username, err)
		}
		return
	}
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
		Name:       r.FormValue("name"),
		Permission: r.FormValue("permission"),
	}

	service.Info("Authenticating device %s for topic %s vhost %s resource %s permission %s", form.Username, form.Name, form.Vhost, form.Resource, form.Permission)
	if strings.Contains(form.Name, form.Username) {
		fmt.Fprint(w, "allow")
		return
	}
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
