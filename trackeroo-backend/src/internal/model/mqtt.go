package model

type (
	MqttUserAuthRequest  = Login
	MqttTopicAuthRequest struct {
		Username   string `json:"username"`
		Vhost      string `json:"vhost"`
		Resource   string `json:"resource"`
		Topic      string `json:"topic"`
		Name       string `json:"name"`
		Permission string `json:"permission"`
	}
)
