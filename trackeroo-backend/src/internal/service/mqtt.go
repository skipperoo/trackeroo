package service

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"trackeroo-backend/internal/logger"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
)

type TdmClient struct {
	client       pahoMqtt.Client
	broker       string
	heartbeat    int
	port         string
	clientID     string
	password     string
	cleanSession bool
	topicData    string
	running      atomic.Bool
}

func NewTdmClient() *TdmClient {
	username := GetenvOrDefault("MQTT_USERNAME", "apps")
	password := GetenvOrDefault("MQTT_PASSWORD", "apps")
	mqttHost := GetenvOrDefault("MQTT_BROKER", "rabbitmq")
	mqttPort := GetenvOrDefault("MQTT_PORT", "1883")
	tdmClient := new(TdmClient)
	tdmClient.broker = mqttHost
	tdmClient.port = mqttPort
	tdmClient.clientID = username
	tdmClient.password = password
	tdmClient.cleanSession = true
	tdmClient.heartbeat = 10
	tdmClient.topicData = "j/data/"
	return tdmClient
}

func (t *TdmClient) run() {
	t.initClient()
	for t.running.Load() {
		for !t.client.IsConnected() {
			t.initClient()
			err := t.connect()
			if err != nil {
				logger.Error("Cannot connect, %v", err)
				time.Sleep(time.Second * 2)
			}
		}
		qmSize, _ := Size("data")
		if qmSize > 0 {
			id, data, _ := DequeueData("data")
			if str, ok := data.(string); ok {
				devID, tagPayload, _ := strings.Cut(string(str), "/")
				tag, payload, _ := strings.Cut(string(tagPayload), "/")
				for err := t.Publish(devID, tag, payload); err != nil; {
					logger.Error("Cannot publish %v: %v. Retrying...", payload, err)
					time.Sleep(1 * time.Second)
				}
				Ack("data", id)
				logger.Debug("Published %v!", str)
			} else {
				logger.Error("Cannot publish data != string -> %v", data)
				Ack("data", id)
			}

		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (t *TdmClient) connect() error {
	token := t.client.Connect()
	for !token.WaitTimeout(3 * time.Second) {
	}
	return token.Error()
}

func (t *TdmClient) initClient() {
	opts := pahoMqtt.NewClientOptions()
	logger.Debug("Connecting to MQTT broker %s", fmt.Sprintf("mqtt://%s:%s", t.broker, t.port))
	opts.AddBroker(fmt.Sprintf("mqtt://%s:%s", t.broker, t.port))
	opts.SetUsername(t.clientID)
	opts.SetKeepAlive(time.Duration(t.heartbeat) * time.Second)
	opts.SetPingTimeout(time.Duration(t.heartbeat) * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(func(c pahoMqtt.Client, err error) {
		logger.Error("MQTT Connection lost:", err)
	})
	opts.SetOnConnectHandler(func(c pahoMqtt.Client) {
		logger.Info("Connected to the mqtt broker!")
	})
	opts.SetPassword(t.password)
	opts.SetClientID(t.clientID)
	t.client = pahoMqtt.NewClient(opts)
}

func (t *TdmClient) Publish(devID, tag, payload string) error {
	token := t.client.Publish(t.topicData+devID+"/"+tag, byte(1), true, payload)
	token.Wait()
	return token.Error()
}

func (t *TdmClient) Start() {
	t.running.Store(true)
	go t.run()
	logger.Debug("Started MQTT goroutine")
}

func (t *TdmClient) Stop() {
	t.running.Store(false)
	logger.Debug("Stopped MQTT service")
}
