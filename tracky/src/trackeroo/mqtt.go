package trackeroo

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	JOB     string = "@"
	REQUEST string = "#"
	VAR     string = "__"
)

type TdmClient struct {
	Task
	client       pahoMqtt.Client
	broker       string
	heartbeat    int
	port         int
	mode         string
	clientId     string
	cleanSession bool
	caCert       string
	topicUp      string
	topicDn      string
	topicData    string
	creds        Credentials
}

func NewTdmClient() *TdmClient {
	credentials, err := GetCredentials()
	if err != nil {
		Error("Cannot read credentials: %v", err)
	}
	tdmClient := new(TdmClient)
	tdmClient.creds = *credentials
	tdmClient.broker = credentials.MqttHost
	tdmClient.port = credentials.MqttPort
	tdmClient.mode = credentials.MqttMode
	err = CreateCaFile()
	if err != nil {
		Error("Cannot read credentials: %v", err)
	}
	tdmClient.caCert = "credentials/cacert.pem"
	tdmClient.clientId = credentials.ID
	tdmClient.cleanSession = true
	tdmClient.heartbeat = 10
	tdmClient.topicUp = "j/up/" + tdmClient.clientId
	tdmClient.topicDn = "j/dn/" + tdmClient.clientId
	tdmClient.topicData = "j/data/" + tdmClient.clientId
	tdmClient.name = "MQTT"
	tdmClient.kicked = UnixTime()
	tdmClient.wtd = 120

	return tdmClient
}

func (t *TdmClient) handleJobRequest(jname string, args map[string]any) {
	if fn, ok := Jobs[jname]; ok {
		go func() {
			ret := fn(args)
			t.replyJob(jname, ret)
		}()
	} else {
		t.replyJob(jname, map[string]any{"error": fmt.Sprintf("Cannot find job %s", jname)})
	}
}

func (t *TdmClient) handleDnMsg(client pahoMqtt.Client, msg pahoMqtt.Message) {
	payload := msg.Payload()
	Debug("Message received -> [%s] %s", msg.Topic(), string(payload))
	var data map[string]any
	err := json.Unmarshal(payload, &data)
	if err != nil {
		Error("Cannot unmarshal %v: %v", payload, err)
		return
	}

	if key, ok := data["key"].(string); ok {
		switch {
		case key[0] == '#':
			switch key {
			case "#status":
				if value, ok := data["value"].(map[string]any); ok {
					if expected, ok := value["expected"].(map[string]any); ok {
						for k := range expected {
							if k[0] == '@' {
								j, _ := expected[k].(map[string]any)
								v, ok := j["v"].(map[string]any)
								if !ok {
									Error("Missing v")
									t.replyJob(k[1:], map[string]any{"error": "Missing args"})
									return
								}
								args, ok := v["args"].(map[string]any)
								if !ok {
									Error("Missing args")
									t.replyJob(k[1:], map[string]any{"error": "Missing args"})
									return
								}
								t.handleJobRequest(k[1:], args)
							}
						}
					}
				}
			case "#now":
				Debug("Now: %v", data)
			}
		case key[0] == '@':
			value, ok := data["value"].(map[string]any)
			if !ok {
				Error("Missing value")
				t.replyJob(key[1:], map[string]any{"error": "Missing args"})
			}
			args, _ := value["args"].(map[string]any)
			t.handleJobRequest(key[1:], args)
		}
	}
}

func (t *TdmClient) run() {
	t.initClient()
	t.running = true
	// fmt.Printf("%+v", z)
	for t.running {
		for !t.client.IsConnected() {
			t.initClient()
			err := t.connect()
			if err != nil {
				Error("Cannot connect, %v", err)
				Millisleep(2000)
			}
		}
		t.Kick()
		Millisleep(1000)
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
	if t.mode == "secure" {
		opts.AddBroker(fmt.Sprintf("mqtts://%s:%s", t.broker, strconv.Itoa(t.port)))
		certpool := x509.NewCertPool()
		ca, err := os.ReadFile(t.caCert)
		if err != nil {
			Error("Cannot open %s", t.caCert)
		}
		certpool.AppendCertsFromPEM(ca)
		tlsConfig := tls.Config{
			RootCAs: certpool,
		}
		opts.SetTLSConfig(&tlsConfig)
	} else {
		opts.AddBroker(fmt.Sprintf("mqtt://%s:%s", t.broker, strconv.Itoa(t.port)))
	}
	opts.SetUsername(t.clientId)
	opts.SetKeepAlive(time.Duration(t.heartbeat) * time.Second)
	opts.SetPingTimeout(time.Duration(t.heartbeat) * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(func(c pahoMqtt.Client, err error) {
		Error("MQTT Connection lost:", err)
	})
	opts.SetOnConnectHandler(func(c pahoMqtt.Client) {
		Info("Connected!")
		// t.client.Subscribe(t.topicDn, byte(1), t.handleDnMsg)
		// t.requestStatus()
		// t.requestTime()
		// t.sendOsInfo()
		// t.sendManifest()
	})
	token, err := GetToken(t.creds.PrivateKey, 200, 200, t.creds.ID)
	// fmt.Println(token)
	if err != nil {
		Error("Cannot create token, %v", err)
	}
	opts.SetPassword(token)
	opts.SetClientID(t.clientId)
	t.client = pahoMqtt.NewClient(opts)
}

func (t *TdmClient) Publish(tag string, payload string) error {
	token := t.client.Publish(t.topicData+"/"+tag, byte(1), true, payload)
	token.Wait()
	return token.Error()
}

func (t *TdmClient) Start() {
	go t.run()
	Debug("Started MQTT goroutine")
}

func (t *TdmClient) up(tag string, payload map[string]any, prefix string) {
	value := map[string]any{
		"key":   prefix + tag,
		"value": payload,
	}
	jsonValue, err := json.Marshal(value)
	if err != nil {
		Error("Cannot marshal %v %v", jsonValue, err)
		return
	}
	t.client.Publish(t.topicUp, byte(1), true, string(jsonValue))
	Debug("Sent message -> %v", string(jsonValue))
}

func (t *TdmClient) requestStatus() {
	payload := map[string]any{}
	t.request("status", payload)
}

func (t *TdmClient) requestTime() {
	payload := map[string]any{}
	t.request("now", payload)
}

func (t *TdmClient) sendOsInfo() {
	value := map[string]any{
		"target": "UNKOWN",
		"fw":     "Factory",
		"zfs":    "Factory",
		"boot":   1,
		"os":     "v0.0.1-alpha-go",
	}
	t.up("info", value, VAR)
}

func (t *TdmClient) sendManifest() {
	jobs := []string{
		"reset",
		"restart",
	}
	for k := range Jobs {
		jobs = append(jobs, k)
	}
	value := map[string]any{
		"jobs": jobs,
	}
	t.up("manifest", value, VAR)
}

func (t *TdmClient) replyJob(key string, value map[string]any) {
	t.up(key, value, JOB)
}

func (t *TdmClient) request(key string, value map[string]any) {
	t.up(key, value, REQUEST)
}
