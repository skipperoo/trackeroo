package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	MQTTBroker   string
	MQTTClientID string
	MQTTUsername string
	MQTTPassword string
	PostgresURL  string
	TopicPattern string
	KafkaBroker  string
}

type MQTTData struct {
	TSUnix  int64           `json:"ts_unix"`
	DevID   string          `json:"dev_id"`
	Tag     string          `json:"tag"`
	Payload json.RawMessage `json:"payload"`
}

type Service struct {
	config      *Config
	mqttClient  *autopaho.ConnectionManager
	db          *sql.DB
	kafkaWriter *kafka.Writer
	knownTopics map[string]bool
	ctx         context.Context
	cancel      context.CancelFunc
}

type Envelope struct {
	TsUnix      int64           `json:"ts_unix"`
	Ts          string          `json:"ts"`
	DevID       string          `json:"dev_id"`
	Tag         string          `json:"tag"`
	PayloadJSON json.RawMessage `json:"payloadJson"`
}

func NewService(config *Config) *Service {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		config:      config,
		knownTopics: make(map[string]bool),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (s *Service) connectPostgres() error {
	var err error
	s.db, err = sql.Open("postgres", s.config.PostgresURL)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err = s.db.Ping(); err != nil {
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	return nil
}

func (s *Service) connectKafka() error {
	s.kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{s.config.KafkaBroker},
		Async:   true,
	})

	if err := s.ensureTopicExists("health-check"); err != nil {
		log.Printf("failed to create first topic for health-check: %v", err)
	}

	err := s.kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Topic: "health-check",
			Value: []byte("ping"),
		},
	)

	if err != nil {
		log.Printf("Failed to write test message to Kafka: %v", err)
		return err
	}

	log.Println("Successfully connected to Kafka at", s.config.KafkaBroker)
	return nil
}

func (s *Service) ensureTopicExists(topic string) error {
	if s.knownTopics[topic] {
		return nil
	}

	conn, err := kafka.Dial("tcp", s.config.KafkaBroker)
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	if err != nil && !strings.Contains(err.Error(), "Topic with this name already exists") {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}

	s.knownTopics[topic] = true
	log.Printf("Topic ready: %s", topic)
	return nil
}

func (s *Service) connectMQTT() error {
	// Parse broker URL
	serverURL := s.config.MQTTBroker

	// Create autopaho config
	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{{Scheme: "tcp", Host: strings.TrimPrefix(serverURL, "tcp://")}},
		KeepAlive:                     60,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			log.Println("MQTT connected")
			s.subscribeToTopics(cm)
		},
		OnConnectError: func(err error) {
			log.Printf("MQTT connection error: %v", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: s.config.MQTTClientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				s.messageHandler,
			},
			OnClientError: func(err error) {
				log.Printf("MQTT client error: %v", err)
			},
		},
	}

	// Add authentication if provided
	if s.config.MQTTUsername != "" {
		cliCfg.ConnectUsername = s.config.MQTTUsername
		cliCfg.ConnectPassword = []byte(s.config.MQTTPassword)
	}

	// Create connection manager
	cm, err := autopaho.NewConnection(s.ctx, cliCfg)
	if err != nil {
		return fmt.Errorf("failed to create MQTT connection: %w", err)
	}

	s.mqttClient = cm

	// Wait for initial connection
	if err := cm.AwaitConnection(s.ctx); err != nil {
		return fmt.Errorf("failed to await initial MQTT connection: %w", err)
	}

	log.Println("Successfully connected to MQTT broker")
	return nil
}

func (s *Service) subscribeToTopics(cm *autopaho.ConnectionManager) {
	_, err := cm.Subscribe(s.ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				Topic: s.config.TopicPattern,
				QoS:   1,
			},
		},
	})
	if err != nil {
		log.Printf("Failed to subscribe to topics: %v", err)
		return
	}
	log.Printf("Subscribed to topic pattern: %s", s.config.TopicPattern)
}

func (s *Service) messageHandler(pr paho.PublishReceived) (bool, error) {
	topic := pr.Packet.Topic
	payload := pr.Packet.Payload

	log.Printf("Received message on topic %s", topic)

	// Parse topic: j/data/DEVID/TAG
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "j" || parts[1] != "data" {
		log.Printf("Invalid topic format: %s, expected j/data/DEVID/TAG", topic)
		return true, nil // Acknowledge message
	}

	devID := parts[2]
	tag := parts[3]

	// Parse JSON payload
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		log.Printf("Invalid JSON payload for topic %s: %v", topic, err)
		return true, nil
	}

	tsUnix, ok := data["ts"].(int64)
	if !ok {
		tsUnix = time.Now().Unix()
	}

	ts := time.Unix(tsUnix, 0).UTC()
	start := time.Now()
	jsonPayload := json.RawMessage(payload)
	if err := s.insertData(tsUnix, ts, devID, tag, jsonPayload); err != nil {
		log.Printf("Failed to insert data: %v", err)
		return false, err // Don't acknowledge on DB failure
	}
	log.Printf("Time to write to DB %v", time.Since(start))
	start = time.Now()

	// Kafka
	kafkaTopic := sanitizeTopic(topic)
	if err := s.ensureTopicExists(kafkaTopic); err != nil {
		log.Printf("Error creating topic %s: %v", kafkaTopic, err)
		return false, err
	}

	log.Printf("Time to create topic %v", time.Since(start))
	start = time.Now()
	envelope := Envelope{
		TsUnix:      tsUnix,
		Ts:          ts.Format(time.RFC3339),
		DevID:       devID,
		Tag:         tag,
		PayloadJSON: payload,
	}

	envelopeBytes, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("Failed to marshal envelope: %v", err)
		return false, err
	}

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	err = s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: kafkaTopic,
		Value: envelopeBytes,
	})

	if err != nil {
		log.Printf("Failed to write to Kafka: %v", err)
		return false, err
	}
	log.Printf("Message forwarded to Kafka topic: %s", kafkaTopic)
	log.Printf("Time to write to Kafka %v", time.Since(start))

	log.Printf("Successfully inserted data for dev_id: %s, tag: %s", devID, tag)
	return true, nil // Acknowledge message
}

func (s *Service) insertData(tsUnix int64, ts time.Time, devID, tag string, payload json.RawMessage) error {
	query := `INSERT INTO trackeroo.data (ts_unix, ts, dev_id, tag, payload)
              VALUES ($1, $2, $3, $4, $5)
              ON CONFLICT (ts, dev_id) DO NOTHING`
	_, err := s.db.Exec(query, tsUnix, ts, devID, tag, payload)
	if err != nil {
		return fmt.Errorf("failed to insert into database: %w", err)
	}
	return nil
}

func (s *Service) Start() error {
	if err := s.connectPostgres(); err != nil {
		return err
	}

	if err := s.connectMQTT(); err != nil {
		return err
	}

	if err := s.connectKafka(); err != nil {
		return err
	}
	return nil
}

func (s *Service) Stop() {
	log.Println("Shutting down service...")

	s.cancel() // Cancel context to stop MQTT connection

	if s.mqttClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.mqttClient.Disconnect(ctx)
		log.Println("Disconnected from MQTT broker")
	}

	if s.db != nil {
		s.db.Close()
		log.Println("Closed PostgreSQL connection")
	}

	if s.kafkaWriter != nil {
		s.kafkaWriter.Close()
		log.Println("Closed Kafka writer")
	}
}

func loadConfig() *Config {
	return &Config{
		MQTTBroker:   getEnvOrDefault("MQTT_BROKER", "tcp://localhost:1883"),
		MQTTClientID: getEnvOrDefault("MQTT_CLIENT_ID", "brokeroo"),
		MQTTUsername: getEnvOrDefault("MQTT_USERNAME", ""),
		MQTTPassword: getEnvOrDefault("MQTT_PASSWORD", ""),
		PostgresURL:  getEnvOrDefault("POSTGRES_URL", "postgres://user:password@localhost/dbname?sslmode=disable"),
		TopicPattern: getEnvOrDefault("TOPIC_PATTERN", "j/data/+/+"),
		KafkaBroker:  getEnvOrDefault("KAFKA_BROKER", "kafka:9092"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func sanitizeTopic(topic string) string {
	return strings.ReplaceAll(topic, "/", "-")
}

func main() {
	log.Println("Starting MQTT to Kafka and PostgreSQL service...")

	config := loadConfig()
	service := NewService(config)

	if err := service.Start(); err != nil {
		log.Fatalf("Failed to start service: %v", err)
	}

	log.Println("Service started successfully")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	service.Stop()
	log.Println("Service stopped")
}
