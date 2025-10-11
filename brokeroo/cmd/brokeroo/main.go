package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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
	mqttClient  mqtt.Client
	db          *sql.DB
	kafkaWriter *kafka.Writer
	knownTopics map[string]bool /* for caching kafka topics  */
}

type Envelope struct {
	TsUnix      int64           `json:"ts_unix"`
	Ts          string          `json:"ts"`
	DevID       string          `json:"dev_id"`
	Tag         string          `json:"tag"`
	PayloadJSON json.RawMessage `json:"payloadJson"`
}

func NewService(config *Config) *Service {
	return &Service{
		config:      config,
		knownTopics: make(map[string]bool),
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
		Brokers:      []string{s.config.KafkaBroker},
		BatchSize:    1,
		BatchTimeout: 5 * time.Millisecond,
	})

	if err := s.ensureTopicExists("health-check"); err != nil {
		log.Printf("failed to create first topic for healt-check: %v", err)
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
	log.Printf("Topic pronto: %s", topic)
	return nil
}

func (s *Service) connectMQTT() error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.config.MQTTBroker)
	opts.SetClientID(s.config.MQTTClientID)
	opts.SetUsername(s.config.MQTTUsername)
	opts.SetPassword(s.config.MQTTPassword)
	opts.SetCleanSession(false) // Keep unacked messages
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetPingTimeout(10 * time.Second)
	opts.SetConnectTimeout(10 * time.Second)

	// Set connection lost handler
	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	})

	// Set reconnect handler
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("MQTT connected/reconnected")
		s.subscribeToTopics()
	})

	s.mqttClient = mqtt.NewClient(opts)

	c := 0
	for {
		if token := s.mqttClient.Connect(); token.Wait() && token.Error() != nil {
			if c == 10 {
				return fmt.Errorf("failed to connect to MQTT broker: %w", token.Error())
			}
			c += 1
			log.Println("Connessione fallita, retry tra 2s:", token.Error())
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}

	log.Println("Successfully connected to MQTT broker")
	return nil
}

func (s *Service) subscribeToTopics() {
	token := s.mqttClient.Subscribe(s.config.TopicPattern, 1, s.messageHandler)
	if token.Wait() && token.Error() != nil {
		log.Printf("Failed to subscribe to topics: %v", token.Error())
		return
	}
	log.Printf("Subscribed to topic pattern: %s", s.config.TopicPattern)
}

func (s *Service) messageHandler(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := msg.Payload()

	log.Printf("Received message on topic %s", topic)

	// Parse topic: j/data/DEVID/TAG
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "j" || parts[1] != "data" {
		log.Printf("Invalid topic format: %s, expected j/data/DEVID/TAG", topic)
		return
	}

	devID := parts[2]
	tag := parts[3]

	// Parse JSON payload
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		log.Printf("Invalid JSON payload for topic %s: %v", topic, err)
		return
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
		return
	}
	log.Printf("Time to write to DB %v", time.Since(start))
	start = time.Now()
	/* kafka */
	kafkaTopic := sanitizeTopic(topic)
	if err := s.ensureTopicExists(kafkaTopic); err != nil {
		log.Printf("Errore creazione topic %s: %v", kafkaTopic, err)
		return
	} else {
		log.Printf("Time to create topic %v", time.Since(start))
		start = time.Now()
		envelope := Envelope{
			TsUnix:      tsUnix,
			Ts:          ts.Format(time.RFC3339), // is this right?
			DevID:       devID,
			Tag:         tag,
			PayloadJSON: payload, // il payload originale come stringa JSON
		}

		envelopeBytes, err := json.Marshal(envelope)
		if err != nil {
			log.Printf("Failed to marshal envelope: %v", err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) /* 5 second timeout */
		defer cancel()

		err = s.kafkaWriter.WriteMessages(ctx, kafka.Message{
			Topic: kafkaTopic,
			Value: envelopeBytes,
		})

		if err != nil {
			log.Printf("Failed to write to Kafka: %v", err)
		} else {
			log.Printf("Message forwarded to Kafka topic: %s", kafkaTopic)
		}
		log.Printf("Time to write to Kafka %v", time.Since(start))
	}

	log.Printf("Successfully inserted data for dev_id: %s, tag: %s", devID, tag)
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
	// Connect to PostgreSQL
	if err := s.connectPostgres(); err != nil {
		return err
	}

	// Connect to MQTT
	if err := s.connectMQTT(); err != nil {
		return err
	}

	// Connect to Kafka
	if err := s.connectKafka(); err != nil {
		return err
	}
	return nil
}

func (s *Service) Stop() {
	log.Println("Shutting down service...")

	if s.mqttClient != nil && s.mqttClient.IsConnected() {
		s.mqttClient.Unsubscribe(s.config.TopicPattern)
		s.mqttClient.Disconnect(250)
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

	// Wait for interrupt signal to gracefully shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	service.Stop()
	log.Println("Service stopped")
}
