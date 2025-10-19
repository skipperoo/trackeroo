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

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	RabbitMQURL   string
	QueueName     string
	PostgresURL   string
	KafkaBroker   string
	PrefetchCount int
}

type MQTTData struct {
	TSUnix  int64           `json:"ts_unix"`
	DevID   string          `json:"dev_id"`
	Tag     string          `json:"tag"`
	Payload json.RawMessage `json:"payload"`
}

type Service struct {
	config      *Config
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	db          *sql.DB
	kafkaWriter *kafka.Writer
	knownTopics map[string]bool
}

type Envelope struct {
	TsUnix      int64           `json:"ts_unix"`
	Ts          string          `json:"ts"`
	DevID       string          `json:"dev_id"`
	Tag         string          `json:"tag"`
	PayloadJSON json.RawMessage `json:"payloadJson"`
}

type IncomingMessage struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
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
	log.Printf("Topic pronto: %s", topic)
	return nil
}

func (s *Service) connectRabbitMQ() error {
	var err error

	// Connect with retry logic
	for i := 0; i < 10; i++ {
		s.amqpConn, err = amqp.Dial(s.config.RabbitMQURL)
		if err == nil {
			break
		}
		log.Printf("Failed to connect to RabbitMQ (attempt %d/10): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ after retries: %w", err)
	}

	s.amqpChannel, err = s.amqpConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Declare queue (durable, to survive broker restarts)
	_, err = s.amqpChannel.QueueDeclare(
		s.config.QueueName, // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Set QoS (prefetch count) for load balancing
	// This ensures each worker only gets N messages at a time
	err = s.amqpChannel.Qos(
		s.config.PrefetchCount, // prefetch count
		0,                      // prefetch size
		false,                  // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	log.Printf("Successfully connected to RabbitMQ, queue: %s", s.config.QueueName)
	return nil
}

func (s *Service) startConsuming(ctx context.Context) error {
	msgs, err := s.amqpChannel.Consume(
		s.config.QueueName, // queue
		"",                 // consumer tag (auto-generated)
		false,              // auto-ack (IMPORTANT: false for load balancing)
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Println("Started consuming messages from RabbitMQ")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					log.Println("Message channel closed")
					return
				}
				s.handleMessage(msg)
			}
		}
	}()

	return nil
}

func (s *Service) handleMessage(delivery amqp.Delivery) {
	// Parse the incoming message
	var incoming IncomingMessage
	if err := json.Unmarshal(delivery.Body, &incoming); err != nil {
		log.Printf("Invalid JSON payload: %v", err)
		delivery.Ack(false)
		return
	}

	topic := incoming.Topic
	payload := incoming.Payload

	log.Printf("Received message on topic %s", topic)

	// Parse topic: j/data/DEVID/TAG
	parts := strings.Split(topic, "/")
	if len(parts) != 4 || parts[0] != "j" || parts[1] != "data" {
		log.Printf("Invalid topic format: %s, expected j/data/DEVID/TAG", topic)
		delivery.Ack(false)
		return
	}

	devID := parts[2]
	tag := parts[3]

	// Parse JSON payload
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		log.Printf("Invalid JSON payload for topic %s: %v", topic, err)
		delivery.Ack(false)
		return
	}

	// Extract timestamp
	var tsUnix int64
	if tsFloat, ok := data["ts"].(float64); ok {
		tsUnix = int64(tsFloat)
	} else {
		tsUnix = time.Now().Unix()
	}

	ts := time.Unix(tsUnix, 0).UTC()

	// Insert into PostgreSQL
	start := time.Now()
	if err := s.insertData(tsUnix, ts, devID, tag, payload); err != nil {
		log.Printf("Failed to insert data: %v", err)
		// Negative acknowledgment with requeue
		delivery.Nack(false, true)
		return
	}
	log.Printf("Time to write to DB %v", time.Since(start))

	// Forward to Kafka
	start = time.Now()
	kafkaTopic := sanitizeTopic(topic)
	if err := s.ensureTopicExists(kafkaTopic); err != nil {
		log.Printf("Error creating topic %s: %v", kafkaTopic, err)
		delivery.Nack(false, true)
		return
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
		delivery.Nack(false, true)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = s.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Topic: kafkaTopic,
		Value: envelopeBytes,
	})

	if err != nil {
		log.Printf("Failed to write to Kafka: %v", err)
		delivery.Nack(false, true)
		return
	}

	log.Printf("Message forwarded to Kafka topic: %s", kafkaTopic)
	log.Printf("Time to write to Kafka %v", time.Since(start))
	log.Printf("Successfully processed data for dev_id: %s, tag: %s", devID, tag)

	// Acknowledge message only after successful processing
	delivery.Ack(false)
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

func (s *Service) Start(ctx context.Context) error {
	if err := s.connectPostgres(); err != nil {
		return err
	}

	if err := s.connectRabbitMQ(); err != nil {
		return err
	}

	if err := s.connectKafka(); err != nil {
		return err
	}

	if err := s.startConsuming(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Service) Stop() {
	log.Println("Shutting down service...")

	if s.amqpChannel != nil {
		s.amqpChannel.Close()
		log.Println("Closed RabbitMQ channel")
	}

	if s.amqpConn != nil {
		s.amqpConn.Close()
		log.Println("Closed RabbitMQ connection")
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
		RabbitMQURL:   getEnvOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:     getEnvOrDefault("QUEUE_NAME", "brokeroo"),
		PostgresURL:   getEnvOrDefault("POSTGRES_URL", "postgres://user:password@localhost/dbname?sslmode=disable"),
		KafkaBroker:   getEnvOrDefault("KAFKA_BROKER", "kafka:9092"),
		PrefetchCount: getEnvOrDefaultInt("PREFETCH_COUNT", 1),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intVal int
		if _, err := fmt.Sscanf(value, "%d", &intVal); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func sanitizeTopic(topic string) string {
	return strings.ReplaceAll(topic, "/", "-")
}

func main() {
	log.Println("Starting RabbitMQ to Kafka and PostgreSQL service...")

	config := loadConfig()
	service := NewService(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := service.Start(ctx); err != nil {
		log.Fatalf("Failed to start service: %v", err)
	}

	log.Println("Service started successfully")

	// Wait for interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	cancel()
	service.Stop()
	log.Println("Service stopped")
}
