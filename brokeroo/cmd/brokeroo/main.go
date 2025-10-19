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
	ExchangeName  string
	RoutingKey    string
	PostgresURL   string
	KafkaBroker   string
	PrefetchCount int
	BatchSize     int
	BatchTimeout  time.Duration
	MinPrefetch   int
	MaxPrefetch   int
}

type Service struct {
	config      *Config
	amqpConn    *amqp.Connection
	amqpChannel *amqp.Channel
	db          *sql.DB
	kafkaWriter *kafka.Writer
	knownTopics map[string]bool

	// metrics
	dbLatencies []time.Duration
}

type Envelope struct {
	TsUnix      int64           `json:"ts_unix"`
	Ts          string          `json:"ts"`
	DevID       string          `json:"dev_id"`
	Tag         string          `json:"tag"`
	PayloadJSON json.RawMessage `json:"payloadJson"`
}

type IncomingMessage struct {
	Msg   amqp.Delivery
	DevID string
	Tag   string
	TS    int64
	TSStr string
	Body  json.RawMessage
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

func (s *Service) connectRabbitMQ() error {
	var err error
	s.amqpConn, err = amqp.Dial(s.config.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	s.amqpChannel, err = s.amqpConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// QoS: start with configured prefetch
	if err := s.amqpChannel.Qos(s.config.PrefetchCount, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	return nil
}

func (s *Service) startConsuming(ctx context.Context) error {
	msgs, err := s.amqpChannel.Consume(
		s.config.QueueName, "", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	batch := make([]IncomingMessage, 0, s.config.BatchSize)
	timer := time.NewTimer(s.config.BatchTimeout)

	go func() {
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				incoming, valid := s.parseMessage(msg)
				if !valid {
					msg.Ack(false)
					continue
				}
				batch = append(batch, incoming)

				if len(batch) >= s.config.BatchSize {
					s.processBatch(batch)
					batch = batch[:0]
					if !timer.Stop() {
						<-timer.C
					}
					timer.Reset(s.config.BatchTimeout)
				}
			case <-timer.C:
				if len(batch) > 0 {
					s.processBatch(batch)
					batch = batch[:0]
				}
				timer.Reset(s.config.BatchTimeout)
			}
		}
	}()
	return nil
}

func (s *Service) parseMessage(msg amqp.Delivery) (IncomingMessage, bool) {
	topic := strings.ReplaceAll(msg.RoutingKey, ".", "/")
	parts := strings.Split(topic, "/")
	if len(parts) != 4 {
		return IncomingMessage{}, false
	}

	devID, tag := parts[2], parts[3]
	var data map[string]any
	if err := json.Unmarshal(msg.Body, &data); err != nil {
		return IncomingMessage{}, false
	}

	var tsUnix int64
	if tsFloat, ok := data["ts"].(float64); ok {
		tsUnix = int64(tsFloat)
	} else {
		tsUnix = time.Now().Unix()
	}

	return IncomingMessage{
		Msg:   msg,
		DevID: devID,
		Tag:   tag,
		TS:    tsUnix,
		TSStr: time.Unix(tsUnix, 0).UTC().Format(time.RFC3339),
		Body:  msg.Body,
	}, true
}

func (s *Service) processBatch(batch []IncomingMessage) {
	start := time.Now()
	tx, err := s.db.Begin()
	if err != nil {
		s.nackAll(batch)
		return
	}

	// Build multi-values INSERT
	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]interface{}, 0, len(batch)*5)

	for i, m := range batch {
		valueStrings = append(valueStrings,
			fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
		valueArgs = append(valueArgs, m.TS, m.TSStr, m.DevID, m.Tag, m.Body)
	}

	stmt := fmt.Sprintf(`INSERT INTO trackeroo.data (ts_unix, ts, dev_id, tag, payload)
		VALUES %s ON CONFLICT (ts, dev_id) DO NOTHING`, strings.Join(valueStrings, ","))

	_, err = tx.Exec(stmt, valueArgs...)
	if err != nil {
		_ = tx.Rollback()
		log.Println("Batch insert error:", err)
		s.nackAll(batch)
		return
	}

	if err := tx.Commit(); err != nil {
		s.nackAll(batch)
		return
	}

	// Kafka forward + ack
	for _, m := range batch {
		kafkaTopic := strings.ReplaceAll(m.Msg.RoutingKey, ".", "-")
		_ = s.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Topic: kafkaTopic,
			Value: m.Body,
		})
		m.Msg.Ack(false)
	}

	latency := time.Since(start)
	s.dbLatencies = append(s.dbLatencies, latency)
	if len(s.dbLatencies) > 100 {
		s.dbLatencies = s.dbLatencies[1:]
	}
	log.Printf("✅ batch of %d inserted in %v", len(batch), latency)
}

func (s *Service) nackAll(batch []IncomingMessage) {
	for _, m := range batch {
		m.Msg.Nack(false, true)
	}
}

// Auto prefetch adjuster
func (s *Service) startPrefetchTuner(ctx context.Context) {
	go func() {
		current := s.config.PrefetchCount
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				avgLatency := s.avgDbLatency()
				q, err := s.amqpChannel.QueueInspect(s.config.QueueName)
				if err != nil {
					continue
				}
				if q.Messages > 1000 && avgLatency < 5*time.Millisecond && current < s.config.MaxPrefetch {
					current += 5
				} else if q.Messages < 100 && avgLatency > 20*time.Millisecond && current > s.config.MinPrefetch {
					current -= 5
				}
				if err := s.amqpChannel.Qos(current, 0, false); err == nil {
					log.Printf("Adjusted prefetch=%d (queue=%d, avgDB=%v)", current, q.Messages, avgLatency)
				}
			}
		}
	}()
}

func (s *Service) avgDbLatency() time.Duration {
	if len(s.dbLatencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, l := range s.dbLatencies {
		sum += l
	}
	return sum / time.Duration(len(s.dbLatencies))
}

func loadConfig() *Config {
	return &Config{
		RabbitMQURL:   getEnvOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:     getEnvOrDefault("QUEUE_NAME", "brokeroo"),
		ExchangeName:  getEnvOrDefault("EXCHANGE_NAME", "amq.topic"),
		RoutingKey:    getEnvOrDefault("ROUTING_KEY", "j.data.*.*"),
		PostgresURL:   getEnvOrDefault("POSTGRES_URL", "postgres://user:password@localhost/dbname?sslmode=disable"),
		KafkaBroker:   getEnvOrDefault("KAFKA_BROKER", "kafka:9092"),
		PrefetchCount: getEnvOrDefaultInt("PREFETCH_COUNT", 10),
		BatchSize:     getEnvOrDefaultInt("BATCH_SIZE", 50),
		BatchTimeout:  500 * time.Millisecond,
		MinPrefetch:   5,
		MaxPrefetch:   100,
	}
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var iv int
		if _, err := fmt.Sscanf(v, "%d", &iv); err == nil {
			return iv
		}
	}
	return def
}

func main() {
	config := loadConfig()
	service := NewService(config)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := service.connectPostgres(); err != nil {
		log.Fatal(err)
	}
	if err := service.connectRabbitMQ(); err != nil {
		log.Fatal(err)
	}
	service.kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{config.KafkaBroker},
		Async:   true,
	})

	if err := service.startConsuming(ctx); err != nil {
		log.Fatal(err)
	}
	service.startPrefetchTuner(ctx)

	log.Println("Service running...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	cancel()
}
