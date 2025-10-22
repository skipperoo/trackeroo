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
	"sync"
	"sync/atomic"
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
	mu          sync.RWMutex

	// metrics
	dbLatencies      []time.Duration
	saturatedBatches atomic.Int64
	timeoutBatches   atomic.Int64
	totalBatches     atomic.Int64
}

type Envelope struct {
	Msg         amqp.Delivery   `json:"-"`
	TsUnix      int64           `json:"ts_unix"`
	Ts          string          `json:"ts"`
	DevID       string          `json:"dev_id"`
	Tag         string          `json:"tag"`
	PayloadJSON json.RawMessage `json:"payloadJson"`
}

// type Envelope struct {
// 	Msg   amqp.Delivery
// 	DevID string
// 	Tag   string
// 	TS    int64
// 	TSStr string
// 	Body  json.RawMessage
// }

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
	log.Printf("topic ready: %s", topic)
	return nil
}

func (s *Service) connectKafka() error {
	s.kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{s.config.KafkaBroker},
		Async:   true,
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
		log.Printf("failed to write test message to Kafka: %v", err)
		return err
	}

	log.Println("Successfully connected to Kafka at", s.config.KafkaBroker)
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
	args := amqp.Table{
		"x-queue-type": "quorum",
	}
	q, err := s.amqpChannel.QueueDeclare(
		s.config.QueueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		args,
	)
	if err != nil {
		_ = s.amqpChannel.Close()
		_ = s.amqpConn.Close()
		time.Sleep(5 * time.Second)
		return fmt.Errorf("Queue declare failed: %v", err)
	}
	err = s.amqpChannel.QueueBind(
		q.Name,                // queue name
		s.config.RoutingKey,   // routing key (use "" for fanout exchanges)
		s.config.ExchangeName, // exchange name
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		_ = s.amqpChannel.Close()
		_ = s.amqpConn.Close()
		return fmt.Errorf("failed to bind queue: %w", err)
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

	batch := make([]Envelope, 0, s.config.BatchSize)
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

func (s *Service) parseMessage(msg amqp.Delivery) (Envelope, bool) {
	topic := strings.ReplaceAll(msg.RoutingKey, ".", "/")
	parts := strings.Split(topic, "/")
	if len(parts) != 4 {
		return Envelope{}, false
	}

	devID, tag := parts[2], parts[3]
	var data map[string]any
	if err := json.Unmarshal(msg.Body, &data); err != nil {
		return Envelope{}, false
	}

	var tsUnix int64
	if tsFloat, ok := data["ts"].(float64); ok {
		tsUnix = int64(tsFloat)
	} else {
		tsUnix = time.Now().Unix()
	}
	ts := time.Unix(tsUnix, 0).UTC()

	return Envelope{
		Msg:         msg,
		DevID:       devID,
		TsUnix:      tsUnix,
		Tag:         tag,
		Ts:          ts.Format(time.RFC3339),
		PayloadJSON: msg.Body,
	}, true
}

func (s *Service) processBatch(batch []Envelope) {
	if len(batch) >= s.config.BatchSize {
		s.saturatedBatches.Add(1)
	} else {
		s.timeoutBatches.Add(1)
	}
	s.totalBatches.Add(1)
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
		valueArgs = append(valueArgs, m.TsUnix, m.Ts, m.DevID, m.Tag, m.PayloadJSON)
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
		kafkaTopic := sanitizeTopic(m.Msg.RoutingKey)
		if err := s.ensureTopicExists(kafkaTopic); err != nil {
			log.Printf("error creating topic %s: %v", kafkaTopic, err)
			s.nackAll(batch)
			return
		}
		envelopeBytes, err := json.Marshal(m)
		if err != nil {
			log.Printf("failed to marshal envelope: %v", err)
			s.nackAll(batch)
			return
		}
		_ = s.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Topic: kafkaTopic,
			Value: envelopeBytes,
		})
		m.Msg.Ack(false)
	}

	latency := time.Since(start)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dbLatencies = append(s.dbLatencies, latency)
	if len(s.dbLatencies) > 100 {
		s.dbLatencies = s.dbLatencies[1:]
	}
	// log.Printf("batch of %d inserted in %v", len(batch), latency)
}

func (s *Service) nackAll(batch []Envelope) {
	for _, m := range batch {
		m.Msg.Nack(false, true)
	}
}

// Auto prefetch adjuster
func (s *Service) startPrefetchTuner(ctx context.Context) {
	go func() {
		current := s.config.PrefetchCount
		last := s.config.PrefetchCount
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				avgLatency := s.avgDbLatency()
				batchFillRate := s.getBatchFillRate() // New metric

				args := amqp.Table{"x-queue-type": "quorum"}
				q, err := s.amqpChannel.QueueDeclarePassive(
					s.config.QueueName, true, false, false, false, args,
				)
				if err != nil {
					continue
				}

				current = s.calculateOptimalPrefetch(
					avgLatency,
					batchFillRate,
					current,
				)

				if last == current {
					continue
				}

				if err := s.amqpChannel.Qos(current, 0, false); err == nil {
					log.Printf(">>> adjusted prefetch=%d (queue=%d, db_latency=%v, batch_fill=%.2f%%)", current, q.Messages, avgLatency, batchFillRate*100)
					last = current
				}
			}
		}
	}()
}

func (s *Service) calculateOptimalPrefetch(
	avgLatency time.Duration,
	batchFillRate float64,
	current int,
) int {

	if batchFillRate > 0.95 && current > s.config.MinPrefetch {
		return max(current-5, s.config.MinPrefetch)
	}

	if batchFillRate < 0.5 && avgLatency < 10*time.Millisecond &&
		current < s.config.MaxPrefetch {
		return min(current+5, s.config.MaxPrefetch)
	}

	if avgLatency > 25*time.Millisecond && current > s.config.MinPrefetch {
		return max(current-5, s.config.MinPrefetch)
	}

	return current
}

func (s *Service) getBatchFillRate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.totalBatches.Load() == 0 {
		return 0
	}
	return float64(s.saturatedBatches.Load()) / float64(s.totalBatches.Load())
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

func sanitizeTopic(topic string) string {
	return strings.ReplaceAll(topic, ".", "-")
}

func loadConfig() *Config {
	batchSize := getEnvOrDefaultInt("BATCH_SIZE", 50)
	config := &Config{
		RabbitMQURL:   getEnvOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:     getEnvOrDefault("QUEUE_NAME", "brokeroo"),
		ExchangeName:  getEnvOrDefault("EXCHANGE_NAME", "amq.topic"),
		RoutingKey:    getEnvOrDefault("ROUTING_KEY", "j.data.*.*"),
		PostgresURL:   getEnvOrDefault("POSTGRES_URL", "postgres://user:password@localhost/dbname?sslmode=disable"),
		KafkaBroker:   getEnvOrDefault("KAFKA_BROKER", "kafka:9092"),
		PrefetchCount: batchSize * 3,
		BatchSize:     batchSize,
		BatchTimeout:  time.Duration(getEnvOrDefaultInt("BATCH_TIMEOUT_MS", 100)) * time.Millisecond,
		MinPrefetch:   batchSize * 2,
		MaxPrefetch:   batchSize * 4,
	}
	log.Printf("config -> (queue_name=%s, exchange_name=%s, routing_key=%s, prefetch_count=%d, batch_size=%d)", config.QueueName, config.ExchangeName, config.RoutingKey, config.PrefetchCount, config.BatchSize)
	return config
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
