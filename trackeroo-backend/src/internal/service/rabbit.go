// Package service provides access to DB, cache, login validation and redis.
package service

import (
	"context"
	"fmt"
	"strings"
	"time"
	"trackeroo-backend/internal/logger"

	"github.com/streadway/amqp"
)

type ConnectionEvent struct {
	Timestamp time.Time
	User      string
	Name      string
}

func getClientIP(connName string) string {
	parts := strings.Split(connName, "->")
	if len(parts) != 2 {
		return ""
	}
	client := strings.TrimSpace(parts[1])
	ip := strings.Split(client, ":")[0]
	return ip
}

func StartRabbitWatcher(ctx context.Context) {
	go runRabbitWatcher(ctx)
}

func runRabbitWatcher(ctx context.Context) {
	username := GetenvOrDefault("RABBITMQ_USERNAME", "apps")
	password := GetenvOrDefault("RABBITMQ_PASSWORD", "apps")
	host := GetenvOrDefault("RABBITMQ_HOST", "rabbitmq")
	port := GetenvOrDefault("RABBITMQ_PORT", "5672")
	rabbitEndpoint := fmt.Sprintf("amqp://%s:%s@%s:%s/", username, password, host, port)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping RabbitMQ watcher")
			return
		default:
		}

		conn, err := amqp.Dial(rabbitEndpoint)
		if err != nil {
			logger.Error("Cannot connect to RabbitMQ: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		logger.Info("Connected to RabbitMQ")

		ch, err := conn.Channel()
		if err != nil {
			logger.Error("Failed to open channel: %v", err)
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// Declare queue
		q, err := ch.QueueDeclare(
			"device_conn_events",
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,
		)
		if err != nil {
			logger.Error("Queue declare failed: %v", err)
			_ = ch.Close()
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// Bind queue to connection events
		if err := ch.QueueBind(
			q.Name,
			"connection.*",
			"amq.rabbitmq.event",
			false,
			nil,
		); err != nil {
			logger.Error("Queue bind failed: %v", err)
			_ = ch.Close()
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		msgs, err := ch.Consume(
			q.Name,
			"",
			false, // auto-ack
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,
		)
		if err != nil {
			logger.Error("Consume failed: %v", err)
			_ = ch.Close()
			_ = conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		// Process messages until context cancel OR channel closes
	loop:
		for {
			select {
			case <-ctx.Done():
				logger.Info("Context canceled, shutting down RabbitMQ watcher")
				_ = ch.Close()
				_ = conn.Close()
				return
			case msg, ok := <-msgs:
				if !ok {
					logger.Warning("RabbitMQ channel closed, reconnecting...")
					_ = ch.Close()
					_ = conn.Close()
					time.Sleep(5 * time.Second)
					break loop
				}
				msg.Ack(false)

				// Parse and handle event

				user, ok := msg.Headers["user"].(string)
				if !ok {
					continue
				}
				name, ok := msg.Headers["name"].(string)
				if !ok {
					name = ""
				}
				ev := ConnectionEvent{
					Timestamp: msg.Timestamp,
					User:      user,
					Name:      name,
				}

				switch msg.RoutingKey {
				case "connection.created":
					logger.Debug("Device %s connected from %s", ev.User, ev.Name)
					SetDeviceStatus(ctx, ev.User, getClientIP(ev.Name), ev.Timestamp, true)
				case "connection.closed":
					logger.Debug("Device %s disconnected", ev.User)
					SetDeviceStatus(ctx, ev.User, getClientIP(ev.Name), ev.Timestamp, false)
				}
			}
		}
	}
}
