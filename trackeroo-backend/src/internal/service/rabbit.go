// Package service provides access to DB, cache, login validation and redis.
package service

import (
	"trackeroo-backend/internal/logger"

	"github.com/streadway/amqp"
)

var ctrlChan = make(chan struct{})

func StartRabbitWatcher() {
	go runRabbitWatcher()
}

func StopRabbitWatcher() {
	close(ctrlChan)
}

func declareQueue(ch *amqp.Channel) (*amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		"device_conn_events", // queue name
		true,                 // durable
		false,                // auto-delete
		false,                // exclusive
		false,                // no-wait
		nil,                  // args
	)
	if err != nil {
		logger.Error("Queue declare failed: %v", err)
		return nil, err
	}
	return &q, nil
}

func connect() {
	rabbitEndpoint := GetenvOrDefault("RABBITMQ_URL", "amqp://trackeroo:trackeroo@rabbitmq:5673/")
	conn, err := amqp.Dial(rabbitEndpoint)
	if err != nil {
		logger.Error("Cannot connect to rabbit: %v")
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		logger.Error("Failed to open channel: %v", err)
	}
	defer ch.Close()
}

func runRabbitWatcher() {
	for {
		select {
		case <-ctrlChan:
			logger.Info("Stopping rabbit events watcher")
			return
		default:
			// do smt``
		}
	}
}
