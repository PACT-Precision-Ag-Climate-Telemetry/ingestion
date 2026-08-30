// Package rabbit owns the RabbitMQ connection: topology setup, the consumer
// loop and its rejection paths, and publishing of telemetry and errors.
package rabbit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"ingest/internal/telemetry"
)

// Client holds the broker connection and the three channels the service uses:
// one for consuming, one for publishing telemetry under publisher confirms, and
// one for publishing errors.
type Client struct {
	conn        *amqp.Connection
	consumeCh   *amqp.Channel
	jsonCh      *amqp.Channel
	jsonConfirm chan amqp.Confirmation
	errorCh     *amqp.Channel
	exchanges   map[string]struct{}
	publishLock sync.Mutex
	wg          sync.WaitGroup
}

// Connect dials the broker, declares the queue topology, and starts the
// consumer for queueName. The consumer runs until ctx is cancelled or the
// deliveries channel closes; Close waits for it to finish.
func Connect(ctx context.Context, url, queueName string) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	errorChannel, err := conn.Channel()
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, err
	}

	jsonChannel, err := conn.Channel()
	if err != nil {
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = conn.Close()
		return nil, err
	}

	// closeAll unwinds everything opened so far. The original code repeated
	// these four Close calls at each failure point.
	closeAll := func() {
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = jsonChannel.Close()
		_ = conn.Close()
	}

	exists, err := queueExists(conn, queueName)
	if err != nil {
		closeAll()
		return nil, err
	}

	if !exists {
		_, err = channel.QueueDeclare(
			queueName,
			true,
			false,
			false,
			false,
			amqp.Table{
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": "failed_messages_queue",
			},
		)
		if err != nil {
			closeAll()
			return nil, err
		}
	} else {
		fmt.Printf("RabbitMQ queue %q already exists; keeping existing queue arguments as-is\n", queueName)
	}

	_, err = channel.QueueDeclare(
		"failed_messages_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		closeAll()
		return nil, err
	}

	_, err = errorChannel.QueueDeclare(
		"error_handler_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		closeAll()
		return nil, err
	}

	if err := jsonChannel.Confirm(false); err != nil {
		closeAll()
		return nil, err
	}

	jsonConfirmations := jsonChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

	deliveries, err := channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		closeAll()
		return nil, err
	}

	client := &Client{
		conn:        conn,
		consumeCh:   channel,
		jsonCh:      jsonChannel,
		jsonConfirm: jsonConfirmations,
		errorCh:     errorChannel,
		exchanges:   make(map[string]struct{}),
	}

	client.wg.Add(1)
	go func() {
		defer client.wg.Done()
		client.consume(ctx, queueName, deliveries)
	}()

	return client, nil
}

func (c *Client) consume(ctx context.Context, queueName string, deliveries <-chan amqp.Delivery) {
	fmt.Println("RabbitMQ consumer running on queue", queueName)
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				fmt.Println("RabbitMQ deliveries closed")
				return
			}

			if rejected, err := c.rejectMalformedJSON(ctx, delivery, "rabbitmq_consumer"); err != nil {
				fmt.Println("Error rejecting malformed JSON message:", err)
				continue
			} else if rejected {
				continue
			}

			if _, err := telemetry.Parse(delivery.Body); err != nil {
				fmt.Println("Error processing RabbitMQ message:", err)
				if rejectErr := c.rejectDelivery(ctx, delivery, "rabbitmq_consumer", err); rejectErr != nil {
					fmt.Println("Error rejecting RabbitMQ message:", rejectErr)
				}
				continue
			}

			if err := delivery.Ack(false); err != nil {
				fmt.Println("Error acknowledging RabbitMQ message:", err)
				_ = c.PublishError(ctx, "rabbitmq_ack", err.Error(), delivery.Body)
			}
		}
	}
}

// Close shuts the channels and connection down, then waits for the consumer
// goroutine to return.
func (c *Client) Close() {
	if c == nil {
		return
	}

	if c.consumeCh != nil {
		_ = c.consumeCh.Close()
	}
	if c.jsonCh != nil {
		_ = c.jsonCh.Close()
	}
	if c.errorCh != nil {
		_ = c.errorCh.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}

	c.wg.Wait()
}

func queueExists(conn *amqp.Connection, queueName string) (bool, error) {
	probeCh, err := conn.Channel()
	if err != nil {
		return false, err
	}
	defer func() {
		_ = probeCh.Close()
	}()

	_, err = probeCh.QueueDeclarePassive(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err == nil {
		return true, nil
	}

	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) && amqpErr.Code == 404 {
		return false, nil
	}

	return false, err
}

// rejectMalformedJSON checks JSON-only deliveries and rejects malformed
// payloads with requeue=false so they can be dead-lettered.
func (c *Client) rejectMalformedJSON(ctx context.Context, delivery amqp.Delivery, source string) (bool, error) {
	if !strings.EqualFold(strings.TrimSpace(delivery.ContentType), "application/json") {
		return false, nil
	}

	if json.Valid(delivery.Body) {
		return false, nil
	}

	if err := c.rejectDelivery(ctx, delivery, source, fmt.Errorf("malformed JSON payload")); err != nil {
		return true, err
	}

	return true, nil
}

func (c *Client) rejectDelivery(ctx context.Context, delivery amqp.Delivery, source string, cause error) error {
	if cause != nil {
		if err := c.PublishError(ctx, source, cause.Error(), delivery.Body); err != nil {
			return fmt.Errorf("publishing rejection error payload: %w", err)
		}
	}

	if err := delivery.Reject(false); err != nil {
		return fmt.Errorf("basic.reject failed: %w", err)
	}

	return nil
}
