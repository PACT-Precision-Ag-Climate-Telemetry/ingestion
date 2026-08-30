// The publishing side: parsed telemetry to the topic exchange, and failures to
// the error queue.

package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"ingest/internal/config"
	"ingest/internal/telemetry"
)

// PublishTelemetry publishes one parsed payload to the telemetry exchange and
// waits for the broker's publisher confirm.
func (c *Client) PublishTelemetry(ctx context.Context, data *telemetry.Data) error {
	if c == nil || c.jsonCh == nil {
		return nil
	}
	if data == nil {
		return fmt.Errorf("telemetry data is nil")
	}

	exchangeName := config.TelemetryExchange()

	c.publishLock.Lock()
	defer c.publishLock.Unlock()

	if err := c.ensureExchangeLocked(exchangeName); err != nil {
		return err
	}

	routingKey := telemetryRoutingKey(data.ID, data.Version)

	payload, err := json.Marshal(data)
	fmt.Printf("%s\n", string(payload))
	if err != nil {
		return err
	}

	if err := c.jsonCh.PublishWithContext(
		ctx,
		exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         payload,
			Timestamp:    time.Now(),
			DeliveryMode: amqp.Persistent,
		},
	); err != nil {
		return err
	}

	if c.jsonConfirm == nil {
		return nil
	}

	select {
	case confirmation, ok := <-c.jsonConfirm:
		if !ok {
			return fmt.Errorf("json payload publish confirmation channel closed")
		}
		if !confirmation.Ack {
			return fmt.Errorf("json payload publish was not acknowledged")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func telemetryRoutingKey(deviceID, version string) string {
	deviceToken := sanitizeToken(deviceID, "unknown")

	versionMajor := strings.TrimSpace(strings.SplitN(version, ".", 2)[0])
	versionToken := sanitizeToken(versionMajor, "0")

	return fmt.Sprintf("pact.telemetry.%s.%s", versionToken, deviceToken)
}

func sanitizeToken(value, fallback string) string {
	token := strings.TrimSpace(value)
	if token == "" {
		token = fallback
	}

	var b strings.Builder
	b.Grow(len(token))
	for _, r := range token {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}

	return b.String()
}

// ensureExchangeLocked declares the telemetry exchange the first time it is
// used. The caller must hold publishLock.
func (c *Client) ensureExchangeLocked(exchangeName string) error {
	if _, exists := c.exchanges[exchangeName]; exists {
		return nil
	}

	if err := c.jsonCh.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	c.exchanges[exchangeName] = struct{}{}
	return nil
}

// PublishError records a failure on the error queue. It is best-effort: callers
// ignore its error because there is nowhere left to report to.
func (c *Client) PublishError(ctx context.Context, source, errText string, payload []byte) error {
	if c == nil || c.errorCh == nil {
		return nil
	}

	message := fmt.Sprintf(`{"source":%q,"error":%q,"payload":%q}`, source, errText, string(payload))

	c.publishLock.Lock()
	defer c.publishLock.Unlock()

	return c.errorCh.PublishWithContext(
		ctx,
		"",
		"error_handler_queue",
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         []byte(message),
			Timestamp:    time.Now(),
			DeliveryMode: amqp.Persistent,
		},
	)
}
