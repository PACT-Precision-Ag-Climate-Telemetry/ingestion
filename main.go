package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Header layout (17 bytes):
//   0-1   magic bytes
//   2     version major
//   3     version minor
//   4     version patch
//   5-14  ID (10 bytes)
//   15    flags
//   16    record count
const headerSize = 17

// Per-record fixed portion (everything except the leading time field):
// CO2, ch4_raw, ch4_ppm, level, distance, moisture_raw, moisture_pct,
// batt_v, batt_p (uint16 each = 18 bytes) + status + error (1 byte each) = 20 bytes.
const recordFixedSize = 20

// Older records are prefixed with a 2-byte timedif; the newest record is
// prefixed with a full 8-byte absolute timestamp instead.
const oldRecordSize = 2 + recordFixedSize // 22 bytes
const newestRecordSize = 8 + recordFixedSize // 28 bytes

// Optional trailing blocks, gated by bits in the header flags byte.
const gpsBlockSize = 8  // 4-byte latitude + 4-byte longitude
const cellBlockSize = 4 // 2-byte MCC + 2-byte MNC

const telemetryJSONExchange = "pact.telemetry"

// Flag bit assignments within the header flags byte.
// NOTE: bit positions are an assumption based on the protocol diagram
// ("v0 w/GPS w/Batt") - adjust here if the device firmware disagrees.
const (
	FlagGPSPresent  byte = 1 << 0
	FlagCellPresent byte = 1 << 1
)

// TelemetryReading is a single sample within a telemetry payload. Payloads
// carry multiple readings (oldest to newest) in one message.
type TelemetryReading struct {
	Timestamp         uint64 `json:"timestamp"`
	CarbonDioxide     uint16 `json:"carbon_dioxide"`
	MethaneRaw        uint16 `json:"methane_raw"`
	Methane           uint16 `json:"methane"`
	Level             uint16 `json:"level"`
	Distance          uint16 `json:"distance"`
	MoistureRaw       uint16 `json:"moisture_raw"`
	Moisture          uint16 `json:"moisture"`
	BatteryVoltage    uint16 `json:"battery_voltage"`
	BatteryPercentage uint16 `json:"battery_percentage"`
	Status            byte   `json:"status"`
	ErrorCode         byte   `json:"error_code"`
}

type TelemetryData struct {
	ID                string             `json:"id"`
	Version           string             `json:"version"`
	Flags             byte               `json:"flags"`
	Readings          []TelemetryReading `json:"readings"` // oldest to newest
	HasGPS            bool               `json:"has_gps"`
	Latitude          float32            `json:"latitude,omitempty"`
	Longitude         float32            `json:"longitude,omitempty"`
	HasCell           bool               `json:"has_cell"`
	MobileCountryCode uint16             `json:"mobile_country_code,omitempty"`
	MobileNetworkCode uint16             `json:"mobile_network_code,omitempty"`
}

type Server struct {
	listener          net.Listener
	rabbitConn        *amqp.Connection
	rabbitCh          *amqp.Channel
	rabbitJSONCh      *amqp.Channel
	rabbitJSONConfirm chan amqp.Confirmation
	rabbitErrorCh     *amqp.Channel
	rabbitExchanges   map[string]struct{}
	rabbitPublishLock sync.Mutex
	wg                sync.WaitGroup
}

func NewServer() *Server {
	return &Server{
		rabbitExchanges: make(map[string]struct{}),
	}
}

func init() {
	loadDotEnv(".env")
}

func (s *Server) Start(ctx context.Context, addr string) error {
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Println("Server running on", addr)
	go s.acceptConnections(ctx)
	return nil
}

func (s *Server) StartRabbitMQ(ctx context.Context, url, queueName string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}

	errorChannel, err := conn.Channel()
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return err
	}

	jsonChannel, err := conn.Channel()
	if err != nil {
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = conn.Close()
		return err
	}

	queueExists, err := rabbitQueueExists(conn, queueName)
	if err != nil {
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = jsonChannel.Close()
		_ = conn.Close()
		return err
	}

	if !queueExists {
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
			_ = channel.Close()
			_ = errorChannel.Close()
			_ = jsonChannel.Close()
			_ = conn.Close()
			return err
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
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = jsonChannel.Close()
		_ = conn.Close()
		return err
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
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = jsonChannel.Close()
		_ = conn.Close()
		return err
	}

	if err := jsonChannel.Confirm(false); err != nil {
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = jsonChannel.Close()
		_ = conn.Close()
		return err
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
		_ = channel.Close()
		_ = errorChannel.Close()
		_ = jsonChannel.Close()
		_ = conn.Close()
		return err
	}

	s.rabbitConn = conn
	s.rabbitCh = channel
	s.rabbitJSONConfirm = jsonConfirmations
	s.rabbitJSONCh = jsonChannel
	s.rabbitErrorCh = errorChannel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
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

				if rejected, err := s.rejectMalformedJSON(ctx, delivery, "rabbitmq_consumer"); err != nil {
					fmt.Println("Error rejecting malformed JSON message:", err)
					continue
				} else if rejected {
					continue
				}

				if _, err := parseTelemetryPayload(delivery.Body); err != nil {
					fmt.Println("Error processing RabbitMQ message:", err)
					if rejectErr := s.rejectDelivery(ctx, delivery, "rabbitmq_consumer", err); rejectErr != nil {
						fmt.Println("Error rejecting RabbitMQ message:", rejectErr)
					}
					continue
				}

				if err := delivery.Ack(false); err != nil {
					fmt.Println("Error acknowledging RabbitMQ message:", err)
					_ = s.publishRabbitMQError(ctx, "rabbitmq_ack", err.Error(), delivery.Body)
				}
			}
		}
	}()

	return nil
}

func (s *Server) acceptConnections(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.listener.Close()
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				fmt.Println("Error accepting:", err)
				continue
			}
			s.wg.Add(1)
			go s.handleConnection(ctx, conn)
		}
	}
}

// readTelemetryMessage reads one full variable-length telemetry payload off
// the connection: it reads the fixed header first, uses the count/flags in
// that header to work out how many more bytes to expect, then reads the
// rest. Returns the complete raw payload (header included).
func readTelemetryMessage(reader io.Reader) ([]byte, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	flags := header[15]
	count := int(header[16])
	if count == 0 {
		return nil, fmt.Errorf("record count is zero")
	}

	remaining := expectedRemainingSize(count, flags)
	rest := make([]byte, remaining)
	if _, err := io.ReadFull(reader, rest); err != nil {
		return nil, fmt.Errorf("reading body (%d bytes): %w", remaining, err)
	}

	return append(header, rest...), nil
}

// expectedRemainingSize returns the number of bytes that follow the header
// for a payload with the given record count and flags.
func expectedRemainingSize(count int, flags byte) int {
	size := 0
	for i := 0; i < count; i++ {
		if i == count-1 {
			size += newestRecordSize
		} else {
			size += oldRecordSize
		}
	}
	if flags&FlagGPSPresent != 0 {
		size += gpsBlockSize
	}
	if flags&FlagCellPresent != 0 {
		size += cellBlockSize
	}
	return size
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(1 * time.Minute)) // Set a read timeout

	reader := bufio.NewReader(conn)

	select {
	case <-ctx.Done():
		return
	default:
	}

	payload, err := readTelemetryMessage(reader)
	if err != nil {
		if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
			fmt.Println("Connection closed by client")
			return
		}
		fmt.Println("Error reading:", err)
		_ = s.publishRabbitMQError(ctx, "tcp_ingest", err.Error(), payload)
		return
	}

	parsed, err := parseTelemetryPayload(payload)
	if err != nil {
		fmt.Println("Error processing TCP payload:", err)
		_ = s.publishRabbitMQError(ctx, "tcp_ingest", err.Error(), payload)
		return
	}

	if err := s.publishRabbitMQJSON(ctx, parsed); err != nil {
		fmt.Println("Error publishing parsed TCP payload:", err)
		_ = s.publishRabbitMQError(ctx, "json_publish", err.Error(), payload)
		return
	}

	response := []byte{0b00000000}
	if _, err := conn.Write(response); err != nil {
		fmt.Println("Error writing response:", err)
	}
}

// parseTelemetryPayload parses a complete, self-contained telemetry
// payload (header + records + optional GPS/cell blocks).
func parseTelemetryPayload(payload []byte) (*TelemetryData, error) {
	if len(payload) < headerSize {
		return nil, fmt.Errorf("payload too short: received %d bytes, need at least %d for header", len(payload), headerSize)
	}

	magicByte1, err := parseMagicByte("magic_byte_1")
	if err != nil {
		return nil, fmt.Errorf("invalid magic_byte_1: %w", err)
	}

	magicByte2, err := parseMagicByte("magic_byte_2")
	if err != nil {
		return nil, fmt.Errorf("invalid magic_byte_2: %w", err)
	}

	if payload[0] != magicByte1 || payload[1] != magicByte2 {
		return nil, fmt.Errorf("invalid magic bytes: %x", payload[:2])
	}

	versionMajor := payload[2]
	versionMinor := payload[3]
	versionPatch := payload[4]
	if versionMajor != 1 {
		return nil, fmt.Errorf("unsupported version major: %d", versionMajor)
	}

	rawId := payload[5:15]
	id := fmt.Sprintf("%c%c%c-%c%c%c-%c%c%c%c", rawId[0], rawId[1], rawId[2], rawId[3], rawId[4], rawId[5], rawId[6], rawId[7], rawId[8], rawId[9])

	version := fmt.Sprintf("%d.%d.%d", versionMajor, versionMinor, versionPatch)

	flags := payload[15]
	count := int(payload[16])
	if count == 0 {
		return nil, fmt.Errorf("record count is zero")
	}

	hasGPS := flags&FlagGPSPresent != 0
	hasCell := flags&FlagCellPresent != 0

	expectedSize := headerSize + expectedRemainingSize(count, flags)
	if len(payload) < expectedSize {
		return nil, fmt.Errorf("payload too short: received %d bytes, need %d for count=%d flags=0x%02x", len(payload), expectedSize, count, flags)
	}

	type rawRecord struct {
		isNewest    bool
		timedif     uint16
		timestamp   uint64
		co2         uint16
		ch4Raw      uint16
		ch4Ppm      uint16
		level       uint16
		distance    uint16
		moistureRaw uint16
		moisturePct uint16
		battV       uint16
		battP       uint16
		status      byte
		errorCode   byte
	}

	offset := headerSize
	rawRecords := make([]rawRecord, 0, count)
	for i := 0; i < count; i++ {
		isNewest := i == count-1

		var timedif uint16
		var timestamp uint64
		if isNewest {
			timestamp = binary.LittleEndian.Uint64(payload[offset : offset+8])
			offset += 8
		} else {
			timedif = binary.LittleEndian.Uint16(payload[offset : offset+2])
			offset += 2
		}

		co2 := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		ch4Raw := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		ch4Ppm := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		level := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		distance := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		moistureRaw := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		moisturePct := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		battV := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		battP := binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		status := payload[offset]
		offset++
		errorCode := payload[offset]
		offset++

		rawRecords = append(rawRecords, rawRecord{
			isNewest:    isNewest,
			timedif:     timedif,
			timestamp:   timestamp,
			co2:         co2,
			ch4Raw:      ch4Raw,
			ch4Ppm:      ch4Ppm,
			level:       level,
			distance:    distance,
			moistureRaw: moistureRaw,
			moisturePct: moisturePct,
			battV:       battV,
			battP:       battP,
			status:      status,
			errorCode:   errorCode,
		})
	}

	// Records are stored oldest-to-newest. The newest record carries an
	// absolute timestamp; every older record carries a timedif representing
	// the gap to the *next* (newer) record. Walk backward from the newest
	// timestamp, accumulating timedifs, to recover each record's timestamp.
	newestTimestamp := rawRecords[count-1].timestamp
	timestamps := make([]uint64, count)
	timestamps[count-1] = newestTimestamp

	var cumulative uint64
	for i := count - 2; i >= 0; i-- {
		cumulative += uint64(rawRecords[i].timedif)
		timestamps[i] = newestTimestamp - cumulative
	}

	readings := make([]TelemetryReading, count)
	for i, r := range rawRecords {
		readings[i] = TelemetryReading{
			Timestamp:         timestamps[i],
			CarbonDioxide:     r.co2,
			MethaneRaw:        r.ch4Raw,
			Methane:           r.ch4Ppm,
			Level:             r.level,
			Distance:          r.distance,
			MoistureRaw:       r.moistureRaw,
			Moisture:          r.moisturePct,
			BatteryVoltage:    r.battV,
			BatteryPercentage: r.battP,
			Status:            r.status,
			ErrorCode:         r.errorCode,
		}
	}

	data := &TelemetryData{
		ID:       id,
		Version:  version,
		Flags:    flags,
		Readings: readings,
		HasGPS:   hasGPS,
		HasCell:  hasCell,
	}

	if hasGPS {
		latBits := binary.LittleEndian.Uint32(payload[offset : offset+4])
		offset += 4
		lonBits := binary.LittleEndian.Uint32(payload[offset : offset+4])
		offset += 4
		data.Latitude = math.Float32frombits(latBits)
		data.Longitude = math.Float32frombits(lonBits)
	}

	if hasCell {
		data.MobileCountryCode = binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
		data.MobileNetworkCode = binary.LittleEndian.Uint16(payload[offset : offset+2])
		offset += 2
	}

	fmt.Printf("Parsed Data: %+v\n", data)

	return data, nil
}

func (s *Server) publishRabbitMQJSON(ctx context.Context, data *TelemetryData) error {
	if s.rabbitJSONCh == nil {
		return nil
	}
	if data == nil {
		return fmt.Errorf("telemetry data is nil")
	}

	s.rabbitPublishLock.Lock()
	defer s.rabbitPublishLock.Unlock()

	if err := s.ensureJSONExchangeLocked(telemetryJSONExchange); err != nil {
		return err
	}

	routingKey := telemetryRoutingKey(data.ID, data.Version)

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if err := s.rabbitJSONCh.PublishWithContext(
		ctx,
		telemetryJSONExchange,
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

	if s.rabbitJSONConfirm == nil {
		return nil
	}

	select {
	case confirmation, ok := <-s.rabbitJSONConfirm:
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

func (s *Server) ensureJSONExchangeLocked(exchangeName string) error {
	if _, exists := s.rabbitExchanges[exchangeName]; exists {
		return nil
	}

	if err := s.rabbitJSONCh.ExchangeDeclare(
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

	s.rabbitExchanges[exchangeName] = struct{}{}
	return nil
}

func (s *Server) publishRabbitMQError(ctx context.Context, source, errText string, payload []byte) error {
	if s.rabbitErrorCh == nil {
		return nil
	}

	message := fmt.Sprintf(`{"source":%q,"error":%q,"payload":%q}`, source, errText, string(payload))

	s.rabbitPublishLock.Lock()
	defer s.rabbitPublishLock.Unlock()

	return s.rabbitErrorCh.PublishWithContext(
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

// rejectMalformedJSON checks JSON-only deliveries and rejects malformed
// payloads with requeue=false so they can be dead-lettered.
func (s *Server) rejectMalformedJSON(ctx context.Context, delivery amqp.Delivery, source string) (bool, error) {
	if !strings.EqualFold(strings.TrimSpace(delivery.ContentType), "application/json") {
		return false, nil
	}

	if json.Valid(delivery.Body) {
		return false, nil
	}

	if err := s.rejectDelivery(ctx, delivery, source, fmt.Errorf("malformed JSON payload")); err != nil {
		return true, err
	}

	return true, nil
}

func (s *Server) rejectDelivery(ctx context.Context, delivery amqp.Delivery, source string, cause error) error {
	if cause != nil {
		if err := s.publishRabbitMQError(ctx, source, cause.Error(), delivery.Body); err != nil {
			return fmt.Errorf("publishing rejection error payload: %w", err)
		}
	}

	if err := delivery.Reject(false); err != nil {
		return fmt.Errorf("basic.reject failed: %w", err)
	}

	return nil
}

func parseMagicByte(envName string) (byte, error) {
	value := strings.TrimSpace(os.Getenv(envName))
	if value == "" {
		return 0, fmt.Errorf("%s is not set", envName)
	}

	parsed, err := strconv.ParseUint(value, 0, 8)
	if err != nil {
		return 0, fmt.Errorf("must be a byte value like 0x01 or 1: %w", err)
	}

	return byte(parsed), nil
}

func rabbitQueueExists(conn *amqp.Connection, queueName string) (bool, error) {
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

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}

		if _, alreadySet := os.LookupEnv(key); !alreadySet {
			_ = os.Setenv(key, strings.Trim(value, `"`))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading .env:", err)
	}
}

func (s *Server) Shutdown() {
	if s.rabbitCh != nil {
		_ = s.rabbitCh.Close()
	}
	if s.rabbitJSONCh != nil {
		_ = s.rabbitJSONCh.Close()
	}
	if s.rabbitErrorCh != nil {
		_ = s.rabbitErrorCh.Close()
	}
	if s.rabbitConn != nil {
		_ = s.rabbitConn.Close()
	}

	s.listener.Close()
	s.wg.Wait()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := NewServer()
	go func() {
		if err := server.Start(ctx, ":8080"); err != nil {
			fmt.Println("Server failed:", err)
		}
	}()

	rabbitMQURL := strings.TrimSpace(os.Getenv("RABBITMQ_URL"))
	rabbitMQQueue := strings.TrimSpace(os.Getenv("RABBITMQ_QUEUE"))
	if rabbitMQURL != "" && rabbitMQQueue != "" {
		go func() {
			if err := server.StartRabbitMQ(ctx, rabbitMQURL, rabbitMQQueue); err != nil {
				fmt.Println("RabbitMQ consumer failed:", err)
			}
		}()
	} else {
		fmt.Println("RabbitMQ consumer disabled: set RABBITMQ_URL and RABBITMQ_QUEUE")
	}

	<-ctx.Done()
	server.Shutdown()
}