package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

const telemetryPayloadSize = 54

type TelemetryData struct {
	ID                string  `json:"id"`
	Version           string  `json:"version"`
	Timestamp         uint64  `json:"timestamp"`
	Latitude          float32 `json:"latitude"`
	Longitude         float32 `json:"longitude"`
	CarbonDioxide     uint16  `json:"carbon_dioxide"`
	MethaneRaw        uint16  `json:"methane_raw"`
	Methane           uint16  `json:"methane"`
	Level             uint16  `json:"level"`
	Distance          uint16  `json:"distance"`
	MoistureRaw       uint16  `json:"moisture_raw"`
	Moisture          uint16  `json:"moisture"`
	MobileCountryCode uint16  `json:"mobile_country_code"`
	MobileNetworkCode uint16  `json:"mobile_network_code"`
	Uptime            uint32  `json:"uptime"`
	ErrorCode         byte    `json:"error_code"`
}

type Server struct {
	listener          net.Listener
	rabbitConn        *amqp.Connection
	rabbitCh          *amqp.Channel
	rabbitJSONCh      *amqp.Channel
	rabbitJSONConfirm chan amqp.Confirmation
	rabbitErrorCh     *amqp.Channel
	rabbitPublishLock sync.Mutex
	wg                sync.WaitGroup
}

func NewServer() *Server {
	return &Server{}
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

	_, err = channel.QueueDeclare(
		queueName,
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

	err = jsonChannel.ExchangeDeclare(
		"pact.telemetry.json",
		"fanout",
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

				if _, err := parseTelemetryPayload(delivery.Body); err != nil {
					fmt.Println("Error processing RabbitMQ message:", err)
					_ = s.publishRabbitMQError(ctx, "rabbitmq_consumer", err.Error(), delivery.Body)
					_ = delivery.Nack(false, false)
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

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(1 * time.Minute)) // Set a read timeout
	buffer := make([]byte, telemetryPayloadSize)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := conn.Read(buffer)
			if err != nil {
				if errors.Is(err, net.ErrClosed) || err.Error() == "EOF" {
					fmt.Println("Connection closed by client")
					return
				}

				fmt.Println("Error reading:", err)
				return
			}

			payload := append([]byte(nil), buffer[:n]...)

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

			return
		}
	}
}

func parseTelemetryPayload(payload []byte) (*TelemetryData, error) {
	if len(payload) < telemetryPayloadSize {
		return nil, fmt.Errorf("payload too short: received %d bytes, need at least %d", len(payload), telemetryPayloadSize)
	}

	magicBytes := payload[:2]
	magicByte1, err := parseMagicByte("magic_byte_1")
	if err != nil {
		return nil, fmt.Errorf("invalid magic_byte_1: %w", err)
	}

	magicByte2, err := parseMagicByte("magic_byte_2")
	if err != nil {
		return nil, fmt.Errorf("invalid magic_byte_2: %w", err)
	}

	if magicBytes[0] != magicByte1 || magicBytes[1] != magicByte2 {
		return nil, fmt.Errorf("invalid magic bytes: %x", magicBytes)
	}

	rawId := payload[2:12]
	id := fmt.Sprintf("%c%c%c-%c%c%c-%c%c%c%c", rawId[0], rawId[1], rawId[2], rawId[3], rawId[4], rawId[5], rawId[6], rawId[7], rawId[8], rawId[9])

	versionMajor := payload[12]
	versionMinor := payload[13]
	versionPatch := payload[14]
	version := fmt.Sprintf("%d.%d.%d", versionMajor, versionMinor, versionPatch)

	timestamp := binary.LittleEndian.Uint64(payload[15:23])

	latitude := binary.LittleEndian.Uint32(payload[23:27])
	longitude := binary.LittleEndian.Uint32(payload[27:31])

	carbonDioxide := binary.LittleEndian.Uint16(payload[31:33])
	methaneRaw := binary.LittleEndian.Uint16(payload[33:35])
	methane := binary.LittleEndian.Uint16(payload[35:37])

	level := binary.LittleEndian.Uint16(payload[37:39])
	distance := binary.LittleEndian.Uint16(payload[39:41])

	moistureRaw := binary.LittleEndian.Uint16(payload[41:43])
	moisture := binary.LittleEndian.Uint16(payload[43:45])

	mobileCountryCode := binary.LittleEndian.Uint16(payload[45:47])
	mobileNetworkCode := binary.LittleEndian.Uint16(payload[47:49])

	uptime := binary.LittleEndian.Uint32(payload[49:53])

	errorCode := payload[53]

	// fmt.Printf("Received: %x\n", payload[:telemetryPayloadSize])
	// fmt.Printf("Parsed ID: %s\n", id)
	// fmt.Printf("Parsed Version: %s\n", version)
	// fmt.Printf("Parsed Timestamp: %d\n", timestamp)
	// fmt.Printf("Parsed Latitude: %f\n", math.Float32frombits(latitude))
	// fmt.Printf("Parsed Longitude: %f\n", math.Float32frombits(longitude))
	// fmt.Printf("Parsed Carbon Dioxide: %d\n", carbonDioxide)
	// fmt.Printf("Parsed Methane Raw: %d\n", methaneRaw)
	// fmt.Printf("Parsed Methane: %d\n", methane)
	// fmt.Printf("Parsed Level: %d\n", level)
	// fmt.Printf("Parsed Distance: %d\n", distance)
	// fmt.Printf("Parsed Moisture Raw: %d\n", moistureRaw)
	// fmt.Printf("Parsed Moisture: %d\n", moisture)
	// fmt.Printf("Parsed Mobile Country Code: %d\n", mobileCountryCode)
	// fmt.Printf("Parsed Mobile Network Code: %d\n", mobileNetworkCode)
	// fmt.Printf("Parsed Uptime: %d\n", uptime)
	// fmt.Printf("Parsed Error Code: %d\n", errorCode)

	data := &TelemetryData{
		ID:                id,
		Version:           version,
		Timestamp:         timestamp,
		Latitude:          math.Float32frombits(latitude),
		Longitude:         math.Float32frombits(longitude),
		CarbonDioxide:     carbonDioxide,
		MethaneRaw:        methaneRaw,
		Methane:           methane,
		Level:             level,
		Distance:          distance,
		MoistureRaw:       moistureRaw,
		Moisture:          moisture,
		MobileCountryCode: mobileCountryCode,
		MobileNetworkCode: mobileNetworkCode,
		Uptime:            uptime,
		ErrorCode:         errorCode,
	}

	fmt.Printf("Parsed Data: %+v\n", data)

	return data, nil
}

func (s *Server) publishRabbitMQJSON(ctx context.Context, data *TelemetryData) error {
	if s.rabbitJSONCh == nil {
		return nil
	}

	s.rabbitPublishLock.Lock()
	defer s.rabbitPublishLock.Unlock()

	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if err := s.rabbitJSONCh.PublishWithContext(
		ctx,
		"pact.telemetry.json",
		"",
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
