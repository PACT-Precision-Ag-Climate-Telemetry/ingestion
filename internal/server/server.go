// Package server is the ingest server itself: lifecycle, TCP accept loop, and
// the per-connection handler that parses a payload and hands it to a publisher.
package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"ingest/internal/telemetry"
)

// Publisher is the sink for parsed payloads and for failures. It is declared
// here, rather than in the package that implements it, so the server depends on
// what it needs rather than on RabbitMQ.
type Publisher interface {
	PublishTelemetry(ctx context.Context, data *telemetry.Data) error
	PublishError(ctx context.Context, source, errText string, payload []byte) error
}

type Server struct {
	listener net.Listener

	// The broker is dialled in the background, so the publisher arrives after
	// the listener is already accepting. The mutex covers that handoff: before
	// the split these were fields on one struct written by the RabbitMQ
	// goroutine and read by every connection handler, with no synchronisation.
	publisherMu sync.RWMutex
	publisher   Publisher

	wg sync.WaitGroup
}

func New() *Server {
	return &Server{}
}

// SetPublisher installs the sink for parsed payloads. Until it is called -- or
// if RabbitMQ is disabled entirely -- parsed payloads are dropped, which is what
// the service did before when the broker had not connected yet.
func (s *Server) SetPublisher(publisher Publisher) {
	s.publisherMu.Lock()
	defer s.publisherMu.Unlock()
	s.publisher = publisher
}

func (s *Server) currentPublisher() Publisher {
	s.publisherMu.RLock()
	defer s.publisherMu.RUnlock()
	return s.publisher
}

func (s *Server) publishTelemetry(ctx context.Context, data *telemetry.Data) error {
	publisher := s.currentPublisher()
	if publisher == nil {
		return nil
	}
	return publisher.PublishTelemetry(ctx, data)
}

func (s *Server) publishError(ctx context.Context, source, errText string, payload []byte) error {
	publisher := s.currentPublisher()
	if publisher == nil {
		return nil
	}
	return publisher.PublishError(ctx, source, errText, payload)
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

	reader := bufio.NewReader(conn)

	select {
	case <-ctx.Done():
		return
	default:
	}

	payload, err := telemetry.ReadMessage(reader)
	if err != nil {
		if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
			fmt.Println("Connection closed by client")
			return
		}
		fmt.Println("Error reading:", err)
		_ = s.publishError(ctx, "tcp_ingest", err.Error(), payload)
		return
	}

	parsed, err := telemetry.Parse(payload)
	if err != nil {
		fmt.Println("Error processing TCP payload:", err)
		_ = s.publishError(ctx, "tcp_ingest", err.Error(), payload)
		return
	}

	if err := s.publishTelemetry(ctx, parsed); err != nil {
		fmt.Println("Error publishing parsed TCP payload:", err)
		_ = s.publishError(ctx, "json_publish", err.Error(), payload)
		return
	}

	response := []byte{0b00000000}
	if _, err := conn.Write(response); err != nil {
		fmt.Println("Error writing response:", err)
	}
}

// Shutdown stops accepting and waits for in-flight connections. Closing the
// broker is the caller's job, since the caller owns it.
func (s *Server) Shutdown() {
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
}
