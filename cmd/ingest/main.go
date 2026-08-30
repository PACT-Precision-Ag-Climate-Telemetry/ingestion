// Command ingest accepts binary telemetry over TCP, parses it, and publishes
// the result to RabbitMQ.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"ingest/internal/config"
	"ingest/internal/rabbit"
	"ingest/internal/server"
)

const listenAddress = ":8080"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config.Load(".env")

	srv := server.New()
	go func() {
		if err := srv.Start(ctx, listenAddress); err != nil {
			fmt.Println("Server failed:", err)
		}
	}()

	// The broker is dialled in the background so the listener comes up straight
	// away. Until it connects, parsed payloads have nowhere to go and are
	// dropped -- the same behaviour as before.
	var (
		clientMu sync.Mutex
		client   *rabbit.Client
	)

	rabbitMQURL := config.RabbitURL()
	rabbitMQQueue := config.RabbitQueue()
	if rabbitMQURL != "" && rabbitMQQueue != "" {
		go func() {
			connected, err := rabbit.Connect(ctx, rabbitMQURL, rabbitMQQueue)
			if err != nil {
				fmt.Println("RabbitMQ consumer failed:", err)
				return
			}

			clientMu.Lock()
			client = connected
			clientMu.Unlock()

			srv.SetPublisher(connected)
		}()
	} else {
		fmt.Println("RabbitMQ consumer disabled: set RABBITMQ_URL and RABBITMQ_QUEUE")
	}

	<-ctx.Done()

	srv.Shutdown()

	clientMu.Lock()
	connected := client
	clientMu.Unlock()
	connected.Close()
}
