// Package config loads the .env file at startup and parses the environment
// values the rest of the service reads.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const defaultTelemetryExchange = "pact.telemetry"

// Load reads path into the process environment. Values already present in the
// environment win, so a real environment variable overrides the file. A missing
// file is not an error: the service runs on whatever the environment provides.
//
// This used to run from an init() function. It is now called explicitly from
// main so that the ordering against the first Parse is visible rather than
// implied by package initialisation order.
func Load(path string) {
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

// MagicBytes returns the two bytes every telemetry payload must start with.
// They are read on each call rather than cached, so a test can set them with
// t.Setenv and so a deployment can change them without a rebuild.
func MagicBytes() (byte, byte, error) {
	first, err := parseMagicByte("magic_byte_1")
	if err != nil {
		return 0, 0, fmt.Errorf("invalid magic_byte_1: %w", err)
	}

	second, err := parseMagicByte("magic_byte_2")
	if err != nil {
		return 0, 0, fmt.Errorf("invalid magic_byte_2: %w", err)
	}

	return first, second, nil
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

// TelemetryExchange is the topic exchange parsed telemetry is published to.
func TelemetryExchange() string {
	name := strings.TrimSpace(os.Getenv("RABBITMQ_TELEMETRY_EXCHANGE"))
	if name == "" {
		return defaultTelemetryExchange
	}

	return name
}

// RabbitURL is the broker connection string. An empty value disables the
// RabbitMQ consumer.
func RabbitURL() string {
	return strings.TrimSpace(os.Getenv("RABBITMQ_URL"))
}

// RabbitQueue is the queue the consumer reads from. An empty value disables the
// RabbitMQ consumer.
func RabbitQueue() string {
	return strings.TrimSpace(os.Getenv("RABBITMQ_QUEUE"))
}
